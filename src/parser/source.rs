//! Source coordinates shared by diagnostics, literals and projection labels.

use super::*;

thread_local! {
    /// Raw SQL text of the statement currently being parsed. Used by
    /// literal-conversion sites (`convert_value`) that need to peek at
    /// the original token text to distinguish sqlparser tokens that
    /// share a variant but not a syntax — e.g. `HexStringLiteral` from
    /// `0xCAFE` vs from `X'CAFE'`, which MySQL 8.0 treats differently
    /// on odd payload length. Nested parses (view expansion, prepared
    /// re-parse) save/restore the outer text without a copy.
    /// The pointer is valid only while the matching
    /// [`ParserRawSqlGuard`] is alive; parsing is synchronous and the guard
    /// restores the previous pointer before the borrowed SQL can go away.
    static PARSER_RAW_SQL: std::cell::Cell<Option<*const str>> = const { std::cell::Cell::new(None) };
    static PARSER_SOURCE_POSITION: std::cell::Cell<SourcePosition> = const {
        std::cell::Cell::new(SourcePosition::start())
    };
}

pub(crate) struct ParserRawSqlGuard {
    previous: Option<*const str>,
    previous_position: SourcePosition,
}

impl ParserRawSqlGuard {
    pub(crate) fn activate(sql: &str) -> Self {
        let previous = PARSER_RAW_SQL.with(|slot| slot.replace(Some(sql as *const str)));
        let previous_position =
            PARSER_SOURCE_POSITION.with(|slot| slot.replace(SourcePosition::start()));
        Self {
            previous,
            previous_position,
        }
    }
}

impl Drop for ParserRawSqlGuard {
    fn drop(&mut self) {
        PARSER_RAW_SQL.with(|slot| slot.set(self.previous.take()));
        PARSER_SOURCE_POSITION.with(|slot| slot.set(self.previous_position));
    }
}

/// Extract the exact source substring covered by `span` from the raw SQL
/// text of the current parse. Returns `None` when there is no active
/// parse (e.g. this is being called outside `parse_statement`) or when
/// the span does not resolve inside the buffer.
///
/// `span` uses 1-based character coordinates. Reuse the current source's
/// cursor so sequential literal lookups scan each source character once.
pub(super) fn raw_source_is_x_hex(span: sqlparser::tokenizer::Span) -> Option<bool> {
    PARSER_RAW_SQL.with(|slot| {
        let pointer = slot.get()?;
        // SAFETY: `ParserRawSqlGuard` installs this pointer from a live `&str`
        // and is necessarily alive during AST conversion. Nested parses save
        // and restore the outer pointer before either input is dropped.
        let source = unsafe { &*pointer };
        // Convert 1-based (line, column) → byte offset in the source.
        let start = cached_position_in_source(source, span.start.line, span.start.column)?;
        Some(
            source[start..]
                .trim_start()
                .as_bytes()
                .first()
                .is_some_and(|byte| matches!(*byte, b'X' | b'x')),
        )
    })
}

#[derive(Clone, Copy)]
pub(super) struct SourcePosition {
    offset: usize,
    line: u64,
    column: u64,
}

impl SourcePosition {
    pub(super) const fn start() -> Self {
        Self {
            offset: 0,
            line: 1,
            column: 1,
        }
    }

    pub(super) fn seek(&mut self, source: &str, line: u64, column: u64) -> Option<usize> {
        if line == 0 || column == 0 {
            return None;
        }
        // AST traversal is usually in source order, but rewrites and error
        // spans can revisit earlier tokens. Never apply a forward cursor to
        // a backwards lookup. The guard also resets it for nested SQL.
        if (line, column) < (self.line, self.column) {
            *self = Self::start();
        }
        for ch in source[self.offset..].chars() {
            if self.line == line && (self.column == column || ch == '\n') {
                return Some(self.offset);
            }
            self.offset += ch.len_utf8();
            if ch == '\n' {
                self.line += 1;
                self.column = 1;
            } else {
                self.column += 1;
            }
        }
        (self.line == line && self.column == column).then_some(self.offset)
    }
}

fn cached_position_in_source(source: &str, line: u64, column: u64) -> Option<usize> {
    PARSER_SOURCE_POSITION.with(|slot| {
        let mut position = slot.get();
        let result = position.seek(source, line, column);
        slot.set(position);
        result
    })
}

pub(super) fn position_in_source(source: &str, line_1: u64, col_1: u64) -> Option<usize> {
    let active = PARSER_RAW_SQL.with(|slot| {
        slot.get()
            .is_some_and(|pointer| std::ptr::eq(pointer, source as *const str))
    });
    if active {
        cached_position_in_source(source, line_1, col_1)
    } else {
        SourcePosition::start().seek(source, line_1, col_1)
    }
}

pub(super) fn raw_source_byte_span(span: sqlparser::tokenizer::Span) -> Option<Span> {
    PARSER_RAW_SQL.with(|slot| {
        let pointer = slot.get()?;
        // SAFETY: identical lifetime invariant to `raw_source_is_x_hex`.
        let source = unsafe { &*pointer };
        let start = cached_position_in_source(source, span.start.line, span.start.column)?;
        let end = cached_position_in_source(source, span.end.line, span.end.column)?;
        (end >= start).then_some(Span { start, end })
    })
}

/// Keep the source borrow within the synchronous conversion callback. Nested
/// conversions reuse the same source and its coordinates instead of Display.
pub(super) fn with_parser_source<T>(f: impl FnOnce(&str) -> T) -> Option<T> {
    PARSER_RAW_SQL.with(|slot| {
        let pointer = slot.get()?;
        // SAFETY: installed and restored only by ParserRawSqlGuard; the
        // callback cannot return a borrow whose lifetime exceeds this call.
        Some(f(unsafe { &*pointer }))
    })
}

/// An explicit edit in the input coordinate space. Edits must be sorted and
/// disjoint; inserted text is attributed to the replaced source range.
pub(super) struct TextEdit<'a> {
    pub range: std::ops::Range<usize>,
    pub replacement: &'a str,
}

#[derive(Debug)]
struct EditOrigin {
    generated: std::ops::Range<usize>,
    original: std::ops::Range<usize>,
}

/// Sparse byte-coordinate map for a parser transformation. Unchanged ranges
/// map exactly; generated tokens map to their source edit. Identity and slice
/// maps allocate nothing. Maps compose by applying child-to-parent mappings.
#[derive(Debug)]
pub(super) struct SourceMap {
    original_len: usize,
    generated_len: usize,
    offset: usize,
    edits: smallvec::SmallVec<[EditOrigin; 2]>,
}

impl SourceMap {
    pub(super) fn slice(source: &str, range: std::ops::Range<usize>) -> Self {
        assert!(
            source.get(range.clone()).is_some(),
            "source slice must be valid UTF-8"
        );
        Self {
            original_len: source.len(),
            generated_len: range.len(),
            offset: range.start,
            edits: smallvec::SmallVec::new(),
        }
    }

    /// Build provenance for a transformation which already owns its output.
    /// The same validated edit representation also drives `edit_source`.
    pub(super) fn for_edits(source: &str, edits: &[TextEdit<'_>]) -> Result<Self, ParseError> {
        let mut map = Self::slice(source, 0..source.len());
        let mut original = 0;
        let mut generated: usize = 0;
        for edit in edits {
            if edit.range.start < original || source.get(edit.range.clone()).is_none() {
                return Err(ParseError::at_zero(
                    "overlapping or invalid SQL source edit",
                ));
            }
            let start = generated
                .checked_add(edit.range.start - original)
                .ok_or_else(|| ParseError::at_zero("SQL source edit length overflow"))?;
            generated = start
                .checked_add(edit.replacement.len())
                .ok_or_else(|| ParseError::at_zero("SQL source edit length overflow"))?;
            map.edits.push(EditOrigin {
                generated: start..generated,
                original: edit.range.clone(),
            });
            original = edit.range.end;
        }
        map.generated_len = generated
            .checked_add(source.len() - original)
            .ok_or_else(|| ParseError::at_zero("SQL source edit length overflow"))?;
        Ok(map)
    }

    fn position(&self, position: usize, end: bool) -> usize {
        let position = position.min(self.generated_len);
        let index = self.edits.partition_point(|edit| {
            if end {
                edit.generated.start < position
            } else {
                edit.generated.start <= position
            }
        });
        let mapped = if index == 0 {
            self.offset + position
        } else {
            let edit = &self.edits[index - 1];
            if position < edit.generated.end {
                if end {
                    edit.original.end
                } else {
                    edit.original.start
                }
            } else {
                edit.original.end + position - edit.generated.end
            }
        };
        mapped.min(self.original_len)
    }

    pub(super) fn span(&self, span: Span) -> Span {
        let start = self.position(span.start, false);
        let end = if span.start == span.end {
            start
        } else {
            self.position(span.end, true).max(start)
        };
        Span { start, end }
    }

    pub(super) fn error(&self, original: &str, error: ParseError) -> ParseError {
        debug_assert_eq!(original.len(), self.original_len);
        let mut mapped = ParseError::new(
            original,
            self.span(error.span),
            error.message,
            error.expected,
        );
        mapped.spec = error.spec;
        mapped
    }
}

/// Apply edits and record provenance together, so text and mapping cannot
/// disagree. No-op transformations keep borrowing the original buffer.
pub(super) fn edit_source<'a>(
    source: &'a str,
    edits: &[TextEdit<'_>],
) -> Result<(std::borrow::Cow<'a, str>, SourceMap), ParseError> {
    let map = SourceMap::for_edits(source, edits)?;
    if edits.is_empty() {
        return Ok((std::borrow::Cow::Borrowed(source), map));
    }
    let mut output = String::with_capacity(map.generated_len);
    let mut copied = 0;
    for edit in edits {
        output.push_str(&source[copied..edit.range.start]);
        output.push_str(edit.replacement);
        copied = edit.range.end;
    }
    output.push_str(&source[copied..]);
    Ok((std::borrow::Cow::Owned(output), map))
}

thread_local! {
    static REWRITE_MAPS: std::cell::RefCell<Option<Vec<SourceMap>>> = const { std::cell::RefCell::new(None) };
    // This signature is consumed immediately by the pass runner. Addresses
    // are compared only, never dereferenced or retained as source ownership.
    static EXPLICIT_REWRITE_MAP: std::cell::Cell<Option<(usize, usize, usize, usize)>> = const { std::cell::Cell::new(None) };
}

/// Scope provenance to one compatibility pipeline. Nested fragment parsers
/// restore their parent's maps instead of mixing coordinate spaces.
pub(super) struct RewriteTrace {
    previous: Option<Vec<SourceMap>>,
    previous_explicit: Option<(usize, usize, usize, usize)>,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}
impl RewriteTrace {
    pub(super) fn enter() -> Self {
        Self {
            previous: REWRITE_MAPS.with(|slot| slot.replace(Some(Vec::new()))),
            previous_explicit: EXPLICIT_REWRITE_MAP.with(|slot| slot.replace(None)),
            _thread: std::marker::PhantomData,
        }
    }
    pub(super) fn error(&self, original: &str, mut error: ParseError) -> ParseError {
        REWRITE_MAPS.with(|slot| {
            if let Some(maps) = slot.borrow().as_ref() {
                for map in maps.iter().rev() {
                    error.span = map.span(error.span);
                }
            }
        });
        let spec = error.spec;
        let mut mapped = ParseError::new(original, error.span, error.message, error.expected);
        mapped.spec = spec;
        mapped
    }
}
impl Drop for RewriteTrace {
    fn drop(&mut self) {
        REWRITE_MAPS.with(|slot| *slot.borrow_mut() = self.previous.take());
        EXPLICIT_REWRITE_MAP.with(|slot| slot.set(self.previous_explicit.take()));
    }
}

/// Legacy passes that return only text still have a well-defined edit range:
/// the changed middle is attributed to its complete original range; common
/// prefix/suffix bytes retain exact coordinates. Native edit-based grammars
/// provide finer maps themselves. No input buffers are retained by the trace.
pub(super) fn record_rewrite_source(input: &str, output: &str) {
    if input.len() == output.len() {
        return;
    }
    REWRITE_MAPS.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(maps) = slot.as_mut() else {
            return;
        };
        let mut prefix = input
            .bytes()
            .zip(output.bytes())
            .take_while(|(a, b)| a == b)
            .count();
        while !input.is_char_boundary(prefix) || !output.is_char_boundary(prefix) {
            prefix -= 1;
        }
        let mut suffix = input[prefix..]
            .bytes()
            .rev()
            .zip(output[prefix..].bytes().rev())
            .take_while(|(a, b)| a == b)
            .count();
        while !input.is_char_boundary(input.len() - suffix)
            || !output.is_char_boundary(output.len() - suffix)
        {
            suffix -= 1;
        }
        let edit = TextEdit {
            range: prefix..input.len() - suffix,
            replacement: &output[prefix..output.len() - suffix],
        };
        maps.push(SourceMap::for_edits(input, &[edit]).expect("UTF-8 bounded source edit"));
    });
}

pub(super) fn take_explicit_rewrite_map(input: &str, output: &str) -> bool {
    let signature = (
        input.as_ptr() as usize,
        input.len(),
        output.as_ptr() as usize,
        output.len(),
    );
    EXPLICIT_REWRITE_MAP.with(|slot| slot.take() == Some(signature))
}

/// Apply explicit compatibility edits and retain their exact provenance.
pub(super) fn rewrite_source_edits<'a>(
    source: &'a str,
    edits: &[TextEdit<'_>],
) -> Result<std::borrow::Cow<'a, str>, ParseError> {
    let (output, map) = edit_source(source, edits)?;
    if !edits.is_empty() {
        REWRITE_MAPS.with(|slot| {
            if let Some(maps) = slot.borrow_mut().as_mut() {
                maps.push(map);
                EXPLICIT_REWRITE_MAP.with(|slot| {
                    slot.set(Some((
                        source.as_ptr() as usize,
                        source.len(),
                        output.as_ptr() as usize,
                        output.len(),
                    )))
                });
            }
        });
    }
    Ok(output)
}
