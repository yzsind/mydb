//! Provenance for syntax lowered through sqlparser identifiers. Only names
//! allocated by this parse can carry extension semantics; user names are inert.
use super::*;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    marker::PhantomData,
    rc::Rc,
};

#[derive(Clone, Debug)]
pub(super) enum Extension {
    EmptyRow,
    QueryTail,
    EmptyGroupingSet,
    ReturningOld(String),
    ReturningNew(String),
    ReturningTarget(String),
    IndexInclude(usize),
    IndexVisibility,
    VectorOptions(usize),
}

#[derive(Default)]
struct State {
    source: Option<*const str>,
    names: Option<HashSet<usize>>,
    generated: HashMap<String, Extension>,
    next: usize,
}
thread_local! { static STATE: RefCell<State> = RefCell::new(State::default()); }

pub(super) struct Guard<'a> {
    root: bool,
    _source: PhantomData<&'a str>,
    _thread: PhantomData<Rc<()>>,
}
impl<'a> Guard<'a> {
    pub(super) fn enter(source: &'a str) -> Self {
        let root = STATE.with(|state| {
            let mut state = state.borrow_mut();
            if state.source.is_some() {
                return false;
            }
            *state = State::default();
            state.source = Some(source as *const str);
            true
        });
        Self {
            root,
            _source: PhantomData,
            _thread: PhantomData,
        }
    }
}
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        if self.root {
            STATE.with(|state| *state.borrow_mut() = State::default());
        }
    }
}

pub(super) fn register(sql: &str, extension: Extension) -> String {
    STATE.with(|slot| {
        let mut state = slot.borrow_mut();
        if state.names.is_none() {
            // SAFETY: the root Guard borrows the source for the entire
            // synchronous parse, including nested fragment parses.
            let source = state.source.map(|ptr| unsafe { &*ptr }).unwrap_or(sql);
            fn collect_names(source: &str, names: &mut HashSet<usize>) {
                const PREFIX: &[u8] = b"__mydb_extension_";
                let bytes = source.as_bytes();
                for start in memchr::memchr_iter(b'_', bytes) {
                    if !bytes
                        .get(start..start + PREFIX.len())
                        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(PREFIX))
                    {
                        continue;
                    }
                    let start = start + PREFIX.len();
                    let len = bytes[start..]
                        .iter()
                        .take_while(|byte| byte.is_ascii_digit())
                        .count();
                    if let Ok(index) = source[start..start + len].parse::<usize>() {
                        names.insert(index);
                    }
                }
            }
            let mut names = HashSet::new();
            // Over-reserving a substring is harmless and avoids relying on a
            // second tokenizer's exact identifier boundaries for provenance.
            collect_names(source, &mut names);
            if source.as_bytes().contains(&b'\\') {
                let mode =
                    crate::sql_lex::SqlLexMode::from_parser_bits(current_parser_sql_mode_bits());
                let mut cursor =
                    crate::sql_lex::SqlTokenCursor::new(source, mode, MYSQL_VERSION_ID);
                while let Ok(Some(token)) = cursor.next() {
                    if cursor.token_text(&token).as_bytes().contains(&b'\\') {
                        if let Some(decoded) = cursor.decoded_quoted_text(&token) {
                            collect_names(&decoded, &mut names);
                        }
                    }
                }
            }
            state.names = Some(names);
        }
        loop {
            let index = state.next;
            state.next += 1;
            if !state.names.as_ref().unwrap().contains(&index) {
                let name = format!("__mydb_extension_{index}");
                state.generated.insert(name.clone(), extension);
                return name;
            }
        }
    })
}

pub(super) fn get(name: &str) -> Option<Extension> {
    STATE.with(|state| state.borrow().generated.get(name).cloned())
}
