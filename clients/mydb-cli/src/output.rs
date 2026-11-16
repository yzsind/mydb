use mysql::{consts::ColumnType, Column, Value};
use std::{
    fs::File,
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
};
use unicode_width::UnicodeWidthStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Table,
    Tsv,
    Vertical,
}
#[derive(Clone, Copy)]
pub struct Options {
    pub format: Format,
    pub names: bool,
    pub raw: bool,
    pub binary_hex: bool,
    pub quick: bool,
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for b in bytes {
        write!(out, "{b:02X}").unwrap();
    }
    out
}
fn binary(column: &Column) -> bool {
    column.character_set() == 63
        && matches!(
            column.column_type(),
            ColumnType::MYSQL_TYPE_STRING
                | ColumnType::MYSQL_TYPE_VAR_STRING
                | ColumnType::MYSQL_TYPE_VARCHAR
                | ColumnType::MYSQL_TYPE_BLOB
                | ColumnType::MYSQL_TYPE_TINY_BLOB
                | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
                | ColumnType::MYSQL_TYPE_LONG_BLOB
                | ColumnType::MYSQL_TYPE_BIT
                | ColumnType::MYSQL_TYPE_GEOMETRY
        )
}
fn numeric(column: &Column) -> bool {
    matches!(
        column.column_type(),
        ColumnType::MYSQL_TYPE_TINY
            | ColumnType::MYSQL_TYPE_SHORT
            | ColumnType::MYSQL_TYPE_LONG
            | ColumnType::MYSQL_TYPE_INT24
            | ColumnType::MYSQL_TYPE_LONGLONG
            | ColumnType::MYSQL_TYPE_DECIMAL
            | ColumnType::MYSQL_TYPE_NEWDECIMAL
            | ColumnType::MYSQL_TYPE_FLOAT
            | ColumnType::MYSQL_TYPE_DOUBLE
            | ColumnType::MYSQL_TYPE_YEAR
    )
}
fn bytes(value: &Value) -> Vec<u8> {
    match value {
        Value::NULL => b"NULL".to_vec(),
        Value::Bytes(b) => b.clone(),
        Value::Int(v) => v.to_string().into_bytes(),
        Value::UInt(v) => v.to_string().into_bytes(),
        Value::Float(v) => v.to_string().into_bytes(),
        Value::Double(v) => v.to_string().into_bytes(),
        Value::Date(y, m, d, h, min, s, us) => {
            let mut v = format!("{y:04}-{m:02}-{d:02}");
            if (*h, *min, *s, *us) != (0, 0, 0, 0) {
                v.push_str(&format!(" {h:02}:{min:02}:{s:02}"));
            }
            if *us != 0 {
                v.push_str(&format!(".{us:06}"));
            }
            v.into_bytes()
        }
        Value::Time(neg, days, h, m, s, us) => {
            let mut v = format!(
                "{}{}:{m:02}:{s:02}",
                if *neg { "-" } else { "" },
                u64::from(*days) * 24 + u64::from(*h)
            );
            if *us != 0 {
                v.push_str(&format!(".{us:06}"));
            }
            v.into_bytes()
        }
    }
}

fn field_bytes<'a>(
    value: &'a Value,
    column: &Column,
    raw: bool,
) -> io::Result<std::borrow::Cow<'a, [u8]>> {
    let Value::Bytes(data) = value else {
        return Ok(std::borrow::Cow::Owned(bytes(value)));
    };
    if column.column_type() != ColumnType::MYSQL_TYPE_VECTOR || raw {
        return Ok(std::borrow::Cow::Borrowed(data));
    }
    if data.len() % 4 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "VECTOR payload is not an array of FLOAT32 coordinates",
        ));
    }
    let mut text = String::from("[");
    for (i, chunk) in data.chunks_exact(4).enumerate() {
        let value = f32::from_le_bytes(chunk.try_into().unwrap());
        if !value.is_finite() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "VECTOR contains a non-finite coordinate",
            ));
        }
        if i != 0 {
            text.push(',');
        }
        text.push_str(&value.to_string());
    }
    text.push(']');
    Ok(std::borrow::Cow::Owned(text.into_bytes()))
}

fn escape(bytes: &[u8], terminal: bool) -> String {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return hex(bytes);
    };
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\0' => out.push_str("\\0"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\\' => out.push_str("\\\\"),
            c if terminal && c.is_control() => {
                use std::fmt::Write;
                write!(out, "\\u{{{:X}}}", c as u32).unwrap();
            }
            c => out.push(c),
        }
    }
    out
}

fn display_text(bytes: &[u8]) -> String {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return hex(bytes);
    };
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\0' => out.push(' '),
            '\n' | '\r' | '\t' => out.push(c),
            c if c.is_control() => {
                use std::fmt::Write;
                write!(out, "\\u{{{:X}}}", c as u32).unwrap();
            }
            c => out.push(c),
        }
    }
    out
}

fn display_width(text: &str) -> usize {
    // Match mysql's flat-field padding while preserving Unicode display widths.
    // unicode-width folds CRLF into one cell; here both bytes remain significant.
    if text.is_ascii() {
        text.len()
    } else {
        UnicodeWidthStr::width(text) + text.matches("\r\n").count()
    }
}

struct RowStore {
    rows: Vec<Vec<String>>,
    file: Option<BufWriter<File>>,
    bytes: usize,
    limit: usize,
}
impl RowStore {
    fn new(limit: usize) -> Self {
        Self {
            rows: vec![],
            file: None,
            bytes: 0,
            limit,
        }
    }
    fn write_row(out: &mut impl Write, row: &[String]) -> io::Result<()> {
        for cell in row {
            out.write_all(&(cell.len() as u64).to_le_bytes())?;
            out.write_all(cell.as_bytes())?;
        }
        Ok(())
    }
    fn push(&mut self, row: Vec<String>) -> io::Result<()> {
        self.bytes = self.bytes.saturating_add(
            row.iter()
                .map(|s| s.capacity() + std::mem::size_of::<String>())
                .sum::<usize>(),
        );
        if self.file.is_none() && self.bytes > self.limit {
            let mut file = BufWriter::new(tempfile::tempfile().map_err(|e| {
                io::Error::new(e.kind(), format!("cannot create result spool: {e}"))
            })?);
            for old in self.rows.drain(..) {
                Self::write_row(&mut file, &old)?;
            }
            self.file = Some(file);
        }
        if let Some(file) = &mut self.file {
            Self::write_row(file, &row)?;
        } else {
            self.rows.push(row);
        }
        Ok(())
    }
    fn replay(
        &mut self,
        columns: usize,
        mut visit: impl FnMut(&[String]) -> io::Result<()>,
    ) -> io::Result<()> {
        if let Some(file) = &mut self.file {
            file.flush()?;
            file.seek(SeekFrom::Start(0))?;
            let mut reader = BufReader::new(file.get_mut());
            loop {
                let mut row = Vec::with_capacity(columns);
                for i in 0..columns {
                    let mut len = [0; 8];
                    match reader.read(&mut len[..1])? {
                        0 if i == 0 => return Ok(()),
                        0 => {
                            return Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "truncated result spool",
                            ))
                        }
                        _ => reader.read_exact(&mut len[1..])?,
                    }
                    let len = usize::try_from(u64::from_le_bytes(len)).map_err(io::Error::other)?;
                    let mut cell = vec![0; len];
                    reader.read_exact(&mut cell)?;
                    row.push(String::from_utf8(cell).map_err(io::Error::other)?);
                }
                visit(&row)?;
            }
        }
        for row in &self.rows {
            visit(row)?;
        }
        Ok(())
    }
}

pub struct Renderer<'a, W: Write> {
    out: &'a mut W,
    columns: Vec<Column>,
    names: Vec<String>,
    widths: Vec<usize>,
    options: Options,
    store: RowStore,
    pub count: u64,
    started: bool,
}

fn border(out: &mut impl Write, widths: &[usize]) -> io::Result<()> {
    for width in widths {
        write!(out, "+")?;
        for _ in 0..width + 2 {
            out.write_all(b"-")?;
        }
    }
    writeln!(out, "+")
}
fn table_row(
    out: &mut impl Write,
    row: &[String],
    widths: &[usize],
    right: &[bool],
) -> io::Result<()> {
    for (i, value) in row.iter().enumerate() {
        let padding = widths[i].saturating_sub(display_width(value));
        write!(out, "| ")?;
        if right[i] {
            for _ in 0..padding {
                out.write_all(b" ")?;
            }
        }
        out.write_all(value.as_bytes())?;
        if !right[i] {
            for _ in 0..padding {
                out.write_all(b" ")?;
            }
        }
        out.write_all(b" ")?;
    }
    writeln!(out, "|")
}

impl<'a, W: Write> Renderer<'a, W> {
    pub fn new(out: &'a mut W, columns: Vec<Column>, options: Options) -> io::Result<Self> {
        let names: Vec<_> = columns
            .iter()
            .map(|c| {
                if options.format == Format::Tsv {
                    escape(c.name_ref(), false)
                } else {
                    display_text(c.name_ref())
                }
            })
            .collect();
        let widths = names
            .iter()
            .map(|s| if options.names { display_width(s) } else { 0 })
            .collect();
        let renderer = Self {
            out,
            columns,
            names,
            widths,
            options,
            store: RowStore::new(8 * 1024 * 1024),
            count: 0,
            started: false,
        };
        if options.format == Format::Tsv && options.names {
            for (i, c) in renderer.columns.iter().enumerate() {
                if i > 0 {
                    renderer.out.write_all(b"\t")?;
                }
                if options.raw {
                    renderer.out.write_all(c.name_ref())?;
                } else {
                    renderer
                        .out
                        .write_all(escape(c.name_ref(), false).as_bytes())?;
                }
            }
            writeln!(renderer.out)?;
        }
        Ok(renderer)
    }
    pub fn row(&mut self, row: &mysql::Row) -> io::Result<()> {
        self.count += 1;
        if self.options.format == Format::Tsv {
            for (i, c) in self.columns.iter().enumerate() {
                if i > 0 {
                    self.out.write_all(b"\t")?;
                }
                let value = row
                    .as_ref(i)
                    .ok_or_else(|| io::Error::other("missing result field"))?;
                let data = field_bytes(value, c, self.options.raw)?;
                if self.options.binary_hex && binary(c) && !matches!(value, Value::NULL) {
                    self.out.write_all(hex(&data).as_bytes())?;
                } else if self.options.raw {
                    self.out.write_all(&data)?;
                } else {
                    self.out.write_all(escape(&data, false).as_bytes())?;
                }
            }
            writeln!(self.out)?;
            if self.count == 1 {
                self.out.flush()?;
            }
            return Ok(());
        }
        let values: Vec<String> = self
            .columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let value = row.as_ref(i).expect("driver returned a short row");
                let data = field_bytes(value, c, false)?;
                Ok(
                    if self.options.binary_hex && binary(c) && !matches!(value, Value::NULL) {
                        hex(&data)
                    } else {
                        display_text(&data)
                    },
                )
            })
            .collect::<io::Result<_>>()?;
        if self.options.format == Format::Vertical {
            writeln!(
                self.out,
                "*************************** {}. row ***************************",
                self.count
            )?;
            let width = self
                .names
                .iter()
                .map(|s| display_width(s))
                .max()
                .unwrap_or(0);
            for (name, value) in self.names.iter().zip(values) {
                for _ in 0..width.saturating_sub(display_width(name)) {
                    self.out.write_all(b" ")?;
                }
                writeln!(self.out, "{name}: {value}")?;
            }
        } else if self.options.quick {
            if !self.started {
                for (width, value) in self.widths.iter_mut().zip(&values) {
                    *width = (*width).max(display_width(value));
                }
                border(self.out, &self.widths)?;
                if self.options.names {
                    table_row(
                        self.out,
                        &self.names,
                        &self.widths,
                        &vec![false; self.columns.len()],
                    )?;
                    border(self.out, &self.widths)?;
                }
                self.started = true;
            }
            table_row(
                self.out,
                &values,
                &self.widths,
                &self.columns.iter().map(numeric).collect::<Vec<_>>(),
            )?;
        } else {
            for (width, value) in self.widths.iter_mut().zip(&values) {
                *width = (*width).max(display_width(value));
            }
            self.store.push(values)?;
        }
        if self.count == 1 && (self.options.quick || self.options.format == Format::Vertical) {
            self.out.flush()?;
        }
        Ok(())
    }
    pub fn finish(mut self) -> io::Result<u64> {
        if self.options.format == Format::Table && self.count != 0 {
            if !self.options.quick {
                border(self.out, &self.widths)?;
                if self.options.names {
                    table_row(
                        self.out,
                        &self.names,
                        &self.widths,
                        &vec![false; self.columns.len()],
                    )?;
                    border(self.out, &self.widths)?;
                }
                let right: Vec<_> = self.columns.iter().map(numeric).collect();
                let (out, widths) = (&mut self.out, &self.widths);
                self.store.replay(self.columns.len(), |row| {
                    table_row(out, row, widths, &right)
                })?;
            }
            border(self.out, &self.widths)?;
        }
        Ok(self.count)
    }
}

pub fn terminal_text(text: &str) -> String {
    escape(text.as_bytes(), true)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn escapes_preserve_data_and_block_terminal_controls() {
        assert_eq!(escape(b"a\t\n\0\\", false), "a\\t\\n\\0\\\\");
        assert_eq!(escape(&[0xff, 0], false), "0xFF00");
        assert_eq!(escape(b"\x1b[2J", true), "\\u{1B}[2J");
        assert_eq!(UnicodeWidthStr::width("中文"), 4);
    }
    #[test]
    fn spilled_and_memory_results_are_identical() {
        let rows = vec![
            vec!["中文".into(), "a\\n".into()],
            vec!["NULL".into(), "".into()],
        ];
        for limit in [0, usize::MAX] {
            let mut s = RowStore::new(limit);
            for row in &rows {
                s.push(row.clone()).unwrap();
            }
            let mut replayed = vec![];
            s.replay(2, |row| {
                replayed.push(row.to_vec());
                Ok(())
            })
            .unwrap();
            assert_eq!(replayed, rows);
        }
    }
    #[test]
    fn vectors_are_typed_coordinates_and_raw_bytes_are_preserved() {
        let column = Column::new(ColumnType::MYSQL_TYPE_VECTOR);
        let data: Vec<u8> = [1.0f32, -2.5, 3.0]
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect();
        let value = Value::Bytes(data.clone());
        assert_eq!(
            field_bytes(&value, &column, false).unwrap().as_ref(),
            b"[1,-2.5,3]"
        );
        assert_eq!(field_bytes(&value, &column, true).unwrap().as_ref(), data);
        assert!(field_bytes(&Value::Bytes(vec![1]), &column, false).is_err());
        assert!(field_bytes(
            &Value::Bytes(f32::NAN.to_le_bytes().to_vec()),
            &column,
            false
        )
        .is_err());
    }
    #[test]
    fn human_text_preserves_whitespace_and_literal_backslashes() {
        let data = b"first\nsecond\tpath\\name\rnext\0end\x1b[2J";
        assert_eq!(
            display_text(data),
            "first\nsecond\tpath\\name\rnext end\\u{1B}[2J"
        );
        assert_eq!(display_width("a\nb"), 3);
        assert_eq!(display_width("中\n文"), 5);
        assert_eq!(display_width("a\r\nb"), 4);
        assert_eq!(escape(b"first\nsecond\\n", false), "first\\nsecond\\\\n");
    }

    #[test]
    fn multiline_table_replay_matches_memory_and_keeps_complete_values() {
        let column = Column::new(ColumnType::MYSQL_TYPE_VAR_STRING).with_name(b"value");
        let rows = [
            vec![display_text(b"first\nsecond\n")],
            vec![display_text("中\n\n文\\n".as_bytes())],
        ];
        let mut rendered = vec![];
        for limit in [0, usize::MAX] {
            let mut out = vec![];
            let options = Options {
                format: Format::Table,
                names: true,
                raw: false,
                binary_hex: false,
                quick: false,
            };
            let mut renderer = Renderer::new(&mut out, vec![column.clone()], options).unwrap();
            renderer.store.limit = limit;
            for row in &rows {
                renderer.widths[0] = renderer.widths[0].max(display_width(&row[0]));
                renderer.store.push(row.clone()).unwrap();
                renderer.count += 1;
            }
            renderer.finish().unwrap();
            let out = String::from_utf8(out).unwrap();
            assert!(out.contains("| first\nsecond\n |"));
            assert!(out.contains("中\n\n文\\n"));
            rendered.push(out);
        }
        assert_eq!(rendered[0], rendered[1]);
    }
}
