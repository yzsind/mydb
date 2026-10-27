#[derive(Clone, Copy, Debug, Default)]
pub struct SqlMode {
    pub ansi_quotes: bool,
    pub no_backslash: bool,
    pub tracking: bool,
}

impl SqlMode {
    pub fn update(&mut self, value: &str) {
        self.ansi_quotes = value
            .split(',')
            .any(|x| x.eq_ignore_ascii_case("ANSI_QUOTES") || x.eq_ignore_ascii_case("ANSI"));
        self.no_backslash = value
            .split(',')
            .any(|x| x.eq_ignore_ascii_case("NO_BACKSLASH_ESCAPES"));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Normal,
    Quote(u8),
    LineComment,
    BlockComment,
}

#[derive(Debug)]
pub struct ScanError {
    pub line: usize,
    pub message: String,
}

pub struct Statement {
    pub sql: String,
    pub vertical: bool,
    pub line: usize,
}

pub struct Scanner {
    buffer: String,
    pos: usize,
    state: State,
    has_code: bool,
    first_line: usize,
    scan_line: usize,
    pub delimiter: String,
}

pub fn validate_delimiter(value: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 32
        || !value.is_ascii()
        || value
            .bytes()
            .any(|b| b.is_ascii_whitespace() || matches!(b, b'\'' | b'"' | b'`' | b'\\' | 0))
    {
        return Err("delimiter must contain 1..32 non-whitespace ASCII characters, without quotes or backslashes".into());
    }
    if value.starts_with(['#', '-']) || value.starts_with("/*") {
        return Err("delimiter cannot start with a comment marker".into());
    }
    Ok(())
}

impl Scanner {
    pub fn new(delimiter: String) -> Self {
        Self {
            buffer: String::new(),
            pos: 0,
            state: State::Normal,
            has_code: false,
            first_line: 1,
            scan_line: 1,
            delimiter,
        }
    }
    pub fn feed(&mut self, text: &str) {
        self.buffer.push_str(text);
    }
    pub fn text(&self) -> &str {
        &self.buffer
    }
    pub fn in_code(&self) -> bool {
        self.state == State::Normal
    }
    pub fn idle(&self) -> bool {
        !self.has_code && self.state == State::Normal
    }
    pub fn line(&self) -> usize {
        self.scan_line
    }
    pub fn clear(&mut self) {
        self.scan_line += self.buffer[self.pos..]
            .bytes()
            .filter(|&b| b == b'\n')
            .count();
        self.buffer.clear();
        self.pos = 0;
        self.state = State::Normal;
        self.has_code = false;
    }
    fn finish(&mut self, end: usize, consumed: usize, vertical: bool) -> Option<Statement> {
        let statement = self.has_code.then(|| Statement {
            sql: self.buffer[..end].to_owned(),
            vertical,
            line: self.first_line,
        });
        self.buffer.drain(..consumed);
        self.pos = 0;
        self.state = State::Normal;
        self.has_code = false;
        statement
    }
    pub fn next(&mut self, mode: SqlMode, eof: bool) -> Result<Option<Statement>, ScanError> {
        while self.pos < self.buffer.len() {
            let bytes = self.buffer.as_bytes();
            let p = self.pos;
            let b = bytes[p];
            let next = bytes.get(p + 1).copied();
            if b == b'\n' {
                self.scan_line += 1;
            }
            match self.state {
                State::LineComment => {
                    if b == b'\n' {
                        self.state = State::Normal;
                    }
                }
                State::BlockComment => {
                    if b == b'*' && next == Some(b'/') {
                        self.pos += 2;
                        self.state = State::Normal;
                        continue;
                    }
                    if b == b'*' && next.is_none() && !eof {
                        break;
                    }
                }
                State::Quote(q) => {
                    let backslash =
                        q != b'`' && !(q == b'"' && mode.ansi_quotes) && !mode.no_backslash;
                    if b == b'\\' && backslash {
                        if next.is_none() && !eof {
                            break;
                        }
                        if next == Some(b'\n') {
                            self.scan_line += 1;
                        }
                        self.pos += if next.is_some() { 2 } else { 1 };
                        continue;
                    }
                    if b == q {
                        if next.is_none() && !eof {
                            break;
                        }
                        if next == Some(q) {
                            self.pos += 2;
                            continue;
                        }
                        self.state = State::Normal;
                    }
                }
                State::Normal => {
                    let rest = &bytes[p..];
                    if rest.starts_with(self.delimiter.as_bytes()) {
                        if let Some(statement) = self.finish(p, p + self.delimiter.len(), false) {
                            return Ok(Some(statement));
                        }
                        continue;
                    }
                    if !eof && self.delimiter.as_bytes().starts_with(rest) {
                        break;
                    }
                    if b == b'\\' {
                        match next {
                            Some(b'g' | b'G') => {
                                let vertical = next == Some(b'G');
                                if let Some(statement) = self.finish(p, p + 2, vertical) {
                                    return Ok(Some(statement));
                                }
                                continue;
                            }
                            Some(b'c') => {
                                self.finish(p, p + 2, false);
                                continue;
                            }
                            None if !eof => break,
                            _ => {
                                return Err(ScanError {
                                    line: self.scan_line,
                                    message: "unknown client command starting with backslash"
                                        .into(),
                                })
                            }
                        }
                    }
                    if b == b'#' {
                        self.state = State::LineComment;
                    } else if b == b'-' && next == Some(b'-') {
                        if p + 2 == bytes.len() && !eof {
                            break;
                        }
                        if bytes
                            .get(p + 2)
                            .is_none_or(|x| x.is_ascii_whitespace() || x.is_ascii_control())
                        {
                            self.state = State::LineComment;
                            self.pos += 2;
                            continue;
                        }
                    } else if b == b'/' && next == Some(b'*') {
                        if p + 2 >= bytes.len() && !eof {
                            break;
                        }
                        // Executable comments are SQL even when they are the only content.
                        if bytes.get(p + 2) == Some(&b'!') && !self.has_code {
                            self.has_code = true;
                            self.first_line = self.scan_line;
                        }
                        self.state = State::BlockComment;
                        self.pos += 2;
                        continue;
                    } else if matches!(b, b'/' | b'-') && next.is_none() && !eof {
                        break;
                    } else if matches!(b, b'\'' | b'"' | b'`') {
                        if b == b'"' && !mode.tracking && bytes[p..].contains(&b'\\') {
                            return Err(ScanError { line: self.scan_line, message: "ambiguous double-quoted input after SQL mode tracking was disabled; reconnect".into() });
                        }
                        self.state = State::Quote(b);
                        if !self.has_code {
                            self.has_code = true;
                            self.first_line = self.scan_line;
                        }
                    } else if !b.is_ascii_whitespace() && !self.has_code {
                        self.has_code = true;
                        self.first_line = self.scan_line;
                    }
                }
            }
            self.pos += 1;
        }
        if eof {
            if matches!(self.state, State::Quote(_) | State::BlockComment) {
                return Err(ScanError {
                    line: self.first_line,
                    message: "unterminated quote or block comment".into(),
                });
            }
            return Ok(self.finish(self.buffer.len(), self.buffer.len(), false));
        }
        // Discard complete trivia to keep comment-only scripts bounded.
        if self.idle() {
            self.buffer.clear();
            self.pos = 0;
        }
        Ok(None)
    }
}

pub enum Command<'a> {
    Help,
    Quit,
    Clear,
    Print,
    Source(&'a str),
    Delimiter(&'a str),
    Status,
    Rehash,
    Unsupported(&'a str),
}

pub fn command(line: &str) -> Option<Command<'_>> {
    let text = line.trim();
    let head = text.split_whitespace().next().unwrap_or("");
    if head.eq_ignore_ascii_case("delimiter") || head == "\\d" {
        return Some(Command::Delimiter(text[head.len()..].trim()));
    }
    let text = text.strip_suffix(';').unwrap_or(text).trim_end();
    let (name, arg) = text
        .split_once(char::is_whitespace)
        .map_or((text, ""), |(n, a)| (n, a.trim()));
    match name.to_ascii_lowercase().as_str() {
        "quit" | "exit" | "\\q" if arg.is_empty() => Some(Command::Quit),
        "help" | "\\h" | "\\?" | "?" if arg.is_empty() => Some(Command::Help),
        "clear" | "\\c" if arg.is_empty() => Some(Command::Clear),
        "print" | "\\p" if arg.is_empty() => Some(Command::Print),
        "status" | "\\s" if arg.is_empty() => Some(Command::Status),
        "rehash" | "\\#" if arg.is_empty() => Some(Command::Rehash),
        "source" | "\\." => Some(Command::Source(arg)),
        "delimiter" | "\\d" => Some(Command::Delimiter(arg)),
        "pager" | "nopager" | "tee" | "notee" | "edit" | "system" | "connect" => {
            Some(Command::Unsupported(name))
        }
        _ if name.starts_with('\\') && !matches!(name, "\\G" | "\\g") => {
            Some(Command::Unsupported(name))
        }
        _ => None,
    }
}

pub fn history_allowed(sql: &str) -> bool {
    if sql.starts_with(char::is_whitespace) {
        return false;
    }
    let lower = sql.to_ascii_lowercase();
    !["password", "identified", "secret", "token", "credential"]
        .iter()
        .any(|s| lower.contains(s))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn mode() -> SqlMode {
        SqlMode {
            tracking: true,
            ..Default::default()
        }
    }
    fn split(text: &str, delimiter: &str, mode: SqlMode) -> Vec<String> {
        let mut s = Scanner::new(delimiter.into());
        s.feed(text);
        let mut out = vec![];
        while let Some(stmt) = s.next(mode, true).unwrap() {
            out.push(stmt.sql.trim().into());
        }
        out
    }
    #[test]
    fn quoted_comments_unicode_and_terminators() {
        let out = split(
            "SELECT 'a;\\\'b', `x;y`, \"中;文\";# hi;\nSELECT 2 /*;*/;-- ;\nSELECT 3",
            ";",
            mode(),
        );
        assert_eq!(out.len(), 3);
        assert!(out[0].contains("中;文"));
        assert!(out[2].ends_with("SELECT 3"));
        assert_eq!(split("SELECT 'a'';b'; SELECT 1--2;", ";", mode()).len(), 2);
        assert_eq!(split("/*!80000 SET @a=1 */;", ";", mode()).len(), 1);
    }
    #[test]
    fn chunk_boundaries_and_incremental_work() {
        let text = "SELECT 'a'';b', `中`; /* c */ SELECT 2\\G";
        for size in 1..=text.len() {
            let mut s = Scanner::new(";".into());
            let mut out = vec![];
            let mut chunk = String::new();
            for c in text.chars() {
                chunk.push(c);
                if chunk.len() >= size {
                    s.feed(&chunk);
                    chunk.clear();
                    while let Some(x) = s.next(mode(), false).unwrap() {
                        out.push(x);
                    }
                }
            }
            s.feed(&chunk);
            while let Some(x) = s.next(mode(), true).unwrap() {
                out.push(x);
            }
            assert_eq!(out.len(), 2, "chunk {size}");
            assert!(out[1].vertical);
        }
    }
    #[test]
    fn modes_and_custom_delimiters() {
        assert_eq!(
            split(
                "CREATE PROCEDURE p() BEGIN SELECT ';'; SELECT 2; END$$SELECT 3$$",
                "$$",
                mode()
            )
            .len(),
            2
        );
        let mut m = mode();
        m.update("ANSI_QUOTES,NO_BACKSLASH_ESCAPES");
        assert_eq!(split("SELECT 'a\\';SELECT 2;", ";", m).len(), 2);
        assert_eq!(split("SELECT \"a\\\";SELECT 2;", ";", m).len(), 2);
        let mut s = Scanner::new(";".into());
        s.feed("SELECT 'x");
        assert!(s.next(mode(), true).is_err());
    }
    #[test]
    fn local_commands_and_history() {
        assert!(matches!(
            command("delimiter $$"),
            Some(Command::Delimiter("$$"))
        ));
        assert!(matches!(
            command("delimiter ;"),
            Some(Command::Delimiter(";"))
        ));
        assert!(matches!(
            command("source 'a b.sql'"),
            Some(Command::Source("'a b.sql'"))
        ));
        assert!(command("SELECT 'quit';").is_none());
        assert!(!history_allowed("ALTER USER a IDENTIFIED BY 'abc'"));
        assert!(!history_allowed(" SELECT 1"));
        assert!(history_allowed("SELECT 1"));
    }
}
