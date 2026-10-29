mod cancel;
mod completion;
mod config;
mod input;
mod output;

use config::Args;
use input::{Command, Scanner, SqlMode, Statement};
use mysql::{prelude::Queryable, Conn};
use output::{Format, Options, Renderer};
use rustyline::{error::ReadlineError, Editor};
use std::{
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, IsTerminal, Write},
    path::{Path, PathBuf},
    time::Instant,
};

#[derive(Debug)]
struct CliError {
    message: String,
    code: i32,
    fatal: bool,
    reported: bool,
}
type Result<T> = std::result::Result<T, CliError>;
impl CliError {
    fn input(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: 1,
            fatal: true,
            reported: false,
        }
    }
}
impl From<io::Error> for CliError {
    fn from(e: io::Error) -> Self {
        Self::input(e.to_string())
    }
}
impl From<mysql::Error> for CliError {
    fn from(e: mysql::Error) -> Self {
        match e {
            mysql::Error::MySqlError(e) => Self {
                message: format!(
                    "ERROR {} ({}): {}",
                    e.code,
                    e.state,
                    output::terminal_text(&e.message)
                ),
                code: 1,
                fatal: false,
                reported: false,
            },
            e => Self::input(format!("connection error: {e}")),
        }
    }
}

struct App {
    args: Args,
    conn: Conn,
    cancel: cancel::Cancellation,
    mode: SqlMode,
    database: String,
    server_version: String,
    interactive: bool,
    failed: bool,
    quit: bool,
    sources: Vec<PathBuf>,
    completions: Vec<String>,
    rehash_needed: bool,
    control_opts: mysql::Opts,
    options: Options,
}

impl App {
    fn connect(args: Args, interactive: bool) -> Result<Self> {
        let password = match &args.password {
            Some(Some(p)) => p.clone(),
            Some(None) => rpassword::prompt_password("Enter password: ")?,
            None => String::new(),
        };
        let control_opts: mysql::Opts = args.opts(&password, true).into();
        let mut conn = Conn::new(args.opts(&password, false))?;
        conn.query_drop("SET NAMES utf8mb4")?;
        conn.query_drop(
            "SET SESSION session_track_system_variables = '*', session_track_schema = ON",
        )?;
        let (database, sql_mode, server_version): (Option<String>, String, String) = conn
            .query_first("SELECT DATABASE(), @@SESSION.sql_mode, VERSION()")?
            .ok_or_else(|| CliError::input("missing initial session state"))?;
        let mut mode = SqlMode {
            tracking: true,
            ..Default::default()
        };
        mode.update(&sql_mode);
        let format = if args.vertical {
            Format::Vertical
        } else if args.table || interactive {
            Format::Table
        } else {
            Format::Tsv
        };
        let options = Options {
            format,
            names: !args.skip_column_names,
            raw: args.raw,
            binary_hex: args.binary_as_hex.unwrap_or(interactive),
            quick: args.quick,
        };
        let cancel = cancel::Cancellation::new(control_opts.clone()).map_err(CliError::input)?;
        Ok(Self {
            args,
            conn,
            cancel,
            mode,
            database: database.unwrap_or_default(),
            server_version,
            interactive,
            failed: false,
            quit: false,
            sources: vec![],
            completions: vec![],
            rehash_needed: true,
            control_opts,
            options,
        })
    }

    fn run_sql(&mut self, statement: Statement, out: &mut impl Write) -> Result<()> {
        let started = Instant::now();
        self.cancel.begin(self.conn.connection_id());
        let old_database = self.database.clone();
        let result = (|| -> Result<()> {
            let mut query = self.conn.query_iter(&statement.sql)?;
            query.enable_result_metadata();
            let mut warnings = 0;
            while let Some(mut set) = query.iter() {
                let columns = set.columns().as_ref().to_vec();
                let has_columns = !columns.is_empty();
                let mut options = self.options;
                if statement.vertical {
                    options.format = Format::Vertical;
                }
                let mut renderer = if has_columns {
                    match Renderer::new(out, columns, options) {
                        Ok(r) => Some(r),
                        Err(e) => {
                            set.abort();
                            return Err(e.into());
                        }
                    }
                } else {
                    None
                };
                while let Some(row) = set.next() {
                    let row = match row {
                        Ok(row) => row,
                        Err(e) => {
                            let e = CliError::from(e);
                            if e.fatal {
                                set.abort();
                            }
                            return Err(e);
                        }
                    };
                    if !self.cancel.interrupted() {
                        if let Some(render) = &mut renderer {
                            if let Err(e) = render.row(&row) {
                                set.abort();
                                return Err(e.into());
                            }
                        }
                    }
                }
                let metadata = set.completed_result().cloned();
                let interrupted = self.cancel.interrupted();
                let count = if interrupted {
                    drop(renderer);
                    0
                } else {
                    match renderer {
                        Some(render) => match render.finish() {
                            Ok(n) => n,
                            Err(e) => {
                                set.abort();
                                return Err(e.into());
                            }
                        },
                        None => 0,
                    }
                };
                if let Some(meta) = metadata {
                    warnings = meta.warnings();
                    for change in meta.session_state_info()? {
                        use mysql::session_state_change::SessionStateChange;
                        match change.decode()? {
                            SessionStateChange::Schema(schema) => {
                                self.database = String::from_utf8(schema.as_bytes().to_vec())
                                    .map_err(|e| CliError::input(e.to_string()))?
                            }
                            SessionStateChange::SystemVariables(vars) => {
                                for var in vars {
                                    match var.name_str().to_ascii_lowercase().as_str() {
                                        "sql_mode" => self.mode.update(&var.value_str()),
                                        "session_track_system_variables" => {
                                            let value = var.value_str();
                                            self.mode.tracking = value == "*"
                                                || value.split(',').any(|s| {
                                                    s.trim().eq_ignore_ascii_case("sql_mode")
                                                });
                                        }
                                        _ => {}
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    self.mode.no_backslash = meta
                        .status_flags()
                        .contains(mysql::consts::StatusFlags::SERVER_STATUS_NO_BACKSLASH_ESCAPES);
                    if self.interactive && !interrupted {
                        let warning = if warnings > 0 {
                            format!(", {warnings} warning(s)")
                        } else {
                            String::new()
                        };
                        if has_columns {
                            writeln!(
                                out,
                                "{} ({:.3} sec){warning}\n",
                                if count == 0 {
                                    "Empty set".into()
                                } else {
                                    format!("{count} row(s) in set")
                                },
                                started.elapsed().as_secs_f64()
                            )?;
                        } else {
                            writeln!(
                                out,
                                "Query OK, {} row(s) affected ({:.3} sec){warning}",
                                meta.affected_rows(),
                                started.elapsed().as_secs_f64()
                            )?;
                            if let Some(info) = meta.info_str().filter(|s| !s.is_empty()) {
                                writeln!(out, "{}", output::terminal_text(&info))?;
                            }
                            writeln!(out)?;
                        }
                    }
                }
            }
            drop(query);
            if self.args.show_warnings && warnings > 0 && !self.cancel.interrupted() {
                let mut query = self.conn.query_iter("SHOW WARNINGS")?;
                let columns = query.columns().as_ref().to_vec();
                let mut renderer = match Renderer::new(out, columns, self.options) {
                    Ok(r) => r,
                    Err(e) => {
                        query.abort();
                        return Err(e.into());
                    }
                };
                for row in query.by_ref() {
                    let row = row?;
                    if let Err(e) = renderer.row(&row) {
                        query.abort();
                        return Err(e.into());
                    }
                }
                if let Err(e) = renderer.finish() {
                    query.abort();
                    return Err(e.into());
                }
            }
            out.flush()?;
            Ok(())
        })();
        let interrupted = self.cancel.finish();
        if self.database != old_database {
            self.rehash_needed = true;
        }
        if interrupted {
            return Err(CliError {
                message: "Query interrupted; completed changes are not automatically rolled back"
                    .into(),
                code: 130,
                fatal: !self.interactive,
                reported: false,
            });
        }
        result
    }

    fn rehash(&mut self) -> Result<()> {
        // Metadata uses a separate connection so completion cannot overwrite
        // the user's warnings, ROW_COUNT(), transaction or session variables.
        let mut conn = Conn::new(self.control_opts.clone())?;
        let databases: Vec<String> = conn.query("SHOW DATABASES")?;
        let mut names = databases;
        if !self.database.is_empty() {
            let schema = format!("`{}`", self.database.replace('`', "``"));
            let tables: Vec<String> = conn.query(format!("SHOW TABLES FROM {schema}"))?;
            for table in tables {
                let quoted = format!("`{}`", table.replace('`', "``"));
                let rows: Vec<mysql::Row> =
                    conn.query(format!("SHOW COLUMNS FROM {quoted} FROM {schema}"))?;
                names.push(table);
                for row in rows {
                    if let Some(Ok(column)) = row.get_opt::<String, _>(0) {
                        names.push(column);
                    }
                }
            }
        }
        self.completions = names;
        self.rehash_needed = false;
        Ok(())
    }

    fn handle_command(
        &mut self,
        cmd: Command<'_>,
        scanner: &mut Scanner,
        out: &mut impl Write,
    ) -> Result<()> {
        match cmd {
            Command::Quit => self.quit = true,
            Command::Clear => scanner.clear(),
            Command::Print => writeln!(out, "{}", output::terminal_text(scanner.text()))?,
            Command::Help => writeln!(out, "MyDB client commands:\n  help (\\h, \\?)       Show this help\n  quit, exit (\\q)      Exit\n  clear (\\c)           Clear SQL buffer\n  print (\\p)           Print SQL buffer\n  source (\\.) FILE     Execute a SQL file in this session\n  delimiter (\\d) TEXT  Change statement delimiter\n  status (\\s)          Show connection status\n  rehash (\\#)          Refresh completion names\n  \\g                   Execute SQL buffer\n  \\G                   Execute SQL buffer vertically")?,
            Command::Status => writeln!(out, "Connection: {}\nServer version: {}\nEndpoint: {}:{}\nUser: {}\nDatabase: {}\nTLS: {}\nClient charset: utf8mb4", self.conn.connection_id(), output::terminal_text(&self.server_version), output::terminal_text(&self.args.host), self.args.port, output::terminal_text(&self.args.user), output::terminal_text(&self.database), if self.conn.is_secure() { "enabled" } else { "disabled" })?,
            Command::Rehash => { self.rehash()?; if self.interactive { writeln!(out, "Completion names refreshed")?; } },
            Command::Delimiter(value) => { input::validate_delimiter(value).map_err(CliError::input)?; scanner.delimiter = value.to_owned(); },
            Command::Source(value) => {
                let value = if value.starts_with(['\'', '"']) && value.len() >= 2 && value.ends_with(value.as_bytes()[0] as char) { &value[1..value.len()-1] } else { value };
                if value.is_empty() { return Err(CliError::input("source requires a file path")); }
                if self.sources.len() >= 16 { return Err(CliError::input("source nesting exceeds 16 levels")); }
                let path = Path::new(value);
                let path = if path.is_relative() { self.sources.last().and_then(|p| p.parent()).unwrap_or(Path::new(".")).join(path) } else { path.to_path_buf() };
                let path = path.canonicalize().map_err(|e| CliError::input(format!("{}: {e}", path.display())))?;
                if self.sources.contains(&path) { return Err(CliError::input(format!("source cycle: {}", path.display()))); }
                let file = File::open(&path)?;
                self.sources.push(path.clone());
                let mut nested = Scanner::new(scanner.delimiter.clone());
                let result = self.read_input(BufReader::new(file), &mut nested, &path.display().to_string(), out);
                scanner.delimiter = nested.delimiter;
                self.sources.pop();
                result?;
            }
            Command::Unsupported(name) => return Err(CliError::input(format!("unsupported client command: {name}"))),
        }
        out.flush()?;
        Ok(())
    }

    fn report(&mut self, mut error: CliError, source: &str, line: usize) -> Result<()> {
        if !error.reported {
            eprintln!("mydb-cli: {source}:{line}: {}", error.message);
            error.reported = true;
        }
        self.failed = true;
        if error.fatal || ((!self.interactive || !self.sources.is_empty()) && !self.args.force) {
            return Err(error);
        }
        Ok(())
    }
    fn process(
        &mut self,
        scanner: &mut Scanner,
        eof: bool,
        source: &str,
        out: &mut impl Write,
    ) -> Result<()> {
        loop {
            match scanner.next(self.mode, eof) {
                Ok(Some(statement)) => {
                    let line = statement.line;
                    if let Err(error) = self.run_sql(statement, out) {
                        self.report(error, source, line)?;
                    }
                }
                Ok(None) => return Ok(()),
                Err(error) => {
                    scanner.clear();
                    return self.report(CliError::input(error.message), source, error.line);
                }
            }
        }
    }
    fn line(
        &mut self,
        line: &str,
        scanner: &mut Scanner,
        source: &str,
        out: &mut impl Write,
    ) -> Result<()> {
        if let Some(cmd) = input::command(line) {
            let allowed = scanner.idle()
                || (scanner.in_code()
                    && line.trim_start().starts_with('\\')
                    && matches!(
                        cmd,
                        Command::Clear
                            | Command::Print
                            | Command::Quit
                            | Command::Help
                            | Command::Status
                            | Command::Rehash
                    ));
            if allowed {
                if let Err(error) = self.handle_command(cmd, scanner, out) {
                    self.report(error, source, scanner.line())?;
                }
                // Preserve source line numbering even for local commands.
                scanner.feed("\n");
                self.process(scanner, false, source, out)?;
                return Ok(());
            }
        }
        scanner.feed(line);
        self.process(scanner, false, source, out)
    }
    fn read_input(
        &mut self,
        mut reader: impl BufRead,
        scanner: &mut Scanner,
        source: &str,
        out: &mut impl Write,
    ) -> Result<()> {
        let mut line = String::new();
        while !self.quit {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => self.line(&line, scanner, source, out)?,
                Err(e) => {
                    return self.report(CliError::input(e.to_string()), source, scanner.line())
                }
            }
        }
        if !self.quit {
            self.process(scanner, true, source, out)?;
        }
        Ok(())
    }
    fn repl(&mut self, out: &mut impl Write) -> Result<()> {
        let editor_config = rustyline::Config::builder()
            .max_history_size(1000)
            .map_err(|e| CliError::input(e.to_string()))?
            .history_ignore_dups(true)
            .map_err(|e| CliError::input(e.to_string()))?
            .history_ignore_space(true)
            .build();
        let mut editor =
            Editor::<completion::SqlHelper, rustyline::history::DefaultHistory>::with_config(
                editor_config,
            )
            .map_err(|e| CliError::input(e.to_string()))?;
        editor.set_helper(Some(completion::SqlHelper::new()));
        let history = if self.args.no_history {
            None
        } else {
            self.args
                .history_file
                .clone()
                .or_else(config::default_history)
        };
        if let Some(path) = &history {
            if path.exists() {
                if let Err(e) = editor.load_history(path) {
                    eprintln!("mydb-cli: history: {e}");
                }
            }
        }
        writeln!(out, "Welcome to the MyDB client. Commands end with ; or \\g.\nConnection id: {}.\nServer version: {}\nType 'help' for help.\n", self.conn.connection_id(), output::terminal_text(&self.server_version))?;
        out.flush()?;
        let mut scanner = Scanner::new(self.args.delimiter.clone());
        let mut history_entry = String::new();
        while !self.quit {
            if self.rehash_needed && !self.args.skip_auto_rehash {
                if let Err(e) = self.rehash() {
                    eprintln!("mydb-cli: completion: {}", e.message);
                    self.rehash_needed = false;
                }
            }
            editor
                .helper_mut()
                .unwrap()
                .update(self.completions.clone());
            let prompt = if scanner.idle() {
                if self.database.is_empty() {
                    "mydb> ".into()
                } else {
                    format!("mydb [{}]> ", output::terminal_text(&self.database))
                }
            } else {
                "    -> ".into()
            };
            match editor.readline(&prompt) {
                Ok(mut line) => {
                    history_entry.push_str(&line);
                    history_entry.push('\n');
                    line.push('\n');
                    if let Err(error) = self.line(&line, &mut scanner, "<terminal>", out) {
                        if error.fatal {
                            if matches!(error.code, 130)
                                || error.message.starts_with("connection error:")
                            {
                                return Err(error);
                            }
                            scanner.clear();
                        }
                    }
                    if scanner.idle() {
                        if history.is_some() && input::history_allowed(&history_entry) {
                            let _ = editor.add_history_entry(history_entry.trim_end());
                        }
                        history_entry.clear();
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    history_entry.clear();
                    scanner.clear();
                    writeln!(out, "^C")?;
                    out.flush()?;
                }
                Err(ReadlineError::Eof) => {
                    if !scanner.idle() {
                        eprintln!("mydb-cli: discarded unfinished SQL buffer");
                    }
                    break;
                }
                Err(e) => return Err(CliError::input(e.to_string())),
            }
        }
        if let Some(path) = history {
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .mode(0o600)
                    .open(&path);
                if let Err(e) = file {
                    eprintln!("mydb-cli: history: {e}");
                    return Ok(());
                }
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
            }
            if let Err(e) = editor.save_history(&path) {
                eprintln!("mydb-cli: history: {e}");
            }
        }
        writeln!(out, "Bye")?;
        out.flush()?;
        Ok(())
    }
}

fn run() -> Result<i32> {
    let args = Args::load().map_err(|e| CliError {
        message: e,
        code: 2,
        fatal: true,
        reported: false,
    })?;
    let interactive = io::stdin().is_terminal()
        && io::stdout().is_terminal()
        && args.execute.is_empty()
        && !args.batch;
    let mut app = App::connect(args, interactive)?;
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    if interactive {
        app.repl(&mut out)?;
        return Ok(0);
    }
    let mut scanner = Scanner::new(app.args.delimiter.clone());
    if app.args.execute.is_empty() {
        app.read_input(io::stdin().lock(), &mut scanner, "<stdin>", &mut out)?;
    } else {
        for (i, text) in std::mem::take(&mut app.args.execute)
            .into_iter()
            .enumerate()
        {
            scanner = Scanner::new(scanner.delimiter.clone());
            app.read_input(
                io::Cursor::new(text),
                &mut scanner,
                &format!("<execute:{}>", i + 1),
                &mut out,
            )?;
            if app.quit {
                break;
            }
        }
    }
    out.flush()?;
    Ok(if app.failed { 1 } else { 0 })
}

fn main() {
    match run() {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            if !error.reported {
                eprintln!("mydb-cli: {}", error.message);
            }
            std::process::exit(error.code);
        }
    }
}
