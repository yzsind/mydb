use rustyline::{
    completion::{Completer, FilenameCompleter, Pair},
    highlight::Highlighter,
    hint::Hinter,
    validate::Validator,
    Context, Helper,
};

pub struct SqlHelper {
    pub words: Vec<String>,
    files: FilenameCompleter,
}
impl SqlHelper {
    pub fn new() -> Self {
        Self {
            words: KEYWORDS.split_whitespace().map(str::to_owned).collect(),
            files: FilenameCompleter::new(),
        }
    }
    pub fn update(&mut self, names: impl IntoIterator<Item = String>) {
        self.words = KEYWORDS.split_whitespace().map(str::to_owned).collect();
        self.words.extend(
            names
                .into_iter()
                .filter(|s| !s.chars().any(char::is_control)),
        );
        self.words.sort();
        self.words.dedup();
    }
}
const KEYWORDS: &str = "SELECT FROM WHERE INSERT INTO VALUES UPDATE DELETE CREATE ALTER DROP TABLE DATABASE INDEX VIEW PROCEDURE FUNCTION CALL BEGIN COMMIT ROLLBACK SET SHOW DESCRIBE EXPLAIN USE JOIN LEFT RIGHT INNER OUTER ON GROUP BY ORDER HAVING LIMIT OFFSET UNION ALL DISTINCT AS AND OR NOT NULL IS IN EXISTS CASE WHEN THEN ELSE END WITH RECURSIVE COUNT SUM AVG MIN MAX JSON VECTOR help quit exit clear print source delimiter status rehash";
impl Helper for SqlHelper {}
impl Highlighter for SqlHelper {}
impl Hinter for SqlHelper {
    type Hint = String;
}
impl Validator for SqlHelper {}
impl Completer for SqlHelper {
    type Candidate = Pair;
    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let head = line[..pos].trim_start().to_ascii_lowercase();
        if head.starts_with("source ") || head.starts_with("\\. ") {
            return self.files.complete(line, pos, ctx);
        }
        let start = line[..pos]
            .rfind(|c: char| !c.is_alphanumeric() && c != '_' && c != '`')
            .map_or(0, |i| i + 1);
        let prefix = line[start..pos]
            .trim_start_matches('`')
            .to_ascii_lowercase();
        let quoted = line[start..pos].starts_with('`');
        let pairs = self
            .words
            .iter()
            .filter(|s| s.to_ascii_lowercase().starts_with(&prefix))
            .map(|s| {
                let replacement = if quoted || !s.chars().all(|c| c.is_alphanumeric() || c == '_') {
                    format!("`{}`", s.replace('`', "``"))
                } else {
                    s.clone()
                };
                Pair {
                    display: s.clone(),
                    replacement,
                }
            })
            .collect();
        Ok((start, pairs))
    }
}
