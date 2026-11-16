use super::*;

#[test]
fn single_pass_separator_matches_canonical_lexer_all_modes() {
    let fragments = [
        "",
        " ",
        ";",
        ";;",
        "SELECT 1",
        "INSERT INTO t VALUES ('a;b')",
        "'a;''b'",
        "'a\\';b'",
        "\"a;\\\"b\"",
        "`a;``b`",
        "'",
        "/*",
        "/* ; */",
        "/*+ ; */",
        "-- ;\n",
        "# ;\n",
        "--x;",
        "中;文",
        "/*! SELECT 2 */",
        "/*!80000 ; SELECT 2 */",
        "/*!99999 ; SELECT 2 */",
        "/*!80000 ' */",
        "/*!80000 */",
        "BEGIN SELECT 1; END",
    ];
    for ansi_quotes in [false, true] {
        for no_backslash_escapes in [false, true] {
            let mode = crate::sql_lex::SqlLexMode {
                ansi_quotes,
                no_backslash_escapes,
            };
            for a in fragments {
                for b in fragments {
                    for c in ["", ";", " 'x'", " /*x*/", " /*!80000 ; SELECT 3 */"] {
                        let sql = format!("{a}{b}{c}");
                        let expected = sql.as_bytes().contains(&b';')
                            && has_non_trailing_separator_with_executable_comments(&sql, mode);
                        assert_eq!(
                            has_non_trailing_statement_separator_with_mode(&sql, mode),
                            expected,
                            "sql={sql:?}, mode={mode:?}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn single_pass_separator_preserves_trailing_and_quoted_semicolons() {
    for sql in [
        "INSERT INTO t VALUES ('a;b','c;d')",
        "SELECT ';'; /* tail ; */",
        "SELECT 1; -- tail\n",
        "SELECT 1 /*!99999 ; SELECT 2 */",
    ] {
        assert!(
            !has_non_trailing_statement_separator_with_mode(sql, Default::default()),
            "{sql}"
        );
    }
    for sql in [
        "SELECT 1; SELECT 2",
        "SELECT 1; 'x'",
        "SELECT 1;;",
        "SELECT 1 /*!80000 ; SELECT 2 */",
        "SELECT ';",
        "SELECT 1; /*",
    ] {
        assert!(
            has_non_trailing_statement_separator_with_mode(sql, Default::default()),
            "{sql}"
        );
    }
}
