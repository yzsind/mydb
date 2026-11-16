//! MySQL 8.0 comment lexical-semantics regressions.

use super::*;

#[test]
fn comment_text_cannot_trigger_select_into_or_index_visibility() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT /* INTO @comment_var */ 1")
        .unwrap()
    else {
        panic!("comment-only INTO must still return a result set");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(1)]]);
    assert_eq!(session.user_variable("comment_var"), None);

    engine
        .execute_with_session(&mut session, "CREATE TABLE comment_idx (a INT)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "CREATE INDEX comment_idx_a ON comment_idx(a) -- INVISIBLE",
        )
        .unwrap();
}

#[test]
fn comments_between_manual_parser_keywords_execute() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();

    engine
        .execute_with_session(
            &mut session,
            "CREATE /* audit */ USER 'comment_gap_user'@'%'",
        )
        .unwrap();

    engine
        .execute_with_session(&mut session, "CREATE TABLE load_comment_gap (id INT)")
        .unwrap();
    let input = dir.path().join("comment-load.tsv");
    std::fs::write(&input, b"7\n").unwrap();
    let sql = format!(
        "LOAD /* transport */ DATA INFILE '{}' INTO TABLE load_comment_gap",
        crate::util::file::sql_path(&input)
    );
    engine.execute_with_session(&mut session, &sql).unwrap();
    let QueryResponse::ResultSet(result) = engine
        .execute_with_session(&mut session, "SELECT id FROM load_comment_gap")
        .unwrap()
    else {
        panic!("expected loaded row");
    };
    assert_eq!(result.rows, vec![vec![Value::Int(7)]]);
}

#[test]
fn unterminated_block_comment_is_mysql_parse_error() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    let err = engine
        .execute_with_session(&mut session, "SELECT 1 /* unterminated")
        .unwrap_err();
    assert_eq!(err.code(), 1064, "{}", err.message());
}

#[test]
fn dml_optimizer_hint_diagnostics_reach_session_warnings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open_test_engine(dir.path());
    let mut session = SessionContext::default();
    engine
        .execute_with_session(
            &mut session,
            "CREATE TABLE hint_dml (id INT PRIMARY KEY, v INT)",
        )
        .unwrap();
    engine
        .execute_with_session(&mut session, "INSERT INTO hint_dml VALUES (1, 1)")
        .unwrap();
    engine
        .execute_with_session(
            &mut session,
            "UPDATE /*+ PARALLEL(4) */ hint_dml SET v = 2 WHERE id = 1",
        )
        .unwrap();
    assert!(session.warnings.iter().any(|warning| {
        warning.code() == 3125
            && warning
                .message()
                .contains("accepted but ignored for UPDATE")
    }));
}
