use mysql::{prelude::Queryable, Conn, OptsBuilder};

#[test]
#[ignore = "requires the local validation server"]
fn completed_select_warning_is_preserved() {
    let opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(14837)
        .user(Some("root"))
        .pass(Some("root"))
        .prefer_socket(false);
    let mut conn = Conn::new(opts).unwrap();
    let mut result = conn.query_iter("SELECT CAST('oops' AS UNSIGNED)").unwrap();
    result.enable_result_metadata();
    let mut set = result.iter().unwrap();
    for row in set.by_ref() {
        row.unwrap();
    }
    assert_eq!(
        set.completed_result().unwrap().warnings(),
        1,
        "the completed SELECT lost its EOF warning count"
    );
}

#[test]
#[ignore = "requires the local validation server"]
fn deprecate_eof_keeps_first_row() {
    let opts = OptsBuilder::new()
        .ip_or_hostname(Some("127.0.0.1"))
        .tcp_port(14837)
        .user(Some("root"))
        .pass(Some("root"))
        .prefer_socket(false)
        .additional_capabilities(mysql::consts::CapabilityFlags::CLIENT_DEPRECATE_EOF);
    let mut conn = Conn::new(opts).unwrap();
    assert_eq!(conn.query_first::<i32, _>("SELECT 42").unwrap(), Some(42));
}
