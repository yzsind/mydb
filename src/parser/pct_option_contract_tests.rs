use super::*;

#[test]
fn percentage_option_numbers_preserve_consumption_and_boundaries() {
    assert_eq!(
        strip_pctfree_pctused("PCTFREE 0   ,PCTUSED=50  ,ROW_FORMAT=DYNAMIC").unwrap(),
        ("ROW_FORMAT=DYNAMIC".into(), Some(0), Some(50))
    );
    assert_eq!(
        strip_pctfree_pctused("PCTFREE=12, DEFAULT CHARSET=utf8mb4").unwrap(),
        (" DEFAULT CHARSET=utf8mb4".into(), Some(12), None)
    );
    for raw in ["", "-1", "１２", "4294967296"] {
        let sql = format!("PCTFREE={raw}");
        let error = strip_pctfree_pctused(&sql).unwrap_err();
        assert!(error
            .to_string()
            .contains("PCTFREE expects an integer literal between 0 and 50"));
    }
    for raw in ["51", "4294967295"] {
        let error = strip_pctfree_pctused(&format!("PCTUSED={raw}")).unwrap_err();
        assert!(error
            .to_string()
            .contains(&format!("PCTUSED={raw} out of range")));
    }
}

#[test]
fn create_percentage_scanning_keeps_literal_identifier_and_ctas_admission() {
    for sql in [
        "CREATE TABLE pctfree (pctused INT, pctfree INT)",
        "CREATE TABLE dst AS SELECT PCTFREE, 'PCTUSED=9' FROM src",
    ] {
        let (kept, values) = strip_create_table_pctfree_pctused(sql, true).unwrap();
        assert_eq!(kept, sql);
        assert_eq!((values.pctfree, values.pctused), (None, None));
    }
    let sql = "CREATE TABLE t (x VARCHAR(20) DEFAULT '汉🙂PCTFREE=7') PCTFREE = 12,";
    let (kept, values) = strip_create_table_pctfree_pctused(sql, true).unwrap();
    assert_eq!(
        kept,
        "CREATE TABLE t (x VARCHAR(20) DEFAULT '汉🙂PCTFREE=7') "
    );
    assert_eq!((values.pctfree, values.pctused), (Some(12), None));
    for raw in ["", "-1", "51", "4294967296"] {
        assert!(strip_create_table_pctfree_pctused(
            &format!("CREATE TABLE t (x INT) PCTFREE={raw}"),
            true
        )
        .is_err());
    }
    let (kept, values) = strip_create_table_pctfree_pctused(sql, false).unwrap();
    assert!(matches!(kept, std::borrow::Cow::Borrowed(_)));
    assert_eq!(kept, sql);
    assert_eq!((values.pctfree, values.pctused), (None, None));
}
