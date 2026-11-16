use super::support::query_rows;
use super::*;

#[test]
fn collated_constant_in_matches_comparison_disjunction_and_native_analysis() {
    std::fs::create_dir_all("/tmp/mydb").unwrap();
    for collation in [
        "utf8mb4_bin",
        "utf8mb4_0900_bin",
        "utf8mb4_0900_ai_ci",
        "utf8mb4_0900_as_cs",
    ] {
        let dir = tempfile::tempdir_in("/tmp/mydb").unwrap();
        let engine = open_test_engine(dir.path());
        engine
            .execute(&format!(
                "CREATE TABLE words (id INT, s VARCHAR(32) COLLATE {collation})"
            ))
            .unwrap();
        engine.execute("INSERT INTO words VALUES (1,'Alpha'),(2,'alpha'),(3,'á'),(4,'A '),(5,'汉字-尾'),(6,''),(7,NULL),(8,'ZZ'),(9,'zz ')").unwrap();
        for tested in ["s", "SUBSTRING(s,1,2)", "SUBSTR(s,-2,2)", "MID(s,0,2)"] {
            for literals in [
                vec!["Al", "a", "汉字", "ZZ"],
                vec!["a", "A "],
                vec!["", "é", "中", "z"],
            ] {
                for with_null in [false, true] {
                    let mut list = literals
                        .iter()
                        .map(|s| format!("'{s}'"))
                        .collect::<Vec<_>>();
                    if with_null {
                        list.push("NULL".into());
                    }
                    let disjunction = list
                        .iter()
                        .map(|s| format!("{tested}={s}"))
                        .collect::<Vec<_>>()
                        .join(" OR ");
                    for negated in [false, true] {
                        let sql = format!(
                            "SELECT id FROM words WHERE {tested} {}IN ({}) ORDER BY id",
                            if negated { "NOT " } else { "" },
                            list.join(",")
                        );
                        let oracle = format!(
                            "SELECT id FROM words WHERE {}({disjunction}) ORDER BY id",
                            if negated { "NOT " } else { "" }
                        );
                        assert_eq!(
                            query_rows(&engine, &sql),
                            query_rows(&engine, &oracle),
                            "{collation}: {sql}"
                        );
                    }
                }
            }
        }
        let plan = engine.execute("EXPLAIN ANALYZE FORMAT=TREE SELECT id FROM words WHERE SUBSTRING(s,1,2) IN ('Al','a','汉字','ZZ')").unwrap();
        let QueryResponse::ResultSet(rs) = plan else {
            panic!("expected analyze result")
        };
        let text = rs
            .rows
            .iter()
            .flatten()
            .map(Value::to_display_string)
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            text.contains("native_expression_rows="),
            "{collation}: {text}"
        );
        assert!(
            !text.contains("fallback_expression_rows="),
            "{collation}: {text}"
        );
    }
}
