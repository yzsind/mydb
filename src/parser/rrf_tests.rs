use super::*;

fn fusion(sql: &str) -> RrfFusion {
    struct Find(Option<RrfFusion>);
    impl visit::AstVisitor for Find {
        fn enter_query(&mut self, query: &QueryExpr) {
            if let QueryExpr::Rrf(rrf) = query {
                self.0 = Some((**rrf).clone());
            }
        }
    }
    let statement = parse_statement(sql).unwrap();
    let mut find = Find(None);
    visit::visit_statement(&mut find, &statement);
    find.0.expect("RRF syntax must create a typed node")
}

#[test]
fn rrf_parser_nary_options_and_lexical_parameters() {
    let rrf=fusion("SELECT * FROM ((SELECT ? AS id ORDER BY id LIMIT ?) AS lexical RRF (SELECT ? AS id ORDER BY id LIMIT ?) AS dense RRF (SELECT ? AS id ORDER BY id LIMIT ?) WITH (RANK_CONSTANT=?,WEIGHTS=(?,1,?),SCORE_DETAILS=TRUE)) h");
    assert_eq!(rrf.branches.len(), 3);
    assert_eq!(rrf.branches[0].name, "lexical");
    assert_eq!(rrf.branches[2].name, "branch_3");
    assert_eq!(rrf.rank_constant, Expr::Placeholder(6));
    assert_eq!(rrf.weights[0], Expr::Placeholder(7));
    assert_eq!(rrf.weights[2], Expr::Placeholder(8));
    assert!(rrf.score_details);
}

#[test]
fn rrf_parser_rejects_ambiguous_or_incomplete_contracts() {
    for sql in [
        "(SELECT 1 AS id LIMIT 1) RRF (SELECT 2 AS id ORDER BY id LIMIT 1)",
        "(SELECT 1 AS id ORDER BY id) RRF (SELECT 2 AS id ORDER BY id LIMIT 1)",
        "(SELECT 1 AS id ORDER BY id LIMIT 1) AS x RRF (SELECT 2 AS id ORDER BY id LIMIT 1) AS X",
        "(SELECT 1 AS id ORDER BY id LIMIT 1) RRF (SELECT 2 AS id ORDER BY id LIMIT 1) WITH (WEIGHTS=(1))",
        "(SELECT 1 AS id ORDER BY id LIMIT 1) RRF (SELECT 2 AS id ORDER BY id LIMIT 1) WITH (SCORE_DETAILS=?)",
        "(SELECT 1 AS id ORDER BY id LIMIT 1) RRF (SELECT 2 AS id ORDER BY id LIMIT 1) WITH (UNKNOWN=1)",
        "(SELECT 1 AS id ORDER BY id LIMIT 1) RRF (SELECT 2 AS id ORDER BY id LIMIT 1) WITH (RANK_CONSTANT=1,RANK_CONSTANT=2)",
    ] { assert!(parse_statement(sql).is_err(),"{sql}"); }
}

#[test]
fn rrf_identifier_and_derived_alias_remain_ordinary_names() {
    for sql in [
        "SELECT rrf FROM rrf AS rrf",
        "SELECT 'RRF (SELECT 1)' AS rrf",
        "SELECT * FROM (SELECT 1 AS id) rrf(id)",
        "SELECT * FROM (SELECT 1 AS id) AS rrf(id)",
    ] {
        parse_statement(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}
