//! Typed RRF syntax conversion and conservative branch validation.
use super::*;

pub(super) fn convert(
    parsed: &adapter::rrf::ParsedRrf,
    sql: &str,
) -> Result<RrfFusion, ParseError> {
    let mut names = std::collections::HashSet::new();
    let mut branches = Vec::with_capacity(parsed.branches.len());
    for (name, query) in &parsed.branches {
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(parse_error_at_zero(format!(
                "RRF: duplicate branch name '{name}'"
            )));
        }
        let query = QueryExpr::try_from(convert::convert_query_ref(sql, query)?)?;
        validate_branch(&query)?;
        branches.push(RrfBranch {
            name: name.clone(),
            query,
        });
    }
    let rank_constant = parsed
        .rank_constant
        .as_ref()
        .map(convert::convert_expr)
        .transpose()?
        .unwrap_or(Expr::Literal(Literal::Int(60)));
    let weights = parsed
        .weights
        .as_ref()
        .map(|items| {
            items
                .iter()
                .map(convert::convert_expr)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_else(|| vec![Expr::Literal(Literal::Int(1)); branches.len()]);
    if weights.len() != branches.len() {
        return Err(parse_error_at_zero(
            "RRF: WEIGHTS must have one value per branch",
        ));
    }
    fn scalar(expr: &Expr) -> bool {
        match expr {
            Expr::Literal(_) | Expr::Placeholder(_) => true,
            Expr::Unary {
                op: UnaryOp::Neg,
                expr,
            } => scalar(expr),
            _ => false,
        }
    }
    if !scalar(&rank_constant) || weights.iter().any(|weight| !scalar(weight)) {
        return Err(parse_error_at_zero(
            "RRF: numeric options require literals or anonymous parameters",
        ));
    }
    Ok(RrfFusion {
        branches,
        rank_constant,
        weights,
        score_details: parsed.score_details,
    })
}

pub(crate) fn validate_branch(query: &QueryExpr) -> Result<(), ParseError> {
    use visit::{visit_query, AstVisitor};
    struct Check {
        invalid: Option<&'static str>,
    }
    impl AstVisitor for Check {
        fn enter_query(&mut self, query: &QueryExpr) {
            match query {
                QueryExpr::SetOp(_) | QueryExpr::Rrf(_) => {
                    self.invalid =
                        Some("RRF: nested fusion and set-operation branches are not supported")
                }
                QueryExpr::WithCte(with) if with.recursive => {
                    self.invalid = Some("RRF: recursive branches are not supported")
                }
                _ => {}
            }
        }
        fn enter_select(&mut self, select: &Select) {
            if select.for_update || select.lock_clause.is_some() {
                self.invalid = Some("RRF: locking reads are not supported");
            }
        }
        fn enter_expr(&mut self, expr: &Expr) {
            if matches!(expr, Expr::Function { name, .. } if [USER_VAR_ASSIGN_FUNCTION,"NEXTVAL","SETVAL","GET_LOCK","RELEASE_LOCK","RELEASE_ALL_LOCKS","LAST_INSERT_ID"].iter().any(|function|name.eq_ignore_ascii_case(function)))
            {
                self.invalid = Some("RRF: side effects are not supported");
            }
        }
    }
    let mut check = Check { invalid: None };
    visit_query(&mut check, query);
    if let Some(message) = check.invalid {
        return Err(parse_error_with_spec_at_zero(
            crate::error::catalog::ER_NOT_SUPPORTED_YET,
            message,
        ));
    }
    let select = match query {
        QueryExpr::Select(select) => select.as_ref(),
        QueryExpr::WithCte(with) => with
            .body
            .as_select()
            .ok_or_else(|| parse_error_at_zero("RRF: expected ordered SELECT"))?,
        _ => return Err(parse_error_at_zero("RRF: expected ordered SELECT")),
    };
    if select.order_by.is_empty() || select.limit.is_none() {
        return Err(parse_error_at_zero(
            "RRF: every branch requires ORDER BY and LIMIT",
        ));
    }
    if select
        .projection
        .iter()
        .any(|item| !matches!(item, SelectItem::Expr { .. }))
    {
        return Err(parse_error_at_zero(
            "RRF: project explicit identity expressions, not wildcards",
        ));
    }
    Ok(())
}

/// Validate input lineage before CTE materialization or view-backed reads.
/// Only fusion inputs are restricted; ordinary outer relational operations
/// and independent set operations retain their normal SQL semantics.
pub(crate) fn validate_statement<F: Fn(&str) -> bool>(
    statement: &Statement,
    forbidden_function: &F,
) -> Result<(), ParseError> {
    use std::collections::HashMap;
    type Scope = HashMap<String, bool>;
    fn scopes<F: Fn(&str) -> bool>(parent: &Scope, ctes: &[CteDef], function: &F) -> Scope {
        let mut scope = parent.clone();
        for cte in ctes {
            scope.insert(cte.name.to_ascii_lowercase(), cte.is_recursive);
        }
        for _ in 0..=ctes.len() {
            let mut changed = false;
            for cte in ctes {
                let name = cte.name.to_ascii_lowercase();
                if !scope[&name] && forbidden(&cte.query, &scope, function) {
                    scope.insert(name, true);
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        scope
    }
    fn references(select: &Select, scope: &Scope) -> bool {
        select.from.iter().enumerate().any(|(i, table)| {
            (i != 0 || select.derived.is_none())
                && scope.get(&table.name.to_ascii_lowercase()) == Some(&true)
        }) || select.joins.iter().any(|join| {
            join.derived.is_none()
                && scope.get(&join.table.name.to_ascii_lowercase()) == Some(&true)
        })
    }
    fn forbidden<F: Fn(&str) -> bool>(query: &QueryExpr, parent: &Scope, function: &F) -> bool {
        struct Inspect<'a, F> {
            scopes: Vec<Scope>,
            function: &'a F,
            found: bool,
        }
        impl<F: Fn(&str) -> bool> visit::AstVisitor for Inspect<'_, F> {
            fn enter_query(&mut self, query: &QueryExpr) {
                match query {
                    QueryExpr::SetOp(_) | QueryExpr::Rrf(_) => self.found = true,
                    QueryExpr::WithCte(with) => {
                        let next = scopes(self.scopes.last().unwrap(), &with.ctes, self.function);
                        self.scopes.push(next);
                        if with.recursive {
                            self.found = true;
                        }
                    }
                    _ => {}
                }
            }
            fn exit_query(&mut self, query: &QueryExpr) {
                if matches!(query, QueryExpr::WithCte(_)) {
                    self.scopes.pop();
                }
            }
            fn enter_select(&mut self, select: &Select) {
                self.found |= references(select, self.scopes.last().unwrap())
                    || select.for_update
                    || select.lock_clause.is_some();
            }
            fn enter_expr(&mut self, expr: &Expr) {
                if let Expr::Function { name, .. } = expr {
                    self.found |= (self.function)(name);
                }
            }
        }
        let mut inspect = Inspect {
            scopes: vec![parent.clone()],
            function,
            found: false,
        };
        visit::visit_query(&mut inspect, query);
        inspect.found
    }
    struct Validate<'a, F> {
        scopes: Vec<Scope>,
        function: &'a F,
        invalid: bool,
    }
    impl<F: Fn(&str) -> bool> visit::AstVisitor for Validate<'_, F> {
        fn enter_statement(&mut self, statement: &Statement) {
            if let Statement::WithCte(with) = statement {
                let next = scopes(self.scopes.last().unwrap(), &with.ctes, self.function);
                self.scopes.push(next);
            }
        }
        fn exit_statement(&mut self, statement: &Statement) {
            if matches!(statement, Statement::WithCte(_)) {
                self.scopes.pop();
            }
        }
        fn enter_query(&mut self, query: &QueryExpr) {
            match query {
                QueryExpr::WithCte(with) => {
                    let next = scopes(self.scopes.last().unwrap(), &with.ctes, self.function);
                    self.scopes.push(next);
                }
                QueryExpr::Rrf(rrf) => {
                    self.invalid |= rrf.branches.iter().any(|branch| {
                        forbidden(&branch.query, self.scopes.last().unwrap(), self.function)
                    });
                }
                _ => {}
            }
        }
        fn exit_query(&mut self, query: &QueryExpr) {
            if matches!(query, QueryExpr::WithCte(_)) {
                self.scopes.pop();
            }
        }
    }
    let mut validator = Validate {
        scopes: vec![Scope::new()],
        function: forbidden_function,
        invalid: false,
    };
    visit::visit_statement(&mut validator, statement);
    if validator.invalid {
        Err(parse_error_with_spec_at_zero(crate::error::catalog::ER_NOT_SUPPORTED_YET,"RRF inputs cannot depend on fusion, set-operation, recursive, locking or side-effecting sources"))
    } else {
        Ok(())
    }
}
