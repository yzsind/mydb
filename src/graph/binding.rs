use super::*;
use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Node,
    Edge,
    Group,
    Path,
    Input,
}

type Scope<'a> = HashMap<&'a str, Kind>;

fn declare<'a>(scope: &mut Scope<'a>, name: &'a str, kind: Kind) -> Result<(), String> {
    if scope.get(name).is_some_and(|previous| *previous != kind) {
        return Err(format!("Incompatible uses of GQL variable '{name}'"));
    }
    scope.insert(name, kind);
    Ok(())
}

fn validate_expr(scope: &Scope<'_>, expr: &Expression) -> Result<(), String> {
    let need = |name: &str, kinds: &[Kind]| match scope.get(name) {
        None => Err(format!("Unbound GQL variable '{name}'")),
        Some(kind) if kinds.is_empty() || kinds.contains(kind) => Ok(()),
        _ => Err(format!("Invalid type for GQL variable '{name}'")),
    };
    match expr {
        Expression::Count(arg) => arg
            .as_ref()
            .map_or(Ok(()), |expr| validate_expr(scope, expr)),
        Expression::Atom(_) => Ok(()),
        Expression::Record(fields) => fields
            .iter()
            .try_for_each(|(_, value)| validate_expr(scope, value)),
        Expression::Variable(name) => need(name, &[]),
        Expression::Property(name, _) => need(name, &[Kind::Node, Kind::Edge, Kind::Input]),
        Expression::ElementId(name) => need(name, &[Kind::Node, Kind::Edge]),
        Expression::PathLength(name) => need(name, &[Kind::Path]),
        Expression::Cast(expr, _) | Expression::Not(expr) | Expression::IsNull(expr, _) => {
            validate_expr(scope, expr)
        }
        Expression::Binary(a, _, b) => {
            validate_expr(scope, a)?;
            validate_expr(scope, b)
        }
    }
}

fn element<'a>(
    scope: &mut Scope<'a>,
    element: &'a Element,
    kind: Kind,
    insert: bool,
) -> Result<(), String> {
    if !insert {
        if let Some(variable) = &element.variable {
            declare(scope, variable, kind)?;
        }
    }
    for (_, expr) in &element.properties {
        validate_expr(scope, expr)?;
    }
    if let Some(predicate) = &element.predicate {
        validate_expr(scope, predicate)?;
    }
    if insert {
        if let Some(variable) = &element.variable {
            if scope.contains_key(variable.as_str())
                && (kind == Kind::Edge
                    || !element.labels.is_empty()
                    || !element.properties.is_empty())
            {
                return Err(format!("INSERT cannot redefine GQL variable '{variable}'"));
            }
            declare(scope, variable, kind)?;
        }
    }
    Ok(())
}

fn pattern<'a>(scope: &mut Scope<'a>, pattern: &'a Pattern, insert: bool) -> Result<(), String> {
    element(scope, &pattern.start, Kind::Node, insert)?;
    for step in &pattern.steps {
        if step.quantified {
            let mut inner = scope.clone();
            element(&mut inner, &step.edge, Kind::Edge, false)?;
            if let Some(variable) = &step.edge.variable {
                declare(scope, variable, Kind::Group)?;
            }
        } else {
            element(scope, &step.edge, Kind::Edge, insert)?;
        }
        element(scope, &step.node, Kind::Node, insert)?;
    }
    if let Some(name) = &pattern.path {
        declare(scope, name, Kind::Path)?;
    }
    Ok(())
}

pub(super) fn validate(program: &Program) -> Result<(), String> {
    let mut scope = Scope::new();
    for clause in &program.clauses {
        match clause {
            Clause::For(name, values) => {
                for expr in values {
                    validate_expr(&scope, expr)?;
                }
                if scope.contains_key(name.as_str()) {
                    return Err(format!("Duplicate GQL binding '{name}'"));
                }
                declare(&mut scope, name, Kind::Input)?;
            }
            Clause::Match {
                patterns,
                predicate,
                ..
            } => {
                for item in patterns {
                    pattern(&mut scope, item, false)?;
                }
                if let Some(expr) = predicate {
                    validate_expr(&scope, expr)?;
                }
            }
            Clause::Insert(patterns) => {
                for item in patterns {
                    pattern(&mut scope, item, true)?;
                }
            }
            Clause::Set(changes) | Clause::Remove(changes) => {
                for change in changes {
                    let name = match change {
                        Change::Label { variable, .. } => variable,
                        Change::Property {
                            variable, value, ..
                        } => {
                            if let Some(expr) = value {
                                validate_expr(&scope, expr)?;
                            }
                            variable
                        }
                    };
                    validate_mutation(&scope, name)?;
                }
            }
            Clause::Delete { variables, .. } => {
                for name in variables {
                    validate_mutation(&scope, name)?;
                }
            }
        }
    }
    if let Some(projection) = &program.result {
        for (expression, _) in &projection.items {
            validate_expr(&scope, expression)?;
        }
        for (expression, _) in &projection.order {
            if matches!(expression, Expression::Variable(name) if projection.items.iter().any(|(_, alias)| alias == name))
            {
                continue;
            }
            if projection
                .items
                .iter()
                .any(|(item, _)| matches!(item, Expression::Count(_)))
                && !projection.items.iter().any(|(item, _)| item == expression)
            {
                return Err("Grouped ORDER BY requires a projected expression or alias".into());
            }
            validate_expr(&scope, expression)?;
        }
        if let Some(limit) = &projection.limit {
            validate_expr(&Scope::new(), limit)?;
        }
    }
    Ok(())
}

fn validate_mutation(scope: &Scope<'_>, name: &str) -> Result<(), String> {
    if matches!(scope.get(name), Some(Kind::Node | Kind::Edge)) {
        Ok(())
    } else {
        Err(format!(
            "GQL mutation requires a bound node or edge variable, got '{name}'"
        ))
    }
}
