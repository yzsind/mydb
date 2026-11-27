use super::storage::{ElementRef, GraphStore};
use super::*;
use crate::graph::Operator;
use serde_json::Value as Json;
use smallvec::SmallVec;

#[derive(Clone, Debug, PartialEq)]
pub(super) enum Bound {
    Null,
    Scalar(Json),
    Element(ElementRef),
    Group(Vec<ElementRef>),
    Path(Path),
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Path {
    pub nodes: Vec<u64>,
    pub edges: Vec<ElementRef>,
}

#[derive(Clone)]
pub(super) struct Bindings<'p>(SmallVec<[(&'p str, Bound); 3]>);

impl<'p> Bindings<'p> {
    pub fn new() -> Self {
        Self(SmallVec::new())
    }

    pub fn get(&self, name: &str) -> Option<&Bound> {
        self.0
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| value)
    }

    pub fn contains_key(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    pub fn insert(&mut self, name: &'p str, value: Bound) {
        debug_assert!(!self.contains_key(name));
        self.0.push((name, value));
    }

    pub fn iter(&self) -> impl Iterator<Item = &(&'p str, Bound)> {
        self.0.iter()
    }
}

pub(super) fn eval(
    store: &GraphStore<'_>,
    atoms: &[Expr],
    row: &Bindings<'_>,
    expr: &Expression,
) -> Result<Json, QueryError> {
    match expr {
        Expression::Atom(index) => {
            let atom = atoms
                .get(*index)
                .ok_or_else(|| invalid("Invalid GQL expression index"))?;
            if let Expr::Literal(crate::parser::Literal::Bool(value)) = atom {
                return Ok(Json::Bool(*value));
            }
            let value = match atom {
                Expr::Literal(value) => literal_value(value),
                Expr::Placeholder(index) => crate::engine::params::current_param(*index)
                    .ok_or_else(|| invalid("GQL parameter has no bound value"))?,
                Expr::BoundValue(value) => value.clone(),
                _ => return Err(invalid("Unsupported GQL expression")),
            };
            scalar(value)
        }
        Expression::Record(fields) => {
            let mut record = serde_json::Map::new();
            for (name, expression) in fields {
                if record
                    .insert(name.clone(), eval(store, atoms, row, expression)?)
                    .is_some()
                {
                    return Err(invalid("Duplicate GQL record field"));
                }
            }
            Ok(Json::Object(record))
        }
        Expression::Variable(name) => match binding(row, name)? {
            Bound::Null => Ok(Json::Null),
            Bound::Scalar(value) => Ok(value.clone()),
            Bound::Element(element) => store.materialize(*element),
            Bound::Group(elements) => {
                let values: Result<Vec<_>, _> = elements
                    .iter()
                    .map(|element| store.materialize(*element))
                    .collect();
                Ok(Json::Array(values?))
            }
            Bound::Path(path) => Ok(serde_json::json!({
                "nodes": path.nodes.iter().map(u64::to_string).collect::<Vec<_>>(),
                "edges": path.edges.iter().map(|edge| edge.id.to_string()).collect::<Vec<_>>(),
            })),
        },
        Expression::Property(variable, name) => match binding(row, variable)? {
            Bound::Null => Ok(Json::Null),
            Bound::Scalar(Json::Object(fields)) => {
                Ok(fields.get(name).cloned().unwrap_or_default())
            }
            Bound::Element(entry) => store.property(*entry, name),
            _ => Err(invalid("GQL property access requires a node or an edge")),
        },
        Expression::ElementId(name) => match binding(row, name)? {
            Bound::Null => Ok(Json::Null),
            Bound::Element(entry) => Ok(Json::String(entry.id.to_string())),
            _ => Err(invalid("ELEMENT_ID requires a node or an edge")),
        },
        Expression::PathLength(name) => match binding(row, name)? {
            Bound::Null => Ok(Json::Null),
            Bound::Path(path) => Ok(Json::from(path.edges.len())),
            _ => Err(invalid("PATH_LENGTH requires a path")),
        },
        Expression::Cast(expr, kind) => cast(eval(store, atoms, row, expr)?, *kind),
        Expression::Not(expr) => Ok(truth(&eval(store, atoms, row, expr)?)?
            .map(|value| !value)
            .map(Json::Bool)
            .unwrap_or_default()),
        Expression::IsNull(expr, not) => {
            Ok(Json::Bool(eval(store, atoms, row, expr)?.is_null() != *not))
        }
        Expression::Binary(left, op, right) => {
            let left = eval(store, atoms, row, left)?;
            if matches!(op, Operator::And | Operator::Or) {
                let left = truth(&left)?;
                let dominating = *op == Operator::Or;
                if left == Some(dominating) {
                    return Ok(Json::Bool(dominating));
                }
                let right = truth(&eval(store, atoms, row, right)?)?;
                if right == Some(dominating) {
                    return Ok(Json::Bool(dominating));
                }
                return Ok(left
                    .zip(right)
                    .map(|_| Json::Bool(!dominating))
                    .unwrap_or_default());
            }
            let right = eval(store, atoms, row, right)?;
            if left.is_null() || right.is_null() {
                return Ok(Json::Null);
            }
            let order = compare(&left, &right);
            let result = match op {
                Operator::Eq => order == Some(Ordering::Equal),
                Operator::Ne => order != Some(Ordering::Equal),
                Operator::Lt => order == Some(Ordering::Less),
                Operator::Le => matches!(order, Some(Ordering::Less | Ordering::Equal)),
                Operator::Gt => order == Some(Ordering::Greater),
                Operator::Ge => matches!(order, Some(Ordering::Greater | Ordering::Equal)),
                Operator::Or | Operator::And => unreachable!(),
            };
            Ok(Json::Bool(result))
        }
    }
}

pub(super) fn binding<'a>(row: &'a Bindings<'_>, name: &str) -> Result<&'a Bound, QueryError> {
    row.get(name)
        .ok_or_else(|| invalid(format!("Unbound GQL variable '{name}'")))
}

pub(super) fn truth(value: &Json) -> Result<Option<bool>, QueryError> {
    match value {
        Json::Null => Ok(None),
        Json::Bool(value) => Ok(Some(*value)),
        _ => Err(invalid("GQL predicate must evaluate to BOOLEAN")),
    }
}

fn scalar(value: Value) -> Result<Json, QueryError> {
    Ok(match value {
        Value::Null => Json::Null,
        Value::Int(value) => Json::from(value),
        Value::UInt(value) => Json::from(value),
        Value::Float(value) if value.is_finite() => Json::from(value),
        Value::Double(value) if value.is_finite() => Json::from(value),
        Value::String(value) => Json::String(value),
        Value::SmallString(value) => Json::String(
            std::str::from_utf8(&value)
                .map_err(|_| invalid("Invalid GQL string"))?
                .to_owned(),
        ),
        Value::Bytes(value) => Json::String(
            String::from_utf8(value).map_err(|_| invalid("GQL strings must be UTF-8"))?,
        ),
        _ => return Err(invalid("Unsupported GQL scalar parameter type")),
    })
}

fn cast(value: Json, kind: Cast) -> Result<Json, QueryError> {
    if value.is_null() {
        return Ok(value);
    }
    let converted = match kind {
        Cast::String => match value {
            Json::String(_) => Some(value),
            Json::Bool(_) | Json::Number(_) => Some(Json::String(value.to_string())),
            _ => None,
        },
        Cast::Int64 => value
            .as_i64()
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
            .or_else(|| {
                value
                    .as_f64()
                    .filter(|value| *value >= i64::MIN as f64 && *value < -(i64::MIN as f64))
                    .map(|value| value as i64)
            })
            .map(Json::from),
        Cast::Float64 => value
            .as_f64()
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
            .filter(|value| value.is_finite())
            .map(Json::from),
        Cast::Boolean => value
            .as_bool()
            .or_else(|| match value.as_str() {
                Some(text) if text.eq_ignore_ascii_case("true") => Some(true),
                Some(text) if text.eq_ignore_ascii_case("false") => Some(false),
                _ => None,
            })
            .map(Json::from),
    };
    converted.ok_or_else(|| invalid(format!("Cannot cast GQL value to {kind:?}")))
}

pub(super) fn wire(value: Json, kind: &ColumnType) -> Value {
    if value.is_null() {
        return Value::Null;
    }
    match kind {
        ColumnType::VarString => {
            Value::String(value.as_str().expect("typed GQL string").to_owned())
        }
        ColumnType::Tiny => Value::Int(i64::from(value.as_bool().expect("typed GQL boolean"))),
        ColumnType::LongLong => Value::Int(value.as_i64().expect("typed GQL integer")),
        ColumnType::Double => Value::Double(value.as_f64().expect("typed GQL float")),
        _ => Value::Json(crate::engine::json::from_serde(&value)),
    }
}

pub(super) fn compare(left: &Json, right: &Json) -> Option<Ordering> {
    match (left, right) {
        (Json::Null, Json::Null) => Some(Ordering::Equal),
        (Json::Number(a), Json::Number(b)) => match (integer(a), integer(b)) {
            (Some(a), Some(b)) => Some(a.cmp(&b)),
            (Some(a), None) => Some(compare_integer_float(a, b.as_f64()?)),
            (None, Some(b)) => Some(compare_integer_float(b, a.as_f64()?).reverse()),
            (None, None) => a.as_f64()?.partial_cmp(&b.as_f64()?),
        },
        (Json::Bool(a), Json::Bool(b)) => Some(a.cmp(b)),
        (Json::String(a), Json::String(b)) => Some(a.cmp(b)),
        (Json::Array(a), Json::Array(b)) if a == b => Some(Ordering::Equal),
        (Json::Object(a), Json::Object(b)) if a == b => Some(Ordering::Equal),
        _ => None,
    }
}

fn integer(value: &serde_json::Number) -> Option<i128> {
    value
        .as_i64()
        .map(i128::from)
        .or_else(|| value.as_u64().map(i128::from))
}

fn compare_integer_float(integer: i128, float: f64) -> Ordering {
    integer.cmp(&(float as i128)).then_with(|| {
        0.0f64
            .partial_cmp(&float.fract())
            .expect("JSON numbers are finite")
    })
}

pub(super) fn distinct_value(value: &Json) -> Json {
    match value {
        Json::Number(number) if number.is_f64() => {
            let float = number.as_f64().expect("JSON float");
            if float.fract() != 0.0 {
                return value.clone();
            }
            let integer = float as i128;
            i64::try_from(integer)
                .map(Json::from)
                .or_else(|_| u64::try_from(integer).map(Json::from))
                .unwrap_or_else(|_| value.clone())
        }
        Json::Array(values) => Json::Array(values.iter().map(distinct_value).collect()),
        Json::Object(values) => Json::Object(
            values
                .iter()
                .map(|(name, value)| (name.clone(), distinct_value(value)))
                .collect(),
        ),
        others => others.clone(),
    }
}

pub(super) fn sort_cmp(left: &Json, right: &Json) -> Ordering {
    let class = |value: &Json| match value {
        Json::Bool(_) => 0,
        Json::Number(_) => 1,
        Json::String(_) => 2,
        Json::Array(_) => 3,
        Json::Object(_) => 4,
        Json::Null => 5,
    };
    compare(left, right).unwrap_or_else(|| {
        class(left)
            .cmp(&class(right))
            .then_with(|| left.to_string().cmp(&right.to_string()))
    })
}
