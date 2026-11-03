//! Shared scalar and column XPath evaluator.
use super::super::{int_value, numeric, string_value};
use super::document::{Document, Kind};
use super::xpath::{Axis, XExpr};
use crate::parser::{BinaryOp, Expr};
use crate::types::{QueryError, Value};

#[derive(Clone, Copy, Debug)]
pub(super) struct Match {
    pub id: usize,
    pub position: usize,
    pub size: usize,
}
enum ResultValue {
    Nodes(Vec<Match>),
    Scalar(Value),
}
pub(super) struct Evaluation<'a> {
    pub document: &'a Document,
    pub xml: &'a [u8],
    pub charset: &'static str,
    pub collation: u16,
}
impl Evaluation<'_> {
    fn scalar(&self, value: ResultValue) -> Result<Value, QueryError> {
        Ok(match value {
            ResultValue::Scalar(v) => v,
            ResultValue::Nodes(ids) => super::decode_owned(
                self.document.text(self.xml, ids.iter().map(|m| m.id)),
                self.charset,
            )?,
        })
    }
    fn boolean(&self, value: ResultValue) -> Result<bool, QueryError> {
        match value {
            ResultValue::Nodes(n) => Ok(n.len() == 1),
            ResultValue::Scalar(Value::Null) => Ok(false),
            ResultValue::Scalar(v) => Ok(numeric(&v)? != 0.0),
        }
    }
    pub fn extract(&self, expr: &XExpr) -> Result<Option<Vec<u8>>, QueryError> {
        match self.eval(expr, None)? {
            ResultValue::Nodes(n) => Ok(Some(self.document.text(self.xml, n.iter().map(|m| m.id)))),
            ResultValue::Scalar(Value::Null) => Ok(None),
            ResultValue::Scalar(v) => {
                super::encode_owned(&formatted_scalar(expr, v), self.charset).map(Some)
            }
        }
    }
    pub fn matches(&self, expr: &XExpr) -> Result<Option<Vec<Match>>, QueryError> {
        if !expr.nodeset() {
            return Ok(None);
        }
        Ok(match self.eval(expr, None)? {
            ResultValue::Nodes(n) => Some(n),
            _ => None,
        })
    }
    fn eval(&self, e: &XExpr, context: Option<Match>) -> Result<ResultValue, QueryError> {
        crate::util::stack::ensure_stack(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
            || self.eval_inner(e, context),
        )
    }
    fn eval_inner(&self, e: &XExpr, context: Option<Match>) -> Result<ResultValue, QueryError> {
        use ResultValue::{Nodes, Scalar};
        match e {
            XExpr::Literal(v) => Ok(Scalar(v.clone())),
            XExpr::Real(v, _) => Ok(Scalar(Value::Double(*v))),
            XExpr::Variable {
                name,
                user,
                offset: _,
            } => Ok(Scalar(if *user {
                super::super::session_user_variable(name).unwrap_or(Value::Null)
            } else {
                super::local_variable(name).unwrap_or(Value::Null)
            })),
            XExpr::Neg(e) => {
                let v = self.scalar(self.eval(e, context)?)?;
                Ok(Scalar(super::super::eval_negate(v)?))
            }
            XExpr::Union(a, b) => {
                let (Nodes(mut a), Nodes(b)) = (self.eval(a, context)?, self.eval(b, context)?)
                else {
                    unreachable!()
                };
                a.extend(b);
                a.sort_unstable_by_key(|n| n.id);
                a.dedup_by_key(|n| n.id);
                for (position, m) in a.iter_mut().enumerate() {
                    m.position = position;
                    m.size = 0;
                }
                Ok(Nodes(a))
            }
            XExpr::Path { base, root, steps } => {
                let mut current = if let Some(b) = base {
                    let Nodes(n) = self.eval(b, context)? else {
                        unreachable!()
                    };
                    n
                } else {
                    vec![if *root {
                        Match {
                            id: 0,
                            position: 0,
                            size: 0,
                        }
                    } else {
                        context.unwrap_or(Match {
                            id: 0,
                            position: 0,
                            size: 0,
                        })
                    }]
                };
                let mut next = Vec::new();
                for step in steps {
                    crate::engine::cancel::check_cancelled()?;
                    next.clear();
                    for m in &current {
                        let matches = |id| self.document.name_matches(self.xml, id, &step.name);
                        let mut position = 0;
                        let mut push = |id| {
                            next.push(Match {
                                id,
                                position,
                                size: 0,
                            });
                            position += 1;
                        };
                        let node = &self.document.nodes[m.id];
                        match step.axis {
                            Axis::Identity => {
                                next.push(*m);
                            }
                            Axis::SelfNode => {
                                if matches(m.id) {
                                    push(m.id)
                                }
                            }
                            Axis::Parent => {
                                if m.id != 0 && matches(node.parent) {
                                    push(node.parent)
                                }
                            }
                            Axis::Child | Axis::Attribute => {
                                let kind = if matches!(step.axis, Axis::Child) {
                                    Kind::Element
                                } else {
                                    Kind::Attribute
                                };
                                let mut i = m.id + 1;
                                while i < node.end {
                                    let n = &self.document.nodes[i];
                                    if n.kind == kind && matches(i) {
                                        push(i);
                                    }
                                    i = n.end;
                                }
                            }
                            Axis::Descendant | Axis::DescendantSelf => {
                                if matches!(step.axis, Axis::DescendantSelf) && matches(m.id) {
                                    push(m.id);
                                }
                                for i in m.id + 1..node.end {
                                    if i & 1023 == 0 {
                                        crate::engine::cancel::check_cancelled()?;
                                    }
                                    if self.document.nodes[i].kind == Kind::Element && matches(i) {
                                        push(i);
                                    }
                                }
                            }
                            Axis::Ancestor | Axis::AncestorSelf => {
                                if matches!(step.axis, Axis::AncestorSelf) && matches(m.id) {
                                    push(m.id);
                                }
                                let mut i = node.parent;
                                while i != 0 {
                                    if matches(i) {
                                        push(i);
                                    }
                                    i = self.document.nodes[i].parent;
                                }
                            }
                        }
                    }
                    if matches!(
                        step.axis,
                        Axis::Ancestor | Axis::AncestorSelf | Axis::Parent
                    ) {
                        let count = next.len();
                        next.sort_unstable_by_key(|m| m.id);
                        next.dedup_by_key(|m| m.id);
                        for (i, m) in next.iter_mut().enumerate() {
                            m.position = if matches!(step.axis, Axis::Parent) {
                                i
                            } else {
                                count.saturating_sub(i + 1)
                            };
                        }
                    }
                    std::mem::swap(&mut next, &mut current);
                    for predicate in &step.predicates {
                        next.clear();
                        let count = current.len();
                        for m in &current {
                            let ctx = Match {
                                id: m.id,
                                position: m.position,
                                size: count,
                            };
                            let result = self.eval(predicate, Some(ctx))?;
                            let keep = if predicate.boolean() || predicate.nodeset() {
                                self.boolean(result)?
                            } else {
                                let v = self.scalar(result)?;
                                let n = match &v {
                                    Value::Double(n) => n.round_ties_even() as i64,
                                    Value::Float(n) => n.round_ties_even() as i64,
                                    _ if matches!(
                                        predicate,
                                        XExpr::Variable { user: true, .. }
                                    ) =>
                                    {
                                        super::super::with_suppressed_expression_warnings(|| {
                                            int_value(&v)
                                        })?
                                    }
                                    _ => int_value(&v)?,
                                };
                                n > 0 && m.position == (n - 1) as usize
                            };
                            if keep {
                                next.push(Match {
                                    id: m.id,
                                    position: next.len(),
                                    size: 0,
                                });
                            }
                        }
                        std::mem::swap(&mut next, &mut current);
                    }
                }
                Ok(Nodes(current))
            }
            XExpr::Binary {
                op, left, right, ..
            } => {
                if matches!(op, BinaryOp::And | BinaryOp::Or) {
                    let l = self.boolean(self.eval(left, context)?)?;
                    if (*op == BinaryOp::And && !l) || (*op == BinaryOp::Or && l) {
                        return Ok(Scalar(Value::Int(l as i64)));
                    }
                    return Ok(Scalar(Value::Int(
                        self.boolean(self.eval(right, context)?)? as i64,
                    )));
                }
                let l = self.eval(left, context)?;
                let r = self.eval(right, context)?;
                if matches!(
                    op,
                    BinaryOp::Eq
                        | BinaryOp::NotEq
                        | BinaryOp::Lt
                        | BinaryOp::LtEq
                        | BinaryOp::Gt
                        | BinaryOp::GtEq
                ) {
                    let compare = |a: &Value, b: &Value| {
                        super::super::with_compare_collation(Some(self.collation), || {
                            super::super::null_cmp_binary(a, *op, b)
                        })
                    };
                    let value = match (l, r) {
                        (Nodes(n), Scalar(v)) => self.compare_nodes(&n, &v, false, &compare)?,
                        (Scalar(v), Nodes(n)) => self.compare_nodes(&n, &v, true, &compare)?,
                        (Scalar(a), Scalar(b)) => compare(&a, &b)?,
                        _ => unreachable!(),
                    };
                    return Ok(Scalar(value));
                }
                let l = self.scalar(l)?;
                let r = self.scalar(r)?;
                let value = if *op == BinaryOp::IntDiv {
                    let args = [Expr::BoundValue(l), Expr::BoundValue(r)];
                    super::super::eval_function("DIV", &args, |e| {
                        if let Expr::BoundValue(v) = e {
                            Ok(v.clone())
                        } else {
                            unreachable!()
                        }
                    })?
                } else {
                    super::super::eval_arithmetic(&l, *op, &r)?
                };
                Ok(Scalar(value))
            }
            XExpr::Call { name, args } => {
                match name.as_str() {
                    "true" => return Ok(Scalar(Value::Int(1))),
                    "false" => return Ok(Scalar(Value::Int(0))),
                    "last" => return Ok(Scalar(Value::Int(context.map_or(0, |m| m.size) as i64))),
                    "position" => {
                        return Ok(Scalar(Value::Int(
                            context.map_or(0, |m| m.position + 1) as i64
                        )))
                    }
                    "boolean" | "not" => {
                        let b = self.boolean(self.eval(&args[0], context)?)?;
                        return Ok(Scalar(Value::Int((b ^ (name == "not")) as i64)));
                    }
                    "count" | "sum" => {
                        let Nodes(nodes) = self.eval(&args[0], context)? else {
                            unreachable!()
                        };
                        if name == "count" {
                            return Ok(Scalar(Value::Int(
                                if nodes.len() == 1 && nodes[0].size > 0 {
                                    nodes[0].size
                                } else {
                                    nodes.len()
                                } as i64,
                            )));
                        }
                        let mut sum = 0.0;
                        for m in nodes {
                            let mut i = m.id + 1;
                            while i < self.document.nodes[m.id].end {
                                let n = &self.document.nodes[i];
                                if n.kind == Kind::Text {
                                    sum += number_prefix(&self.xml[n.span.0..n.span.1]);
                                }
                                i = n.end;
                            }
                        }
                        return Ok(Scalar(Value::Double(sum)));
                    }
                    _ => {}
                }
                let mut values = Vec::with_capacity(args.len());
                for a in args {
                    let value = self.scalar(self.eval(a, context)?)?;
                    values.push(
                        if matches!(
                            name.as_str(),
                            "concat" | "contains" | "substring" | "string-length"
                        ) {
                            formatted_scalar(a, value)
                        } else {
                            value
                        },
                    );
                }
                match name.as_str() {
                    "number" => {
                        let v = if values.is_empty() {
                            self.scalar(Nodes(vec![context.unwrap_or(Match {
                                id: 0,
                                position: 0,
                                size: 0,
                            })]))?
                        } else {
                            values.remove(0)
                        };
                        Ok(Scalar(Value::Double(numeric(&v)?)))
                    }
                    "contains" => {
                        if values.iter().any(|v| matches!(v, Value::Null)) {
                            return Ok(Scalar(Value::Int(0)));
                        }
                        let hay = super::super::str_value(&values[0]);
                        let needle = super::super::str_value(&values[1]);
                        let found = if self.charset == "binary" {
                            let h = super::encode_owned(&values[0], "binary")?;
                            let n = super::encode_owned(&values[1], "binary")?;
                            memchr::memmem::find(&h, &n).is_some()
                        } else {
                            super::super::find_position_collated(&hay, &needle, 1, self.collation)
                                != 0
                        };
                        Ok(Scalar(Value::Int(found as i64)))
                    }
                    "concat" => {
                        if values.iter().take(2).any(|v| matches!(v, Value::Null)) {
                            return Ok(Scalar(Value::Null));
                        }
                        let mut out = super::encode_owned(&values[0], self.charset)?;
                        out.extend_from_slice(&super::encode_owned(&values[1], self.charset)?);
                        Ok(Scalar(super::decode(&out, self.charset)?))
                    }
                    "string-length" => {
                        if values.is_empty() {
                            values.push(self.scalar(Nodes(vec![context.unwrap_or(Match {
                                id: 0,
                                position: 0,
                                size: 0,
                            })]))?);
                        }
                        let v = &values[0];
                        Ok(Scalar(if matches!(v, Value::Null) {
                            Value::Null
                        } else {
                            Value::Int(if let Value::Bytes(b) = v {
                                b.len()
                            } else {
                                string_value(v).chars().count()
                            } as i64)
                        }))
                    }
                    other => {
                        let sql_name = match other {
                            "substring" => "SUBSTRING",
                            "ceiling" => "CEIL",
                            "floor" => "FLOOR",
                            "round" => "ROUND",
                            _ => unreachable!(),
                        };
                        let args: Vec<_> = values.into_iter().map(Expr::BoundValue).collect();
                        Ok(Scalar(super::super::eval_function(sql_name, &args, |e| {
                            if let Expr::BoundValue(v) = e {
                                Ok(v.clone())
                            } else {
                                unreachable!()
                            }
                        })?))
                    }
                }
            }
        }
    }
    fn compare_nodes(
        &self,
        nodes: &[Match],
        v: &Value,
        reverse: bool,
        compare: &impl Fn(&Value, &Value) -> Result<Value, QueryError>,
    ) -> Result<Value, QueryError> {
        for m in nodes {
            let mut i = m.id + 1;
            while i < self.document.nodes[m.id].end {
                let n = &self.document.nodes[i];
                if n.kind == Kind::Text {
                    let text = super::decode(&self.xml[n.span.0..n.span.1], self.charset)?;
                    let answer = if reverse {
                        compare(v, &text)?
                    } else {
                        compare(&text, v)?
                    };
                    if int_value(&answer)? != 0 {
                        return Ok(Value::Int(1));
                    }
                }
                i = n.end;
            }
        }
        Ok(Value::Int(0))
    }
}
fn number_prefix(s: &[u8]) -> f64 {
    let text = String::from_utf8_lossy(s);
    let text = text.trim_start();
    let mut end = 0;
    let mut exp = false;
    for (i, c) in text.char_indices() {
        if c.is_ascii_digit() || c == '.' || ((c == '-' || c == '+') && (i == 0 || exp)) {
            end = i + c.len_utf8();
            exp = false;
        } else if c == 'e' || c == 'E' {
            end = i + 1;
            exp = true;
        } else {
            break;
        }
    }
    text[..end].parse().unwrap_or(0.0)
}

fn formatted_scalar(expr: &XExpr, value: Value) -> Value {
    let (Value::Double(number), Some(decimals)) = (&value, expr.decimals()) else {
        return value;
    };
    if decimals > 30 {
        return value;
    }
    let shortest = number.to_string();
    let mut text = crate::util::decimal::rescale_decimal_big_str(&shortest, decimals)
        .unwrap_or_else(|| format!("{:.*}", decimals as usize, number));
    if number.is_sign_negative() && *number == 0.0 && !text.starts_with('-') {
        text.insert(0, '-');
    }
    Value::String(text)
}
