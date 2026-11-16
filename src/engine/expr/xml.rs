//! MySQL-compatible XML fragments and XPath scalar functions.
mod document;
mod evaluate;
mod xpath;

use crate::engine::func_cache::Governed;
use crate::parser::Expr;
use crate::storage::TableSchema;
use crate::types::{QueryError, Value};
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Effective parameter character domain retained when a prepared binding is NULL.
#[derive(Clone, Copy, Debug, Hash)]
pub(crate) enum ParameterType {
    Text(u16),
    Binary,
    Numeric,
}
impl ParameterType {
    pub(crate) fn of(value: &Value, collation: u16) -> Option<Self> {
        match value {
            Value::Null => None,
            Value::String(_) | Value::SmallString(_) => Some(Self::Text(collation)),
            Value::Bytes(_) => Some(Self::Binary),
            _ => Some(Self::Numeric),
        }
    }
    /// MySQL retains a numeric derived parameter type when later executions
    /// send text; the text payload is still available to val_str().
    pub(crate) fn bind(&mut self, value: &Value, collation: u16) {
        if let Some(next) = Self::of(value, collation) {
            if !matches!(*self, Self::Numeric) || !matches!(next, Self::Text(_)) {
                *self = next;
            }
        }
    }
    pub(crate) fn descriptor(self) -> crate::engine::expr_type::ExprType {
        use crate::engine::expr_type::{ExprType, ExprTypeKind, IntegerWidth, StringKind};
        let mut result = match self {
            Self::Numeric => {
                crate::engine::expr_type::fixed_integer_result(IntegerWidth::Big, false, true)
            }
            _ => ExprType::new(
                ExprTypeKind::String {
                    kind: if matches!(self, Self::Binary) {
                        StringKind::Blob
                    } else {
                        StringKind::Character
                    },
                },
                true,
            ),
        };
        result.collation_id = match self {
            Self::Text(id) => Some(id),
            Self::Binary => Some(crate::charset::BINARY_COLLATION_ID),
            Self::Numeric => None,
        };
        result.coercibility = 4;
        result
    }
}

pub(crate) type Locals = Arc<HashMap<String, Value>>;
thread_local! {
    static LOCALS: RefCell<Option<Locals>> = const {RefCell::new(None)};
    static CACHE: RefCell<lru::LruCache<u64,CacheEntry,ahash::RandomState>> = RefCell::new(lru::LruCache::with_hasher(NonZeroUsize::new(256).unwrap(),ahash::RandomState::new()));
    static DOCUMENT: RefCell<document::Document> = RefCell::new(document::Document::default());
}
struct CacheEntry {
    source: Vec<u8>,
    collation: u16,
    program: Arc<Governed<xpath::Program>>,
}
pub(crate) struct LocalScope(Option<Locals>);
impl Drop for LocalScope {
    fn drop(&mut self) {
        LOCALS.with(|l| *l.borrow_mut() = self.0.take());
    }
}
pub(crate) fn enter_locals(locals: Option<Locals>) -> LocalScope {
    LocalScope(LOCALS.with(|l| std::mem::replace(&mut *l.borrow_mut(), locals)))
}
pub(crate) fn current_locals() -> Option<Locals> {
    LOCALS.with(|l| l.borrow().clone())
}
fn local_variable(name: &str) -> Option<Value> {
    LOCALS.with(|l| {
        l.borrow()
            .as_ref()
            .and_then(|l| l.get(&name.to_ascii_lowercase()).cloned())
    })
}
pub(crate) fn is_xml(name: &str) -> bool {
    name.eq_ignore_ascii_case("EXTRACTVALUE") || name.eq_ignore_ascii_case("UPDATEXML")
}
pub(crate) fn mentions_xml(sql: &str) -> bool {
    sql.as_bytes()
        .windows(9)
        .any(|s| s.eq_ignore_ascii_case(b"UPDATEXML"))
        || sql
            .as_bytes()
            .windows(12)
            .any(|s| s.eq_ignore_ascii_case(b"EXTRACTVALUE"))
}
fn charset(collation: u16) -> Result<&'static str, QueryError> {
    let charset = crate::charset::resolve_collation_id(collation).map_or("utf8mb4", |c| c.charset);
    if matches!(charset, "utf16" | "utf16le" | "ucs2" | "utf32") {
        return Err(crate::error::catalog::ER_UNKNOWN_ERROR.error(format!(
            "Character set '{charset}' is not supported by XPATH"
        )));
    }
    Ok(charset)
}
fn encode<'a>(v: &'a Value, cs: &str) -> Result<Cow<'a, [u8]>, QueryError> {
    match v {
        Value::Bytes(b) => Ok(Cow::Borrowed(b)),
        Value::String(s) if matches!(cs, "binary" | "utf8mb4" | "utf8mb3" | "utf8") => {
            Ok(Cow::Borrowed(s.as_bytes()))
        }
        _ => Ok(Cow::Owned(encode_owned(v, cs)?)),
    }
}
fn encode_owned(v: &Value, cs: &str) -> Result<Vec<u8>, QueryError> {
    if let Value::Bytes(b) = v {
        return Ok(b.clone());
    }
    let text = super::string_value(v);
    if cs == "binary" {
        Ok(text.into_bytes())
    } else {
        crate::charset::encode_string(cs, &text)
    }
}
fn decode(bytes: &[u8], cs: &str) -> Result<Value, QueryError> {
    if cs == "binary" {
        Ok(Value::Bytes(bytes.to_vec()))
    } else {
        Ok(Value::String(crate::charset::decode_bytes(cs, bytes)?))
    }
}
fn decode_owned(bytes: Vec<u8>, cs: &str) -> Result<Value, QueryError> {
    if cs == "binary" {
        return Ok(Value::Bytes(bytes));
    }
    if matches!(cs, "utf8mb4" | "utf8mb3" | "utf8") {
        let text = String::from_utf8(bytes).map_err(|_| {
            crate::error::catalog::ER_INVALID_CHARACTER_STRING
                .error("Invalid utf8mb4 character string")
        })?;
        return Ok(if text.len() <= 31 {
            Value::SmallString(crate::types::SmallStringBuf::from_slice(text.as_bytes()))
        } else {
            Value::String(text)
        });
    }
    decode(&bytes, cs)
}
fn cached(source: &[u8], collation: u16) -> Result<Arc<Governed<xpath::Program>>, QueryError> {
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    source.hash(&mut hash);
    collation.hash(&mut hash);
    let key = hash.finish();
    CACHE.with(|c| {
        if let Some(entry) = c.borrow_mut().get(&key) {
            if entry.source == source && entry.collation == collation {
                return Ok(Arc::clone(&entry.program));
            }
        }
        let bytes = source.len().saturating_mul(256).saturating_add(1024);
        let compile_memory = crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
            crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
            bytes,
        )?;
        let program = xpath::compile(source, charset(collation)?)?;
        drop(compile_memory);
        let program = Arc::new(Governed::new(program, bytes));
        if program.admitted() {
            c.borrow_mut().put(
                key,
                CacheEntry {
                    source: source.to_vec(),
                    collation,
                    program: Arc::clone(&program),
                },
            );
        }
        Ok(program)
    })
}
fn validate_variables(program: &xpath::Program, source: &[u8]) -> Result<(), QueryError> {
    for (name, user, offset) in &program.variables {
        if !*user && local_variable(name).is_none() {
            let b = &source[*offset..source.len().min(*offset + 32)];
            let mut end = offset + 1 + name.len();
            while source.get(end) == Some(&b' ') {
                end += 1;
            }
            return Err(crate::error::catalog::ER_UNKNOWN_ERROR
                .error(format!(
                    "Unknown XPATH variable at: '{}'",
                    String::from_utf8_lossy(b)
                ))
                .with_followup_error(xpath::syntax(source, end)));
        }
    }
    Ok(())
}
fn constant_for_execution(e: &Expr) -> bool {
    crate::parser::visit::try_inspect_value_expr(e, |node| {
        match node {
            Expr::Identifier(_)
            | Expr::CompoundIdentifier(_)
            | Expr::Wildcard
            | Expr::Default(_)
            | Expr::DefaultQualified(_)
            | Expr::Subquery(_)
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::WindowFunction { .. }
            | Expr::GroupKeyRef { .. }
            | Expr::Grouping(_)
            | Expr::QuantifiedComparison { .. } => return Err(()),
            Expr::Function { name, .. }
                if crate::planner::is_aggregate_function(name)
                    || super::function_is_runtime_dependent(&name.to_ascii_uppercase()) =>
            {
                return Err(())
            }
            _ => {}
        }
        Ok(true)
    })
    .is_ok()
}
pub(crate) fn validate(name: &str, args: &[Expr], table: &TableSchema) -> Result<(), QueryError> {
    super::expect_arg_count(
        name,
        args,
        if name.eq_ignore_ascii_case("EXTRACTVALUE") {
            2
        } else {
            3
        },
    )?;
    let collation = super::aggregate_expr_collations(args.iter(), table, name)?
        .unwrap_or_else(super::connection_collation_id);
    let cs = match charset(collation) {
        Ok(cs) => cs,
        Err(error) => {
            for arg in args {
                crate::parser::visit::inspect_value_expr(
                    arg,
                    |node| {
                        if let Expr::Function { name, args, .. } = node {
                            if name.eq_ignore_ascii_case("CONVERT_USING") {
                                if let Some(Expr::Literal(crate::parser::Literal::String(cs))) =
                                    args.get(1)
                                {
                                    if matches!(
                                        cs.to_ascii_lowercase().as_str(),
                                        "ucs2" | "utf16" | "utf16le" | "utf32"
                                    ) {
                                        crate::engine::api::push_statement_warning(crate::error::ConditionLevel::Warning,crate::error::catalog::ER_WARN_DEPRECATED_SYNTAX,format!("'{cs}' is deprecated and will be removed in a future release. Please use utf8mb4 instead"));
                                    }
                                }
                            }
                        }
                        true
                    },
                    |_| {},
                );
            }
            return Err(error);
        }
    };
    if !constant_for_execution(&args[1]) {
        return Err(crate::error::catalog::ER_UNKNOWN_ERROR
            .error("Only constant XPATH queries are supported"));
    }
    if super::expr_is_static_value(&args[1]) {
        let value = super::eval_const_runtime(&args[1])?;
        if !matches!(value, Value::Null) {
            let source = encode(&value, cs)?;
            let program = cached(&source, collation)?;
            validate_variables(&program, &source)?;
        }
    }
    Ok(())
}
pub(crate) fn call<F>(name: &str, args: &[Expr], mut eval: F) -> Result<Value, QueryError>
where
    F: FnMut(&Expr) -> Result<Value, QueryError>,
{
    super::expect_arg_count(
        name,
        args,
        if name.eq_ignore_ascii_case("EXTRACTVALUE") {
            2
        } else {
            3
        },
    )?;
    let collation = match super::COMPARE_COLLATION_ID.with(std::cell::Cell::get) {
        Some(id) => id,
        None => {
            let operands: smallvec::SmallVec<[&Expr; 3]> = args.iter().collect();
            super::aggregate_static_expr_collations(&operands, name)?
                .unwrap_or_else(super::connection_collation_id)
        }
    };
    let path = eval(&args[1])?;
    let Some(call) = PreparedCall::new(&path, collation)? else {
        return Ok(Value::Null);
    };
    let xml = eval(&args[0])?;
    if matches!(xml, Value::Null) {
        return Ok(Value::Null);
    }
    let replacement = if args.len() == 3 {
        Some(eval(&args[2])?)
    } else {
        None
    };
    call.execute(&xml, replacement.as_ref())
}

pub(crate) struct PreparedCall {
    program: Arc<Governed<xpath::Program>>,
    charset: &'static str,
    collation: u16,
}
impl PreparedCall {
    pub(crate) fn new(path: &Value, collation: u16) -> Result<Option<Self>, QueryError> {
        let charset = charset(collation)?;
        if matches!(path, Value::Null) {
            return Ok(None);
        }
        let source = encode(path, charset)?;
        let program = cached(&source, collation)?;
        validate_variables(&program, &source)?;
        Ok(Some(Self {
            program,
            charset,
            collation,
        }))
    }
    pub(crate) fn execute(
        &self,
        xml: &Value,
        replacement: Option<&Value>,
    ) -> Result<Value, QueryError> {
        if matches!(xml, Value::Null) || replacement.is_some_and(|v| matches!(v, Value::Null)) {
            return Ok(Value::Null);
        }
        let xml = encode(xml, self.charset)?;
        match evaluate_raw(
            &self.program,
            &xml,
            replacement,
            self.charset,
            self.collation,
        )? {
            None => Ok(Value::Null),
            Some(bytes) if self.charset == "binary" => Ok(Value::Bytes(bytes)),
            Some(bytes) if matches!(self.charset, "utf8mb4" | "utf8mb3" | "utf8") => {
                String::from_utf8(bytes).map(Value::String).map_err(|e| {
                    crate::error::catalog::ER_INVALID_CHARACTER_STRING.error(format!(
                        "Invalid utf8mb4 character string: '{}'",
                        e.utf8_error()
                    ))
                })
            }
            Some(bytes) => decode(&bytes, self.charset),
        }
    }
}

fn evaluate_raw(
    program: &xpath::Program,
    xml: &[u8],
    replacement: Option<&Value>,
    cs: &'static str,
    collation: u16,
) -> Result<Option<Vec<u8>>, QueryError> {
    DOCUMENT.with(|slot| {
        // Evaluating variable expressions can reenter SQL; take the reusable
        // document out of TLS so no RefCell borrow crosses evaluation.
        let mut document = std::mem::take(&mut *slot.borrow_mut());
        let result = (|| {
            if !document.parse(xml)? {
                return Ok(None);
            }
            let _evaluation_memory =
                crate::engine::executor::mem_budget::ControlledMemoryCharge::try_new(
                    crate::engine::executor::mem_budget::ControlledMemoryCategory::Executor,
                    document
                        .nodes
                        .len()
                        .saturating_mul(std::mem::size_of::<evaluate::Match>())
                        .saturating_mul(program.work_vectors)
                        .saturating_add(xml.len().saturating_mul(2))
                        .saturating_add(replacement.map_or(0, |v| super::str_value(v).len())),
                )?;
            let evaluator = evaluate::Evaluation {
                document: &document,
                xml,
                charset: cs,
                collation,
            };
            if let Some(replacement) = replacement {
                let Some(matches) = evaluator.matches(&program.expression)? else {
                    return Ok(None);
                };
                if matches.len() != 1 {
                    return Ok(Some(xml.to_vec()));
                }
                let replacement = encode(replacement, cs)?;
                let n = &document.nodes[matches[0].id];
                let (start, end) = n.span;
                let mut out = Vec::with_capacity(
                    xml.len()
                        .saturating_sub(end - start)
                        .saturating_add(replacement.len()),
                );
                out.extend_from_slice(&xml[..start]);
                out.extend_from_slice(&replacement);
                out.extend_from_slice(&xml[end..]);
                Ok(Some(out))
            } else {
                evaluator.extract(&program.expression)
            }
        })();
        *slot.borrow_mut() = document;
        result
    })
}

pub(crate) fn expression_mentions_xml(expr: &Expr) -> bool {
    let found = crate::parser::visit::any_expr(
        expr,
        |node| matches!(node, Expr::Function{name,..} if is_xml(name)),
    );
    found
}

/// Prepared UTF-8 column kernel. Only the input document varies per row.
pub(crate) struct ColumnPlan {
    program: Arc<Governed<xpath::Program>>,
    source: Vec<u8>,
    replacement: Option<Value>,
    collation: u16,
}
impl ColumnPlan {
    pub(crate) fn new(
        path: &Value,
        replacement: Option<&Value>,
        collation: u16,
    ) -> Result<Option<Self>, QueryError> {
        let cs = charset(collation)?;
        if !matches!(cs, "utf8mb4" | "utf8mb3" | "utf8")
            || matches!(path, Value::Null)
            || replacement.is_some_and(|v| matches!(v, Value::Null))
        {
            return Ok(None);
        }
        let source = encode(path, cs)?.into_owned();
        let program = cached(&source, collation)?;
        Ok(Some(Self {
            program,
            source,
            replacement: replacement.cloned(),
            collation,
        }))
    }
    pub(crate) fn evaluate(&self, xml: &[u8]) -> Result<Option<Vec<u8>>, QueryError> {
        validate_variables(&self.program, &self.source)?;
        evaluate_raw(
            &self.program,
            xml,
            self.replacement.as_ref(),
            "utf8mb4",
            self.collation,
        )
    }
}

pub(crate) fn parameter_dependent_result(expr: &Expr) -> bool {
    expression_mentions_xml(expr)
        && crate::parser::visit::any_expr(
            expr,
            |e| matches!(e,Expr::Placeholder(index) if crate::engine::params::current_param(*index).is_none()),
        )
}
pub(crate) fn parameter_history(
    sql: &str,
    count: usize,
    collation: &str,
) -> Option<Arc<parking_lot::Mutex<Vec<ParameterType>>>> {
    mentions_xml(sql).then(|| {
        Arc::new(parking_lot::Mutex::new(vec![
            ParameterType::Text(
                crate::charset::resolve_collation(collation).map_or(255, |c| c.id)
            );
            count
        ]))
    })
}
pub(crate) fn bind_parameter_history(
    session: &mut crate::engine::api::SessionContext,
    history: &Option<Arc<parking_lot::Mutex<Vec<ParameterType>>>>,
    params: &[Value],
) {
    if let Some(history) = history {
        let mut types = history.lock();
        let collation =
            crate::charset::resolve_collation(&session.collation_connection).map_or(255, |c| c.id);
        for (slot, value) in types.iter_mut().zip(params) {
            slot.bind(value, collation);
        }
        session.xml_parameter_binding = params.as_ptr() as usize;
        session.xml_parameter_types = Some(Arc::new(types.clone()));
    }
}
