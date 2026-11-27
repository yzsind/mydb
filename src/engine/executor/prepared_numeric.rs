//! PREPARE-time numeric binding, independent of live session-variable reads.
use super::*;
use crate::engine::api::PreparedNumericContext;
mod parameters;

fn depends_on_precision(statement: &Statement) -> bool {
    #[derive(Default)]
    struct Find(bool);
    impl crate::parser::visit::AstVisitor for Find {
        fn enter_expr(&mut self, expr: &Expr) {
            self.0 |= matches!(
                expr,
                Expr::Binary {
                    op: BinaryOp::Div,
                    ..
                }
            ) || matches!(expr, Expr::Function {name, ..} if name.eq_ignore_ascii_case("AVG"));
        }
        fn continue_expr_walk(&self) -> bool {
            !self.0
        }
    }
    let mut find = Find::default();
    crate::parser::visit::visit_statement(&mut find, statement);
    find.0
}

pub(super) fn bind(
    db: &Arc<Database>,
    session: &SessionContext,
    statement: &Statement,
) -> Result<Option<PreparedNumericContext>, QueryError> {
    let catalog_epoch = db.catalog_publication_epoch();
    let mut bound = statement.clone();
    let (result, objects) = crate::engine::cache::with_isolated_plan_object_deps(|| {
        dispatch::expand_views(db, session, &mut bound, 0)?;
        qualify_statement_tables(db, session, &mut bound)
    });
    result?;
    // Views can gain a precision-dependent expression after ALTER VIEW.
    if !depends_on_precision(&bound) && objects.is_empty() {
        return Ok(None);
    }
    // Precision binding is independent of parameter-domain inference. Unknown
    // function signatures retain their original parameter evaluation without
    // making constant division depend on EXECUTE-time session variables.
    let parameters = parameters::derive(&bound, db).unwrap_or_default();
    let mut deps = smallvec::SmallVec::<[crate::engine::cache::PlanDep; 4]>::new();
    for usage in dispatch::collect_statement_table_uses(&bound) {
        let key = qualification::qualify_table_name(session, &usage.table);
        if let Ok(table) = db.table(&key) {
            if !deps.iter().any(|(_, id, _)| *id == table.id) {
                deps.push((Arc::from(key), table.id, table.schema_version));
            }
        }
    }
    let bindings = dispatch::collect_statement_table_uses(statement)
        .into_iter()
        .map(|usage| {
            let logical = qualification::qualify_persistent_table_name(session, &usage.table);
            let physical = session.resolve_temp_table_name(&logical);
            (Arc::<str>::from(logical), physical)
        })
        .collect::<Vec<_>>();
    let bytes = deps
        .iter()
        .chain(objects.iter())
        .fold(64usize, |total, (key, _, _)| {
            total
                .saturating_add(std::mem::size_of::<crate::engine::cache::PlanDep>())
                .saturating_add(key.len())
                .saturating_add(16)
        })
        .saturating_add(
            bindings
                .iter()
                .map(|(logical, physical)| {
                    std::mem::size_of::<(Arc<str>, Option<String>)>()
                        + logical.len()
                        + 16
                        + physical.as_ref().map_or(0, String::capacity)
                })
                .sum::<usize>(),
        );
    let charge = mem_budget::ControlledMemoryCharge::try_new(
        mem_budget::ControlledMemoryCategory::DerivedCache,
        bytes,
    )?;
    let parameters = if parameters.is_empty() {
        None
    } else {
        let charge = mem_budget::ControlledMemoryCharge::try_new(
            mem_budget::ControlledMemoryCategory::DerivedCache,
            parameters.len()
                * std::mem::size_of::<Option<crate::engine::api::PreparedNumericType>>()
                + std::mem::size_of::<crate::engine::api::PreparedNumericParameters>()
                + 32,
        )?;
        Some(Arc::new(crate::engine::api::PreparedNumericParameters {
            types: parameters.into_boxed_slice(),
            _memory_charge: charge,
        }))
    };
    Ok(Some(PreparedNumericContext {
        parameters,
        parameter_reprepared: false,
        precision: session.sysvars.div_precision_increment.min(30) as u8,
        catalog_epoch,
        deps: deps.into_vec().into(),
        objects: objects.into_vec().into(),
        bindings: bindings.into(),
        _memory_charge: Arc::new(charge),
    }))
}

pub(super) fn refresh(
    db: &Arc<Database>,
    session: &SessionContext,
    statement: &Statement,
    context: &mut Option<PreparedNumericContext>,
) -> Result<Option<u8>, QueryError> {
    let Some(current) = context else {
        return Ok(None);
    };
    let epoch = db.catalog_publication_epoch();
    let namespace_changed = (!session.temp_tables.is_empty()
        || current
            .bindings
            .iter()
            .any(|(_, physical)| physical.is_some()))
        && current
            .bindings
            .iter()
            .any(|(logical, physical)| session.resolve_temp_table_name(logical) != *physical);
    if namespace_changed
        || (current.catalog_epoch != epoch
            && (!db.table_versions_match(&current.deps)
                || !db.view_versions_match(&current.objects)))
    {
        // Definition changes rebind precision; ordinary cache eviction,
        // row changes and unrelated DDL do not reprepare it.
        *context = bind(db, session, statement)?;
    } else {
        current.catalog_epoch = epoch;
    }
    Ok(context.as_ref().map(|context| context.precision))
}
