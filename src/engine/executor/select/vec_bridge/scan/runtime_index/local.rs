//! Bounded locality discovery for constant integer domains on heap scans.
use super::*;
use crate::engine::executor::select::bloom::ExactIntKeySetBuilder;
use crate::parser::{BinaryOp, Literal};

pub(super) fn residency_limit() -> u64 {
    let host = mem_budget::effective_memory_limit_bytes();
    host.min(
        mem_budget::current_budget()
            .map_or_else(mem_budget::query_mem_limit_bytes, |budget| budget.limit()),
    ) as u64
}

fn integer(expr: &Expr) -> Option<i128> {
    match expr {
        Expr::Literal(Literal::Int(value)) | Expr::BoundValue(Value::Int(value)) => {
            Some(i128::from(*value))
        }
        Expr::Literal(Literal::UInt(value)) | Expr::BoundValue(Value::UInt(value)) => {
            Some(i128::from(*value))
        }
        _ => None,
    }
}

/// The original predicate still runs after MVCC heap fetch. Restrict optional
/// candidate discovery to diagnostic-free integer predicates; a string cast,
/// volatile expression, subquery or lossy numeric comparison cannot seed it.
pub(super) fn domains(table: &PlanTable, predicate: &Expr) -> Option<Vec<RuntimeFilter>> {
    use crate::storage::DataTypeKind as T;
    if table
        .schema
        .stats
        .heap_page_count
        .saturating_mul(crate::storage::PAGE_SIZE as u64)
        <= residency_limit()
    {
        return None;
    }
    let column = |expr: &Expr| {
        let name = match expr {
            Expr::Identifier(name) => name,
            Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
                let qualifier = &parts[parts.len() - 2];
                if !qualifier
                    .eq_ignore_ascii_case(table.alias.as_deref().unwrap_or(&table.schema.name))
                    && !qualifier.eq_ignore_ascii_case(
                        table
                            .schema
                            .name
                            .rsplit('.')
                            .next()
                            .unwrap_or(&table.schema.name),
                    )
                {
                    return None;
                }
                parts.last()?
            }
            _ => return None,
        };
        let index = table.schema.column_index(name).ok()?;
        matches!(
            table.schema.columns[index].data_type,
            T::TinyInt | T::SmallInt | T::MediumInt | T::Int | T::BigInt
        )
        .then_some(index)
    };
    fn safe(expr: &Expr, column: &impl Fn(&Expr) -> Option<usize>) -> bool {
        match expr {
            Expr::Binary {
                left,
                op: BinaryOp::And | BinaryOp::Or,
                right,
            } => safe(left, column) && safe(right, column),
            Expr::Binary { left, op, right }
                if matches!(
                    op,
                    BinaryOp::Eq
                        | BinaryOp::NotEq
                        | BinaryOp::Lt
                        | BinaryOp::LtEq
                        | BinaryOp::Gt
                        | BinaryOp::GtEq
                ) =>
            {
                (column(left).is_some() && integer(right).is_some())
                    || (integer(left).is_some() && column(right).is_some())
            }
            Expr::IsNull(value) | Expr::IsNotNull(value) => column(value).is_some(),
            Expr::InList { expr, list, .. } => {
                column(expr).is_some()
                    && list.len() <= 256
                    && list.iter().all(|value| integer(value).is_some())
            }
            _ => false,
        }
    }
    if !safe(predicate, &column) {
        return None;
    }
    let mut conjuncts = Vec::new();
    fn flatten<'a>(expr: &'a Expr, output: &mut Vec<&'a Expr>) {
        if let Expr::Binary {
            left,
            op: BinaryOp::And,
            right,
        } = expr
        {
            flatten(left, output);
            flatten(right, output);
        } else {
            output.push(expr);
        }
    }
    flatten(predicate, &mut conjuncts);
    let mut filters = Vec::new();
    for expr in conjuncts {
        let (key, values) = match expr {
            Expr::Binary {
                left,
                op: BinaryOp::Eq,
                right,
            } if column(left).is_some() => (left.as_ref(), vec![integer(right)?]),
            Expr::Binary {
                left,
                op: BinaryOp::Eq,
                right,
            } if column(right).is_some() => (right.as_ref(), vec![integer(left)?]),
            Expr::InList {
                expr,
                list,
                negated: false,
            } => (
                expr.as_ref(),
                list.iter().map(integer).collect::<Option<Vec<_>>>()?,
            ),
            _ => continue,
        };
        if values.is_empty() {
            continue;
        }
        let mut builder = ExactIntKeySetBuilder::with_capacity(values.len()).ok()?;
        for value in values {
            builder.insert(value);
        }
        filters.push(RuntimeFilter::ExactIntKey {
            keys: vec![key.clone()],
            null_safe: vec![false],
            filter: Arc::new(builder.finish().ok()?),
            rejected: Arc::new(std::sync::OnceLock::new()),
        });
    }
    (!filters.is_empty()).then_some(filters)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::mem_budget::{with_query_budget, MemBudget};
    use crate::engine::StorageQueryEngine;

    #[test]
    fn local_integer_access_rechecks_private_lanes_and_hides_them() {
        let dir = tempfile::tempdir_in(crate::util::file::test_temp_dir()).unwrap();
        let engine = StorageQueryEngine::open(dir.path()).unwrap();
        for sql in [
            "CREATE TABLE local_access(k BIGINT, guard_col INT, payload DECIMAL(20,4),INDEX by_k(k))",
            "INSERT INTO local_access VALUES(1,NULL,1.2500),(1,2,2.5000),(2,NULL,3.7500),(NULL,NULL,4.0000)",
            "ANALYZE TABLE local_access",
        ] {engine.execute(sql).unwrap();}
        let db = engine.db_for_tests();
        db.set_undo_retention_ms(0).unwrap();
        db.gc_committed_txn_slots();
        let mut schema = (*db.table("local_access").unwrap()).clone();
        schema.stats.row_count = 100000;
        schema.stats.heap_page_count = (mem_budget::effective_memory_limit_bytes() as u64
            / crate::storage::PAGE_SIZE as u64
            + 1)
        .max(100000);
        schema.stats.columns.get_mut("k").unwrap().distinct_count = 1000;
        schema
            .stats
            .indexes
            .get_mut("by_k")
            .unwrap()
            .prefix_distinct_counts = vec![1000];
        let table = PlanTable::new(schema.id, Arc::new(schema), None);
        with_query_budget(Arc::new(MemBudget::new(64 << 20)), || {
            let parse = |sql: &str| {
                let crate::parser::Statement::Select(select) =
                    crate::parser::parse_statement(sql).unwrap()
                else {
                    panic!()
                };
                select.selection.unwrap()
            };
            for (text, eligible) in [
                ("k=1 AND guard_col IS NULL", true),
                ("k IN (1,2) AND guard_col IS NULL", true),
                ("k=1 OR guard_col IS NULL", false),
                ("k=1 AND guard_col/0>1", false),
                ("k=1.0", false),
                ("k='1'", false),
                ("k IS NULL", false),
            ] {
                let predicate = parse(&format!("SELECT payload FROM local_access WHERE {text}"));
                assert_eq!(domains(&table, &predicate).is_some(), eligible, "{text}");
            }
            let predicate =
                parse("SELECT payload FROM local_access WHERE k=1 AND guard_col IS NULL");
            let output = super::super::super::build_scan_batch_schema(&table, &[2]);
            let original = Box::new(MaterializedBatchOperator::new(output.clone(), vec![]));
            let mut actual = super::super::wrap_filtered(
                db,
                TxnView::read_only(
                    db.visible_scn(),
                    crate::storage::IsolationLevel::ReadCommitted,
                ),
                &table,
                vec![2],
                &[],
                original,
                &predicate,
            )
            .unwrap();
            let mut rows = Vec::new();
            while let Some(batch) = actual.next_batch().unwrap() {
                assert_eq!(batch.schema().len(), 1);
                assert_eq!(batch.columns().len(), 1);
                for row in batch.visible_indices() {
                    rows.push(batch.row(row));
                }
            }
            assert_eq!(
                rows.len(),
                1,
                "the empty fallback proves optional RID execution was selected"
            );
            assert_eq!(rows[0].len(), 1);
            assert_eq!(crate::engine::expr::str_value(&rows[0][0]), "1.2500");
        });
    }
}
