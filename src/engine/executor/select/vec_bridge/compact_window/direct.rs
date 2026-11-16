//! Read ranking and simple value frames directly from sorted batch references.
use super::*;

#[derive(Clone, Copy)]
pub(super) enum End {
    Row,
    Peers,
    Partition,
}

#[derive(Clone, Copy)]
pub(super) enum Plan {
    RowNumber,
    Rank {
        dense: bool,
    },
    Value {
        column: usize,
        nth: Option<usize>,
        end: End,
    },
}

impl Plan {
    pub(super) fn bind(
        window: &crate::planner::PhysicalWindowSpec,
        template: &[PlanField],
    ) -> Option<Self> {
        use crate::parser::{
            WindowFrameBound as Bound, WindowFrameExclusion as Exclusion, WindowFrameUnits as Units,
        };
        let name = window.name.to_ascii_uppercase();
        match name.as_str() {
            "ROW_NUMBER" => return Some(Self::RowNumber),
            "RANK" | "DENSE_RANK" => {
                return Some(Self::Rank {
                    dense: name == "DENSE_RANK",
                })
            }
            "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => {}
            _ => return None,
        }
        let column = expr_column_index_in_template(window.args.first()?, template)?;
        let nth = match name.as_str() {
            "FIRST_VALUE" => Some(0),
            "LAST_VALUE" => None,
            _ => {
                let Expr::Literal(crate::parser::Literal::Int(n)) = window.args.get(1)? else {
                    return None;
                };
                if *n <= 0 {
                    return None;
                }
                Some(usize::try_from(*n - 1).ok()?)
            }
        };
        let end = match &window.frame {
            None if window.order_by.is_empty() => End::Partition,
            None => End::Peers,
            Some(frame)
                if matches!(frame.start, Bound::UnboundedPreceding)
                    && matches!(frame.exclusion, Exclusion::NoOthers) =>
            {
                match frame.end {
                    Bound::UnboundedFollowing => End::Partition,
                    Bound::CurrentRow if matches!(frame.units, Units::Rows) => End::Row,
                    Bound::CurrentRow => End::Peers,
                    _ => return None,
                }
            }
            _ => return None,
        };
        Some(Self::Value { column, nth, end })
    }

    pub(super) fn evaluate(
        self,
        partition: &[(u32, u32)],
        batches: &[(Batch, usize)],
        order: &[usize],
        fields: &[crate::engine::executor::sort::SortField],
    ) -> Result<Vec<Value>, QueryError> {
        let mut output = Vec::with_capacity(partition.len());
        if partition.is_empty() {
            return Ok(output);
        }
        let peers = |a: (u32, u32), b: (u32, u32)| {
            order.iter().zip(fields).all(|(&column, &field)| {
                compare_fixed_rows(
                    &batches[a.0 as usize].0.columns()[column],
                    a.1 as usize,
                    &batches[b.0 as usize].0.columns()[column],
                    b.1 as usize,
                    field,
                )
                .is_eq()
            })
        };
        let read = |position: usize, column: usize| {
            let (batch, row) = partition[position];
            batches[batch as usize].0.columns()[column].value_at(row as usize)
        };
        let mut peer_end = 0;
        let mut rank = 0;
        for (position, &row) in partition.iter().enumerate() {
            if position & 255 == 0 {
                crate::engine::cancel::check_cancelled()?;
            }
            let value = match self {
                Self::RowNumber => Value::Int((position + 1) as i64),
                Self::Rank { dense } => {
                    if position == 0 || !peers(partition[position - 1], row) {
                        rank = if dense {
                            rank + 1
                        } else {
                            (position + 1) as i64
                        };
                    }
                    Value::Int(rank)
                }
                Self::Value { column, nth, end } => {
                    let last = match end {
                        End::Row => position,
                        End::Partition => partition.len() - 1,
                        End::Peers => {
                            peer_end = peer_end.max(position);
                            while peer_end + 1 < partition.len()
                                && peers(row, partition[peer_end + 1])
                            {
                                peer_end += 1;
                                if peer_end & 255 == 0 {
                                    crate::engine::cancel::check_cancelled()?;
                                }
                            }
                            peer_end
                        }
                    };
                    let source = nth.unwrap_or(last);
                    if source <= last {
                        read(source, column)
                    } else {
                        Value::Null
                    }
                }
            };
            output.push(value);
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::executor::vec::schema::{BatchField, BatchSchema};

    #[test]
    fn direct_windows_match_canonical_frames_peers_nulls_and_fixed_domains() {
        let domains = vec![
            (ColumnType::LongLong, Value::Int(-17)),
            (ColumnType::LongLong, Value::UInt(u64::MAX)),
            (ColumnType::Double, Value::Double(-0.0)),
            (
                ColumnType::Date,
                Value::Date(crate::types::encode_tagged_date(2000, 0, 1).unwrap()),
            ),
            (ColumnType::Time, Value::Time(-1234567)),
            (ColumnType::DateTime, Value::DateTime(1234567890123456)),
            (ColumnType::Timestamp, Value::Timestamp(1234567890123456)),
            (
                ColumnType::Decimal {
                    precision: 30,
                    scale: 3,
                },
                Value::Decimal128(i128::MAX / 1000000000, 3),
            ),
        ];
        for (kind, value) in domains {
            let schema = Arc::new(BatchSchema::new(vec![
                BatchField {
                    table: String::new(),
                    alias: None,
                    name: "k".into(),
                    column_type: ColumnType::LongLong,
                    source: Default::default(),
                },
                BatchField {
                    table: String::new(),
                    alias: None,
                    name: "v".into(),
                    column_type: kind,
                    source: Default::default(),
                },
            ]));
            let template = schema
                .fields()
                .iter()
                .map(super::super::super::batch_field_template)
                .collect::<Vec<_>>();
            let columns = schema
                .fields()
                .iter()
                .map(|field| crate::types::Column {
                    name: field.name.clone(),
                    column_type: field.column_type,
                    source: field.source.clone(),
                })
                .collect::<Vec<_>>();
            let keys = [
                Value::Null,
                Value::Int(1),
                Value::Int(1),
                Value::Int(2),
                Value::Int(2),
                Value::Null,
            ];
            let values = [
                value.clone(),
                Value::Null,
                value.clone(),
                value.clone(),
                Value::Null,
                value,
            ];
            let batches = keys
                .chunks(3)
                .zip(values.chunks(3))
                .map(|(k, v)| {
                    (
                        Batch::new(
                            schema.clone(),
                            vec![
                                ColumnVector::from_values(k.to_vec()),
                                ColumnVector::from_values(v.to_vec()),
                            ],
                        ),
                        0,
                    )
                })
                .collect::<Vec<_>>();
            let rows = batches
                .iter()
                .flat_map(|(batch, _)| (0..batch.num_rows()).map(|row| batch.row(row)))
                .collect::<Vec<_>>();
            for ordering in ["", "ORDER BY k", "ORDER BY k DESC"] {
                for frame in [
                    "",
                    "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                    "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                    "GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                    "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING",
                ] {
                    for function in [
                        "ROW_NUMBER()",
                        "RANK()",
                        "DENSE_RANK()",
                        "FIRST_VALUE(v)",
                        "LAST_VALUE(v)",
                        "NTH_VALUE(v,2)",
                        "NTH_VALUE(v,20)",
                    ] {
                        let sql = format!("{function} OVER ({ordering} {frame})");
                        let expression = crate::parser::parse_expr_fragment(&sql).unwrap();
                        let Expr::WindowFunction {
                            name, args, spec, ..
                        } = &expression
                        else {
                            panic!("window");
                        };
                        let window = crate::planner::PhysicalWindowSpec {
                            name: name.clone(),
                            args: args.clone(),
                            partition_by: spec.partition_by.clone(),
                            order_by: spec.order_by.clone(),
                            frame: spec.frame.clone(),
                            output_alias: "w".into(),
                            output_type: kind,
                            top_k: None,
                        };
                        let plan = Plan::bind(&window, &template).unwrap();
                        let order = spec.order_by.iter().map(|_| 0).collect::<Vec<_>>();
                        let fields = spec
                            .order_by
                            .iter()
                            .map(crate::engine::executor::sort::SortField::from_order_by)
                            .collect::<Vec<_>>();
                        let mut partition = (0..6)
                            .map(|i| ((i / 3) as u32, (i % 3) as u32))
                            .collect::<Vec<_>>();
                        partition.sort_by(|a, b| {
                            for (&column, &field) in order.iter().zip(&fields) {
                                let order = compare_fixed_rows(
                                    &batches[a.0 as usize].0.columns()[column],
                                    a.1 as usize,
                                    &batches[b.0 as usize].0.columns()[column],
                                    b.1 as usize,
                                    field,
                                );
                                if !order.is_eq() {
                                    return order;
                                }
                            }
                            a.cmp(b)
                        });
                        let ordered = partition
                            .iter()
                            .map(|&(batch, row)| batch as usize * 3 + row as usize)
                            .collect::<Vec<_>>();
                        let expected = evaluate_window_with_partitions(
                            &expression,
                            &[ordered.clone()],
                            &columns,
                            &rows,
                        )
                        .unwrap();
                        let actual = plan
                            .evaluate(&partition, &batches, &order, &fields)
                            .unwrap();
                        assert_eq!(
                            actual,
                            ordered
                                .iter()
                                .map(|&i| expected[i].clone())
                                .collect::<Vec<_>>(),
                            "{sql}, {kind:?}"
                        );
                    }
                }
            }
        }
    }
}
