pub(super) use crate::planner::explain_access::{IndexAccess, IndexAccessKind};

impl IndexAccess<'_> {
    pub fn tree_label(&self) -> &'static str {
        match self.kind {
            IndexAccessKind::Lookup { .. } if self.access_type() == "const" => "Constant lookup",
            IndexAccessKind::Lookup { .. } => "Index lookup",
            IndexAccessKind::MultiLookup { .. } => "Index multi lookup",
            IndexAccessKind::Range { .. } => "Index range scan",
            IndexAccessKind::Fast { .. } => "Index-only scan",
        }
    }

    pub fn condition(&self) -> Option<String> {
        match self.kind {
            IndexAccessKind::Lookup { parts, .. } => index_lookup_condition(self.index, parts),
            IndexAccessKind::MultiLookup { parts, probes } => {
                index_lookup_condition(self.index, parts)
                    .map(|condition| format!("{condition}; probes={probes}"))
            }
            IndexAccessKind::Range { low, high, .. } | IndexAccessKind::Fast { low, high } => {
                index_range_condition(self.index, low, high)
            }
        }
    }

    pub fn uses_mrr(&self) -> bool {
        matches!(
            self.kind,
            IndexAccessKind::Range {
                covering: false,
                ..
            }
        )
    }

    pub fn like_details(&self) -> Option<Vec<(&'static str, String)>> {
        if matches!(
            self.kind,
            IndexAccessKind::Range { .. } | IndexAccessKind::Fast { .. }
        ) {
            self.filter
                .as_ref()
                .and_then(|filter| like_access_diagnostics(self.table, filter))
        } else {
            None
        }
    }
}

pub(super) struct IndexMergeInfo {
    pub keys: String,
    pub tree_algorithm: &'static str,
    pub json_algorithm: &'static str,
}

impl IndexMergeInfo {
    pub fn new(root: &planner::IndexMergeNode) -> Self {
        let mut indexes = Vec::new();
        root.collect_index_names(&mut indexes);
        let (tree_algorithm, json_algorithm) = match root {
            planner::IndexMergeNode::Union {
                sort_union: true, ..
            } => ("sort-union", "sort_union"),
            planner::IndexMergeNode::Intersection { .. } => ("intersection", "intersection"),
            planner::IndexMergeNode::Union { .. } | planner::IndexMergeNode::Leaf(_) => {
                ("union", "union")
            }
        };
        Self {
            keys: indexes.join(","),
            tree_algorithm,
            json_algorithm,
        }
    }
}

pub(super) fn vector_access_labels(
    index: &crate::storage::IndexSchema,
) -> (&'static str, &'static str) {
    match index.vector_algo() {
        crate::storage::schema::VectorAlgo::IvfSq8 => ("IVF_SQ8", "nprobe"),
        crate::storage::schema::VectorAlgo::Hnsw => ("HNSW", "ef_search"),
    }
}

/// Shared identity and input edge for unary EXPLAIN operators. Renderers
/// keep their wire layout and CTE/ANALYZE context; node ownership is defined once.
struct UnaryExplain<'a> {
    plan: &'a planner::PhysicalPlan,
    input: &'a planner::PhysicalPlan,
    json_kind: &'static str,
}

impl<'a> UnaryExplain<'a> {
    fn from_plan(plan: &'a planner::PhysicalPlan) -> Option<Self> {
        let (input, json_kind) = match plan {
            planner::PhysicalPlan::NullExtend { input, .. } => (input, "null_extension"),
            planner::PhysicalPlan::Sort { input, .. } => (input, "sort"),
            planner::PhysicalPlan::IncrementalSort { input, .. } => (input, "incremental_sort"),
            planner::PhysicalPlan::TopK { input, .. } => (input, "top_k"),
            planner::PhysicalPlan::TopWindow { input, .. } => (input, "top_window"),
            planner::PhysicalPlan::ParallelTopK { input, .. } => (input, "parallel_top_k"),
            planner::PhysicalPlan::HashAggregate { input, .. } => (input, "hash_aggregate"),
            planner::PhysicalPlan::HashRollupAggregate { input, .. } => {
                (input, "hash_rollup_aggregate")
            }
            planner::PhysicalPlan::HashGroupingSetsAggregate { input, .. } => {
                (input, "hash_grouping_sets_aggregate")
            }
            planner::PhysicalPlan::Limit { input, .. } => (input, "limit"),
            planner::PhysicalPlan::Projection { input, .. } => (input, "projection"),
            planner::PhysicalPlan::Filter { input, .. } => (input, "filter"),
            planner::PhysicalPlan::Apply { outer: input, .. } => (input, "apply"),
            planner::PhysicalPlan::WindowAgg { input, .. } => (input, "window"),
            planner::PhysicalPlan::StreamingAggregate { input, .. } => {
                (input, "streaming_aggregate")
            }
            planner::PhysicalPlan::StreamingRollupAggregate { input, .. } => {
                (input, "streaming_rollup_aggregate")
            }
            planner::PhysicalPlan::StreamingGroupingSetsAggregate { input, .. } => {
                (input, "streaming_grouping_sets_aggregate")
            }
            planner::PhysicalPlan::StreamingDistinct { input, .. } => (input, "streaming_distinct"),
            planner::PhysicalPlan::ParallelHashAggregate { input, .. } => {
                (input, "parallel_hash_aggregate")
            }
            planner::PhysicalPlan::ParallelRollupAggregate { input, .. } => {
                (input, "parallel_rollup_aggregate")
            }
            planner::PhysicalPlan::ParallelGroupingSetsAggregate { input, .. } => {
                (input, "parallel_grouping_sets_aggregate")
            }
            planner::PhysicalPlan::ParallelSort { input, .. } => (input, "parallel_sort"),
            _ => return None,
        };
        Some(Self {
            plan,
            input,
            json_kind,
        })
    }

    fn tree_header(&self, indent: &str, arrow: &str) -> String {
        let plan = self.plan;
        let estimated_rows = &planner::estimated_rows(plan);
        let cost = &planner::plan_cost(plan).total;
        match plan {
            planner::PhysicalPlan::NullExtend {
                input: _, table, ..
            } => {
                let out = format!(
                    "{indent}{arrow}NULL extension on {} (cost={cost:.2} rows={estimated_rows:.0})",
                    table.alias.as_deref().unwrap_or(&table.schema.name)
                );
                out
            }
            planner::PhysicalPlan::HashAggregate {
                input, group_by, ..
            } => {
                // Grouped aggregates use a hash table; ungrouped collapse
                // to a single accumulator with no table at all. Label them
                // distinctly so EXPLAIN shows the actual algorithm (matches
                // PG's `HashAggregate` / `Aggregate` naming and the
                // `PhysicalPlan::HashAggregate` variant). MySQL's
                // `using temporary` qualifier means a sort/temp-table
                // group-by, which is not what this operator does.
                let label = if group_by.is_empty() {
                    "Aggregate"
                } else {
                    "HashAggregate"
                };
                let mut out =
                    format!("{indent}{arrow}{label} (cost={cost:.2} rows={estimated_rows:.0})");
                if super::plan::partition_wise_aggregate_storage_ids(input, group_by).is_some() {
                    append_tree_details(&mut out, vec![("partition_wise", "true".to_string())]);
                }
                out
            }
            planner::PhysicalPlan::HashRollupAggregate {
                input: _, group_by, ..
            } => {
                let out = format!(
                "{indent}{arrow}Hash rollup aggregate (keys={}, cost={cost:.2} rows={estimated_rows:.0})",
                group_by.len()
            );
                out
            }
            planner::PhysicalPlan::HashGroupingSetsAggregate {
                input: _,
                group_by,
                grouping_sets,
                ..
            } => {
                let out = format!(
                "{indent}{arrow}Hash grouping sets aggregate (keys={}, sets={}, cost={cost:.2} rows={estimated_rows:.0})",
                group_by.len(),
                grouping_sets.len()
            );
                out
            }
            planner::PhysicalPlan::ParallelHashAggregate {
                input: _, degree, ..
            } => {
                let out = format!(
                "{indent}{arrow}Parallel aggregate (workers={degree}, cost={cost:.2} rows={estimated_rows:.0})"
            );
                out
            }
            planner::PhysicalPlan::ParallelRollupAggregate {
                input: _,
                group_by,
                degree,
                ..
            } => {
                let out = format!(
                "{indent}{arrow}Parallel rollup aggregate (keys={}, workers={degree}, cost={cost:.2} rows={estimated_rows:.0})",
                group_by.len()
            );
                out
            }
            planner::PhysicalPlan::ParallelGroupingSetsAggregate {
                input: _,
                group_by,
                grouping_sets,
                degree,
                ..
            } => {
                let out = format!(
                "{indent}{arrow}Parallel grouping sets aggregate (keys={}, sets={}, workers={degree}, cost={cost:.2} rows={estimated_rows:.0})",
                group_by.len(),
                grouping_sets.len()
            );
                out
            }
            planner::PhysicalPlan::Sort { input: _, .. } => {
                let out = format!("{indent}{arrow}Sort (cost={cost:.2} rows={estimated_rows:.0})");
                out
            }
            planner::PhysicalPlan::IncrementalSort {
                input: _,
                presorted_prefix,
                ..
            } => {
                let out = format!(
                "{indent}{arrow}Incremental Sort (presorted={presorted_prefix}, cost={cost:.2} rows={estimated_rows:.0})"
            );
                out
            }
            planner::PhysicalPlan::TopK {
                input: _,
                k,
                offset,
                dedup_projection,
                ..
            } => {
                let name = if dedup_projection.is_some() {
                    "Distinct TopK"
                } else {
                    "TopK"
                };
                let out = format!(
                "{indent}{arrow}{name} (k={k}, offset={offset}, cost={cost:.2} rows={estimated_rows:.0})"
            );
                out
            }
            planner::PhysicalPlan::TopWindow {
                input: _,
                limit,
                offset,
                ..
            } => {
                let out = format!(
                "{indent}{arrow}TopWindow (limit={limit}, offset={offset}, cost={cost:.2} rows={estimated_rows:.0})"
            );
                out
            }
            planner::PhysicalPlan::ParallelTopK {
                input: _,
                k,
                offset,
                degree,
                dedup_projection,
                ..
            } => {
                let name = if dedup_projection.is_some() {
                    "Parallel Distinct TopK"
                } else {
                    "Parallel TopK"
                };
                let out = format!(
                "{indent}{arrow}{name} (k={k}, offset={offset}, workers={degree}, cost={cost:.2} rows={estimated_rows:.0})"
            );
                out
            }
            planner::PhysicalPlan::Limit {
                input: _,
                limit,
                offset,
                ..
            } => {
                let mut out = format!("{indent}{arrow}Limit");
                if let Some(limit) = limit {
                    out.push_str(&format!(" {limit}"));
                }
                if let Some(offset) = offset {
                    if *offset > 0 {
                        out.push_str(&format!(" offset {offset}"));
                    }
                }
                out
            }
            planner::PhysicalPlan::Projection { .. } => format!("{indent}{arrow}Distinct"),
            planner::PhysicalPlan::Filter {
                input: _,
                predicate,
                ..
            } => {
                let out = format!(
                "{indent}{arrow}Filter (cost={cost:.2} rows={estimated_rows:.0})  (condition: {})",
                explain_expr(predicate)
            );
                out
            }
            planner::PhysicalPlan::Apply {
                outer: _,
                predicate,
                ..
            } => {
                let out = format!(
                "{indent}{arrow}Apply (cost={cost:.2} rows={estimated_rows:.0})  (condition: {})",
                explain_expr(predicate)
            );
                out
            }
            planner::PhysicalPlan::WindowAgg {
                input: _, windows, ..
            } => {
                let names: Vec<String> = windows.iter().map(|w| w.name.to_uppercase()).collect();
                let out = format!(
                "{indent}{arrow}Window functions [{}] (cost={cost:.2} rows={estimated_rows:.0})",
                names.join(", ")
            );
                out
            }
            planner::PhysicalPlan::StreamingAggregate { input: _, .. } => {
                let out = format!(
                    "{indent}{arrow}Streaming aggregate (cost={cost:.2} rows={estimated_rows:.0})"
                );
                out
            }
            planner::PhysicalPlan::StreamingRollupAggregate {
                input: _, group_by, ..
            } => {
                let out = format!(
                "{indent}{arrow}Streaming rollup aggregate (keys={}, cost={cost:.2} rows={estimated_rows:.0})",
                group_by.len()
            );
                out
            }
            planner::PhysicalPlan::StreamingGroupingSetsAggregate {
                input: _,
                group_by,
                grouping_sets,
                ..
            } => {
                let out = format!(
                "{indent}{arrow}Streaming grouping sets aggregate (keys={}, sets={}, cost={cost:.2} rows={estimated_rows:.0})",
                group_by.len(),
                grouping_sets.len()
            );
                out
            }
            planner::PhysicalPlan::StreamingDistinct { input: _, .. } => {
                let out = format!(
                    "{indent}{arrow}Streaming distinct (cost={cost:.2} rows={estimated_rows:.0})"
                );
                out
            }
            planner::PhysicalPlan::ParallelSort {
                input: _, degree, ..
            } => {
                let out = format!(
                "{indent}{arrow}Parallel sort (workers={degree}, cost={cost:.2} rows={estimated_rows:.0})"
            );
                out
            }
            _ => unreachable!("UnaryExplain only accepts unary operators"),
        }
    }
}
