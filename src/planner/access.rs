//! Borrowed operator descriptions shared by capability checks and construction.
//! These accessors expose layout only; callers retain their routing policies.

use super::{Expr, PhysicalPlan};

pub(crate) struct AggregateInput<'a> {
    pub(crate) input: &'a PhysicalPlan,
    pub(crate) group_by: &'a [Expr],
    pub(crate) having: Option<&'a Expr>,
    pub(crate) aggregates: &'a [Expr],
    pub(crate) workers: usize,
    pub(crate) grouping: bool,
}

impl PhysicalPlan {
    /// Ordinary and grouping aggregates share bindings, but grouping masks
    /// require a distinct output contract. Index aggregates have no such input.
    pub(crate) fn aggregate_input(&self) -> Option<AggregateInput<'_>> {
        let (input, group_by, having, aggregates, workers) = match self {
            Self::HashAggregate {
                input,
                group_by,
                having,
                aggregates,
                ..
            }
            | Self::StreamingAggregate {
                input,
                group_by,
                having,
                aggregates,
                ..
            }
            | Self::HashRollupAggregate {
                input,
                group_by,
                having,
                aggregates,
                ..
            }
            | Self::HashGroupingSetsAggregate {
                input,
                group_by,
                having,
                aggregates,
                ..
            }
            | Self::StreamingRollupAggregate {
                input,
                group_by,
                having,
                aggregates,
                ..
            }
            | Self::StreamingGroupingSetsAggregate {
                input,
                group_by,
                having,
                aggregates,
                ..
            } => (input, group_by, having, aggregates, 1),
            Self::ParallelHashAggregate {
                input,
                group_by,
                having,
                aggregates,
                degree,
                ..
            }
            | Self::ParallelRollupAggregate {
                input,
                group_by,
                having,
                aggregates,
                degree,
                ..
            }
            | Self::ParallelGroupingSetsAggregate {
                input,
                group_by,
                having,
                aggregates,
                degree,
                ..
            } => (
                input,
                group_by,
                having,
                aggregates,
                (*degree as usize).max(1),
            ),
            _ => return None,
        };
        Some(AggregateInput {
            input,
            group_by,
            having: having.as_ref(),
            aggregates,
            workers,
            grouping: !matches!(
                self,
                Self::HashAggregate { .. }
                    | Self::StreamingAggregate { .. }
                    | Self::ParallelHashAggregate { .. }
            ),
        })
    }

    pub(crate) fn ordinary_aggregate_input(&self) -> Option<AggregateInput<'_>> {
        self.aggregate_input()
            .filter(|aggregate| !aggregate.grouping)
    }
}
