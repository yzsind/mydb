//! One exhaustive child definition for physical-plan inspection and rewriting.
use super::PhysicalPlan;

pub(crate) type PlanChildren<'a> = smallvec::SmallVec<[&'a PhysicalPlan; 2]>;
pub(crate) type PlanChildrenMut<'a> = smallvec::SmallVec<[&'a mut PhysicalPlan; 2]>;

macro_rules! define_children {
    ($name:ident, $children:ident, [$($mutable:tt)*]) => {
        /// Direct children in execution/display order, without a heap allocation.
        pub(crate) fn $name(plan: & $($mutable)* PhysicalPlan) -> $children<'_> {
            let mut out = $children::new();
            match plan {
                PhysicalPlan::NullExtend { input, .. }
                | PhysicalPlan::Projection { input, .. }
                | PhysicalPlan::Filter { input, .. }
                | PhysicalPlan::Apply { outer: input, .. }
                | PhysicalPlan::Sort { input, .. }
                | PhysicalPlan::IncrementalSort { input, .. }
                | PhysicalPlan::ParallelSort { input, .. }
                | PhysicalPlan::TopK { input, .. }
                | PhysicalPlan::ParallelTopK { input, .. }
                | PhysicalPlan::TopWindow { input, .. }
                | PhysicalPlan::Limit { input, .. }
                | PhysicalPlan::StreamingDistinct { input, .. }
                | PhysicalPlan::HashAggregate { input, .. }
                | PhysicalPlan::HashRollupAggregate { input, .. }
                | PhysicalPlan::HashGroupingSetsAggregate { input, .. }
                | PhysicalPlan::StreamingAggregate { input, .. }
                | PhysicalPlan::StreamingRollupAggregate { input, .. }
                | PhysicalPlan::StreamingGroupingSetsAggregate { input, .. }
                | PhysicalPlan::WindowAgg { input, .. }
                | PhysicalPlan::Materialize { input, .. }
                | PhysicalPlan::Exchange { input, .. }
                | PhysicalPlan::ParallelHashAggregate { input, .. }
                | PhysicalPlan::ParallelRollupAggregate { input, .. }
                | PhysicalPlan::ParallelGroupingSetsAggregate { input, .. } => out.push(input),
                PhysicalPlan::HashJoin { left, right, .. }
                | PhysicalPlan::NestedLoopJoin { left, right, .. }
                | PhysicalPlan::AdaptiveJoin { left, right, .. }
                | PhysicalPlan::SetOp { left, right, .. }
                | PhysicalPlan::RecursiveUnion {
                    anchor: left,
                    recursive: right,
                    ..
                } => {
                    out.push(left);
                    out.push(right);
                }
                PhysicalPlan::IndexNestedLoopJoin { left, .. } => out.push(left),
                PhysicalPlan::IndexInLookup { subquery_plan, .. } => out.push(subquery_plan),
                PhysicalPlan::ValuesScan { .. }
                | PhysicalPlan::JsonTableScan { .. }
                | PhysicalPlan::SystemScan { .. }
                | PhysicalPlan::SeqScan { .. }
                | PhysicalPlan::ZoneMapScan { .. }
                | PhysicalPlan::IndexMerge { .. }
                | PhysicalPlan::IndexLookup { .. }
                | PhysicalPlan::IndexMultiLookup { .. }
                | PhysicalPlan::IndexRangeScan { .. }
                | PhysicalPlan::IndexFastScan { .. }
                | PhysicalPlan::FulltextScan { .. }
                | PhysicalPlan::SpatialScan { .. }
                | PhysicalPlan::VectorIndexScan { .. }
                | PhysicalPlan::VectorBruteScan { .. }
                | PhysicalPlan::CteScan { .. }
                | PhysicalPlan::GenerateSeries { .. }
                | PhysicalPlan::IndexAggregate { .. }
                | PhysicalPlan::CoveringIndexAggregate { .. }
                | PhysicalPlan::ParallelSeqScan { .. }
                | PhysicalPlan::ParallelZoneMapScan { .. }
                | PhysicalPlan::ParallelIndexFastScan { .. }
                | PhysicalPlan::ParallelIndexRangeScan { .. }
                | PhysicalPlan::ParallelIndexAggregate { .. } => {}
            }
            out
        }
    };
}

define_children!(plan_children, PlanChildren, []);
define_children!(plan_children_mut, PlanChildrenMut, [mut]);
