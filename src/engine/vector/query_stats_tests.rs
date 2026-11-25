use super::{
    codec, distance::Metric, graph::VectorGraph, ivf::IvfParams, native_hnsw::NativeHnsw,
    native_ivf::NativeIvf, quantization::Trainer, VectorIndexRuntimeStats,
};
use crate::{storage::vector_config::QuantizationConfig, vector::VectorDataType};
use std::sync::Arc;

fn native_graphs(requested: bool, trained: bool) -> [VectorGraph; 2] {
    let model = trained.then(|| {
        let mut trainer = Trainer::new(
            QuantizationConfig::Sq8,
            VectorDataType::Float32,
            3,
            Metric::L2,
        )
        .unwrap();
        trainer.observe(&codec::from_f32(&[1.0, 2.0, 3.0])).unwrap();
        trainer.finish().unwrap().unwrap()
    });
    let mut hnsw = NativeHnsw::new(VectorDataType::Float32, 3, Metric::L2, 4, 16, None).unwrap();
    hnsw.set_quantizer(requested, model.clone(), 9);
    let mut ivf = NativeIvf::new(
        VectorDataType::Float32,
        3,
        Metric::L2,
        IvfParams::default(),
        None,
    )
    .unwrap();
    ivf.set_quantizer(requested, model, 9);
    [VectorGraph::NativeHnsw(hnsw), VectorGraph::NativeIvf(ivf)]
}

fn counters(graph: &VectorGraph) -> (u64, u64, u64, u64) {
    let mut stats = VectorIndexRuntimeStats::default();
    stats.accumulate(graph);
    (
        stats.generation,
        stats.candidates_returned,
        stats.heap_reranks,
        stats.exact_fallbacks,
    )
}

#[test]
fn native_query_stats_are_quantization_scoped_and_aggregate_across_backends() {
    for (requested, trained) in [(false, false), (true, false), (true, true)] {
        let graphs = native_graphs(requested, trained);
        let generation = if trained { 9 } else { 0 };
        let mut combined = VectorIndexRuntimeStats::default();
        for graph in &graphs {
            assert_eq!(counters(graph), (generation, 0, 0, 0));
            graph.record_query_stats(7, 3, false);
            graph.record_query_stats(11, 5, true);
            graph.record_query_stats(0, 0, true);
            let expected = if requested { (18, 8, 2) } else { (0, 0, 0) };
            assert_eq!(
                counters(graph),
                (generation, expected.0, expected.1, expected.2)
            );
            combined.accumulate(graph);
        }
        assert_eq!(combined.generation, generation);
        assert_eq!(combined.candidates_returned, if requested { 36 } else { 0 });
        assert_eq!(combined.heap_reranks, if requested { 16 } else { 0 });
        assert_eq!(combined.exact_fallbacks, if requested { 4 } else { 0 });
    }
}

#[test]
fn native_query_stats_preserve_concurrent_totals_and_reset_on_restore() {
    for graph in native_graphs(true, true) {
        let graph = Arc::new(graph);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for i in 0..1000 {
                        graph.record_query_stats(3, 2, i % 2 == 0);
                    }
                });
            }
        });
        assert_eq!(counters(&graph), (9, 12_000, 8_000, 2_000));
        let mut bytes = Vec::new();
        let restored = match graph.as_ref() {
            VectorGraph::NativeHnsw(graph) => {
                graph.serialize_into(&mut bytes, 77).unwrap();
                VectorGraph::NativeHnsw(NativeHnsw::deserialize(&bytes).unwrap())
            }
            VectorGraph::NativeIvf(graph) => {
                graph.serialize_into(&mut bytes, 77).unwrap();
                VectorGraph::NativeIvf(NativeIvf::deserialize(&bytes).unwrap())
            }
            _ => unreachable!(),
        };
        assert_eq!(counters(&restored), (9, 0, 0, 0));
        assert_eq!(restored.last_persisted_lsn(), 77);
    }
}
