#include <metal_stdlib>
#include <metal_simdgroup_matrix>
using namespace metal;

// Each SIMD group owns an 8x8 output tile. Four groups cover either 8x32
// (small query batches) or 16x16 outputs. Padded inputs keep matrix operations
// uniform across the whole SIMD group, including all dimension/row tails.
kernel void vector_dots(
    device const float *queries [[buffer(0)]],
    device const float *database [[buffer(1)]],
    device float *dots [[buffer(2)]],
    constant uint4 &shape [[buffer(3)]],
    uint2 group [[threadgroup_position_in_grid]],
    uint simd_group [[simdgroup_index_in_threadgroup]]) {
    const bool small = shape.z <= 8;
    const uint query = group.y * (small ? 8 : 16) + (small ? 0 : simd_group / 2 * 8);
    const uint row = group.x * (small ? 32 : 16) + (small ? simd_group : simd_group % 2) * 8;
    simdgroup_float8x8 accumulator = make_filled_simdgroup_matrix<float, 8, 8>(0.0f);
    for (uint k = 0; k < shape.x; k += 8) {
        simdgroup_float8x8 a, b;
        simdgroup_load(a, queries + ulong(query) * shape.x + k, shape.x);
        simdgroup_load(b, database + ulong(row) * shape.x + k, shape.x, ulong2(0), true);
        simdgroup_multiply_accumulate(accumulator, a, b, accumulator);
    }
    simdgroup_store(accumulator, dots + ulong(query) * shape.y + row, shape.y);
}

// Compressed values are only a conservative filter. Decode to float before
// multiplying and accumulating; final distances are recomputed from FP32 data.
constant uint filter_width [[function_constant(0)]];
kernel void vector_filter(
    device const half *queries [[buffer(0)]],
    device const half *database [[buffer(1)]],
    device float *dots [[buffer(2)]],
    constant uint4 &shape [[buffer(3)]],
    uint2 group [[threadgroup_position_in_grid]],
    uint simd_group [[simdgroup_index_in_threadgroup]],
    uint lane [[thread_index_in_simdgroup]]) {
    const uint row = group.x * 4 + simd_group;
    const uint first = group.y * filter_width;
    float4 accumulator[8];
    for (uint j = 0; j < filter_width; ++j) accumulator[j] = float4(0.0f);
    for (uint k = lane * 4; k < shape.x; k += 128) {
        float4 value = float4(*reinterpret_cast<device const half4 *>(database + ulong(row) * shape.x + k));
        for (uint j = 0; j < filter_width; ++j) {
            float4 query = float4(*reinterpret_cast<device const half4 *>(queries + ulong(first + j) * shape.x + k));
            accumulator[j] = fma(value, query, accumulator[j]);
        }
    }
    for (uint j = 0; j < filter_width; ++j) {
        float4 value = accumulator[j];
        float sum = simd_sum((value.x + value.y) + (value.z + value.w));
        if (lane == 0) dots[ulong(first + j) * shape.y + row] = sum;
    }
}
