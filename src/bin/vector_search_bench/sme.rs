//! Private f32 SME matrix kernel for the standalone vector-search benchmark.
//!
//! Query panels are packed during the measured search. Candidate rows are
//! transposed in ZA1 and consumed directly by ZA0 outer products, avoiding
//! a round trip through a memory-resident database panel.

use std::sync::OnceLock;

std::arch::global_asm!(
    r#"
    .text
    .p2align 2
    .globl _mydb_bench_sme_f32_lanes
_mydb_bench_sme_f32_lanes:
    .arch_extension sme
    rdsvl x0, #1
    lsr x0, x0, #2
    ret

    .p2align 2
    .globl _mydb_bench_sme_dot_rows
_mydb_bench_sme_dot_rows:
    .arch_extension sme
    // Rust has no live ZA state at this private entry point. Preserve D8-D15,
    // whose Z-register aliases are cleared when entering streaming mode.
    stp d8, d9, [sp, #-64]!
    stp d10, d11, [sp, #16]
    stp d12, d13, [sp, #32]
    stp d14, d15, [sp, #48]
    smstart
    ptrue p1.s
    cntw x9
    lsl x10, x2, #2
1:
    // ZA0 accumulates query/candidate dots; ZA1 transposes a row-major block.
    // Zero padding rows once per tile; every valid row is overwritten below.
    zero {{za}}
    cmp x3, x9
    csel x11, x3, x9, lo
    mov x8, #0
    mov x5, x0
2:
    whilelo p0.s, x8, x2
    add x6, x1, x8, lsl #2
    mov w12, #0
    and x14, x11, #-4
    cbz x14, 7f
3:
    ld1w {{za1h.s[w12, 0]}}, p0/z, [x6]
    add x6, x6, x10
    ld1w {{za1h.s[w12, 1]}}, p0/z, [x6]
    add x6, x6, x10
    ld1w {{za1h.s[w12, 2]}}, p0/z, [x6]
    add x6, x6, x10
    ld1w {{za1h.s[w12, 3]}}, p0/z, [x6]
    add x6, x6, x10
    add w12, w12, #4
    cmp x12, x14
    b.lo 3b
7:
    cmp x12, x11
    b.hs 9f
8:
    ld1w {{za1h.s[w12, 0]}}, p0/z, [x6]
    add x6, x6, x10
    add w12, w12, #1
    cmp x12, x11
    b.lo 8b
9:
    sub x13, x2, x8
    cmp x13, x9
    csel x13, x13, x9, lo
    mov w12, #0
    and x14, x13, #-4
    cbz x14, 7f
4:
    mova z1.s, p1/m, za1v.s[w12, 0]
    mova z3.s, p1/m, za1v.s[w12, 1]
    mova z5.s, p1/m, za1v.s[w12, 2]
    mova z7.s, p1/m, za1v.s[w12, 3]
    ld1w {{z0.s}}, p1/z, [x5]
    ld1w {{z2.s}}, p1/z, [x5, #1, mul vl]
    ld1w {{z4.s}}, p1/z, [x5, #2, mul vl]
    ld1w {{z6.s}}, p1/z, [x5, #3, mul vl]
    fmopa za0.s, p1/m, p1/m, z0.s, z1.s
    fmopa za0.s, p1/m, p1/m, z2.s, z3.s
    fmopa za0.s, p1/m, p1/m, z4.s, z5.s
    fmopa za0.s, p1/m, p1/m, z6.s, z7.s
    addvl x5, x5, #4
    add w12, w12, #4
    cmp x12, x14
    b.lo 4b
7:
    cmp x12, x13
    b.hs 9f
8:
    mova z1.s, p1/m, za1v.s[w12, 0]
    ld1w {{z0.s}}, p1/z, [x5]
    fmopa za0.s, p1/m, p1/m, z0.s, z1.s
    addvl x5, x5, #1
    add w12, w12, #1
    cmp x12, x13
    b.lo 8b
9:
    incw x8
    cmp x8, x2
    b.lo 2b
    mov w12, #0
5:
    st1w {{za0h.s[w12, 0]}}, p1, [x4]
    addvl x4, x4, #1
    add w12, w12, #1
    cmp x12, x9
    b.lo 5b
    madd x1, x11, x10, x1
    subs x3, x3, x11
    b.ne 1b
    smstop
    ldp d10, d11, [sp, #16]
    ldp d12, d13, [sp, #32]
    ldp d14, d15, [sp, #48]
    ldp d8, d9, [sp], #64
    ret
"#
);

unsafe extern "C" {
    fn mydb_bench_sme_f32_lanes() -> usize;
    fn mydb_bench_sme_dot_rows(
        queries: *const f32,
        rows: *const f32,
        dim: usize,
        row_count: usize,
        dots: *mut f32,
    );
}

pub(super) fn available() -> bool {
    static AVAILABLE: OnceLock<bool> = OnceLock::new();
    *AVAILABLE.get_or_init(|| unsafe {
        let mut value: libc::c_int = 0;
        let mut len = std::mem::size_of_val(&value);
        let name = c"hw.optional.arm.SME_F32F32";
        libc::sysctlbyname(
            name.as_ptr(),
            (&mut value as *mut libc::c_int).cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        ) == 0
            && value != 0
            && mydb::engine::vec_bench_api::sme_available()
    })
}

pub(super) fn lanes() -> usize {
    assert!(available(), "SME f32 matrix arithmetic is unavailable");
    // SAFETY: Darwin advertises and manages SME state for this process.
    unsafe { mydb_bench_sme_f32_lanes() }
}

/// Transpose at most one tile of row-major vectors into dimension-major panels.
pub(super) fn pack_tile(rows: &[f32], dim: usize, width: usize, packed: &mut [f32]) {
    assert!(dim > 0 && rows.len().is_multiple_of(dim));
    let count = rows.len() / dim;
    assert!(count <= width && packed.len() == dim * width);
    let full_rows = count / 4 * 4;
    let full_dims = dim / 4 * 4;
    // SAFETY: each four-row/four-column load is bounded by full_rows/full_dims;
    // the transposed stores cover the corresponding four lanes of each panel.
    unsafe {
        use std::arch::aarch64::*;
        for row in (0..full_rows).step_by(4) {
            for k in (0..full_dims).step_by(4) {
                let src = rows.as_ptr().add(row * dim + k);
                let a = vld1q_f32(src);
                let b = vld1q_f32(src.add(dim));
                let c = vld1q_f32(src.add(2 * dim));
                let d = vld1q_f32(src.add(3 * dim));
                let ab0 = vtrn1q_f32(a, b);
                let ab1 = vtrn2q_f32(a, b);
                let cd0 = vtrn1q_f32(c, d);
                let cd1 = vtrn2q_f32(c, d);
                let dst = packed.as_mut_ptr().add(k * width + row);
                vst1q_f32(dst, vcombine_f32(vget_low_f32(ab0), vget_low_f32(cd0)));
                vst1q_f32(
                    dst.add(width),
                    vcombine_f32(vget_low_f32(ab1), vget_low_f32(cd1)),
                );
                vst1q_f32(
                    dst.add(2 * width),
                    vcombine_f32(vget_high_f32(ab0), vget_high_f32(cd0)),
                );
                vst1q_f32(
                    dst.add(3 * width),
                    vcombine_f32(vget_high_f32(ab1), vget_high_f32(cd1)),
                );
            }
            for k in full_dims..dim {
                for r in row..row + 4 {
                    packed[k * width + r] = rows[r * dim + k];
                }
            }
        }
    }
    for k in 0..dim {
        for row in full_rows..count {
            packed[k * width + row] = rows[row * dim + k];
        }
        packed[k * width + count..(k + 1) * width].fill(0.0);
    }
}

/// Fuse row-major candidate transposition with matrix dots.
/// Output layout is [candidate tile][query lane][candidate lane].
pub(super) fn dot_rows(queries: &[f32], rows: &[f32], dim: usize, width: usize, dots: &mut [f32]) {
    assert_eq!(
        width,
        lanes(),
        "panel width must match the current streaming VL"
    );
    let panel_floats = dim.checked_mul(width).expect("SME panel size overflow");
    assert!(dim > 0 && queries.len() == panel_floats);
    assert!(rows.len().is_multiple_of(dim) && !rows.is_empty());
    let row_count = rows.len() / dim;
    let output_floats = row_count
        .div_ceil(width)
        .checked_mul(width)
        .and_then(|rows| rows.checked_mul(width))
        .expect("SME output size overflow");
    assert_eq!(dots.len(), output_floats);
    // SAFETY: lanes() checks support, width matches this thread's streaming VL,
    // and input/output lengths cover every valid row and padded output tile.
    unsafe {
        mydb_bench_sme_dot_rows(
            queries.as_ptr(),
            rows.as_ptr(),
            dim,
            row_count,
            dots.as_mut_ptr(),
        );
    }
}
