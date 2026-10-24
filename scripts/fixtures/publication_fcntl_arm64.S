/* Preserve Darwin variadic arguments while a fixed-argument hook runs. */
#if !defined(__APPLE__) || !defined(__arm64__)
#error "publication_fcntl_arm64.S requires macOS arm64"
#endif
.text
.p2align 2
.globl _publication_fcntl
_publication_fcntl:
    sub sp, sp, #112
    stp x0, x1, [sp, #0]
    stp x2, x3, [sp, #16]
    stp x4, x5, [sp, #32]
    stp x6, x7, [sp, #48]
    stp x8, x9, [sp, #64]
    stp x29, x30, [sp, #80]
    bl _publication_before_fcntl
    mov w10, w0
    ldp x0, x1, [sp, #0]
    ldp x2, x3, [sp, #16]
    ldp x4, x5, [sp, #32]
    ldp x6, x7, [sp, #48]
    ldp x8, x9, [sp, #64]
    ldp x29, x30, [sp, #80]
    add sp, sp, #112
    cbz w10, 1f
    cmp w10, #2
    b.eq 2f
    mov w0, #-1
    ret
1:
    b _fcntl
2:
    mov w0, #0
    ret
