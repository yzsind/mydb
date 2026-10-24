//! Linux large-block allocation with deterministic release to the OS.
//!
//! glibc raises its mmap/trim thresholds after freeing large blocks. Parallel
//! analytical queries can then leave tens of MiB at the top of each arena;
//! malloc_trim does not trim non-main arena tops. Keep large Rust allocations
//! outside those arenas, while ordinary small allocations use System.

use std::alloc::{GlobalAlloc, Layout, System};

pub struct MappedSystem;

const MAPPED_MIN: usize = 128 * 1024;
// Linux mappings satisfy at least 4 KiB alignment on supported targets. More
// highly aligned custom types retain System's platform-specific implementation.
const MAPPED_ALIGNMENT: usize = 4096;

fn mapped(layout: Layout) -> bool {
    layout.size() >= MAPPED_MIN && layout.align() <= MAPPED_ALIGNMENT
}

// SAFETY: every allocation is selected solely by its layout. Deallocation and
// reallocation receive that same layout, so no mutable routing state or hidden
// headers are needed. Failed reallocations leave the original block intact.
unsafe impl GlobalAlloc for MappedSystem {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if !mapped(layout) {
            return unsafe { System.alloc(layout) };
        }
        let pointer = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                layout.size(),
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if pointer == libc::MAP_FAILED {
            std::ptr::null_mut()
        } else {
            pointer.cast()
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if mapped(layout) {
            // Anonymous mappings are already zero-initialized, lazily.
            unsafe { self.alloc(layout) }
        } else {
            unsafe { System.alloc_zeroed(layout) }
        }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        if mapped(layout) {
            unsafe {
                libc::munmap(pointer.cast(), layout.size());
            }
        } else {
            unsafe {
                System.dealloc(pointer, layout);
            }
        }
    }

    unsafe fn realloc(&self, pointer: *mut u8, old: Layout, new_size: usize) -> *mut u8 {
        let Ok(new) = Layout::from_size_align(new_size, old.align()) else {
            return std::ptr::null_mut();
        };
        match (mapped(old), mapped(new)) {
            (false, false) => unsafe { System.realloc(pointer, old, new_size) },
            (true, true) => {
                // mremap preserves the bytes and avoids a second resident
                // copy when a spill buffer/hash array grows.
                let resized = unsafe {
                    libc::mremap(pointer.cast(), old.size(), new_size, libc::MREMAP_MAYMOVE)
                };
                if resized == libc::MAP_FAILED {
                    std::ptr::null_mut()
                } else {
                    resized.cast()
                }
            }
            _ => {
                let resized = unsafe { self.alloc(new) };
                if !resized.is_null() {
                    unsafe {
                        std::ptr::copy_nonoverlapping(pointer, resized, old.size().min(new_size));
                        self.dealloc(pointer, old);
                    }
                }
                resized
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapped_allocations_are_zeroed_and_aligned() {
        for align in [1, 16, 64, 4096, 8192] {
            let layout = Layout::from_size_align(MAPPED_MIN * 2 + 7, align).unwrap();
            unsafe {
                let pointer = MappedSystem.alloc_zeroed(layout);
                assert!(!pointer.is_null());
                assert_eq!(pointer as usize % align, 0);
                assert!(std::slice::from_raw_parts(pointer, layout.size())
                    .iter()
                    .all(|b| *b == 0));
                MappedSystem.dealloc(pointer, layout);
            }
        }
    }

    #[test]
    fn failed_mapped_realloc_keeps_the_original_allocation() {
        let layout = Layout::from_size_align(MAPPED_MIN, 1).unwrap();
        unsafe {
            let pointer = MappedSystem.alloc(layout);
            assert!(!pointer.is_null());
            pointer.write(0x7b);
            assert!(MappedSystem.realloc(pointer, layout, usize::MAX).is_null());
            assert!(MappedSystem
                .realloc(pointer, layout, isize::MAX as usize)
                .is_null());
            assert_eq!(pointer.read(), 0x7b);
            MappedSystem.dealloc(pointer, layout);
        }
    }

    #[test]
    fn realloc_preserves_bytes_across_both_allocation_routes() {
        let sizes = [
            1024,
            MAPPED_MIN,
            MAPPED_MIN * 3 + 17,
            MAPPED_MIN + 3,
            2048,
            512,
        ];
        unsafe {
            let mut layout = Layout::from_size_align(sizes[0], 64).unwrap();
            let mut pointer = MappedSystem.alloc(layout);
            assert!(!pointer.is_null());
            std::ptr::write_bytes(pointer, 0x5a, layout.size());
            for size in sizes.into_iter().skip(1) {
                let next = MappedSystem.realloc(pointer, layout, size);
                assert!(!next.is_null());
                assert_eq!(next as usize % layout.align(), 0);
                assert!(std::slice::from_raw_parts(next, layout.size().min(size))
                    .iter()
                    .all(|b| *b == 0x5a));
                std::ptr::write_bytes(next, 0x5a, size);
                pointer = next;
                layout = Layout::from_size_align(size, layout.align()).unwrap();
            }
            MappedSystem.dealloc(pointer, layout);
        }
    }
}
