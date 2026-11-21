//! Global allocator: allocations of 1 MiB or more come from their own 2 MiB-aligned
//! anonymous mapping marked MADV_HUGEPAGE (transparent huge pages; a no-op where THP is
//! off), grown in place or moved by mremap without copying. Everything smaller goes to
//! the system allocator. Huge pages cut page faults ~500x for the big partition buffers
//! and make the kernel's teardown at exit cheap.

use std::alloc::{GlobalAlloc, Layout, System};

const BIG: usize = 1 << 20;
const HUGE: usize = 2 << 20;

pub struct HugeAlloc;

#[inline]
fn rounded(size: usize) -> usize {
    (size + HUGE - 1) & !(HUGE - 1)
}

#[inline]
fn is_big(layout: &Layout) -> bool {
    layout.size() >= BIG && layout.align() <= 4096
}

unsafe fn map_huge(size: usize) -> *mut u8 {
    let length = rounded(size);
    unsafe {
        // Over-map by 2 MiB, then trim so the mapping starts on a 2 MiB boundary.
        let raw = libc::mmap(std::ptr::null_mut(), length + HUGE, libc::PROT_READ | libc::PROT_WRITE,
                             libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0);
        if raw == libc::MAP_FAILED {
            return std::ptr::null_mut();
        }
        let start = raw as usize;
        let aligned = (start + HUGE - 1) & !(HUGE - 1);
        if aligned > start {
            libc::munmap(raw, aligned - start);
        }
        let tail = start + length + HUGE - (aligned + length);
        if tail > 0 {
            libc::munmap((aligned + length) as *mut libc::c_void, tail);
        }
        libc::madvise(aligned as *mut libc::c_void, length, libc::MADV_HUGEPAGE);
        aligned as *mut u8
    }
}

unsafe impl GlobalAlloc for HugeAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if is_big(&layout) { unsafe { map_huge(layout.size()) } } else { unsafe { System.alloc(layout) } }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // Fresh anonymous mappings are already zero.
        if is_big(&layout) { unsafe { map_huge(layout.size()) } } else { unsafe { System.alloc_zeroed(layout) } }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if is_big(&layout) {
            unsafe { libc::munmap(ptr.cast(), rounded(layout.size())) };
        } else {
            unsafe { System.dealloc(ptr, layout) }
        }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_layout = unsafe { Layout::from_size_align_unchecked(new_size, layout.align()) };
        match (is_big(&layout), is_big(&new_layout)) {
            (true, true) => unsafe {
                let (old, new) = (rounded(layout.size()), rounded(new_size));
                if old == new {
                    return ptr;
                }
                let moved = libc::mremap(ptr.cast(), old, new, libc::MREMAP_MAYMOVE);
                if moved == libc::MAP_FAILED {
                    return std::ptr::null_mut();
                }
                libc::madvise(moved, new, libc::MADV_HUGEPAGE);
                moved.cast()
            },
            (false, false) => unsafe { System.realloc(ptr, layout, new_size) },
            _ => unsafe {
                // Crossing the threshold: allocate, copy, free.
                let fresh = self.alloc(new_layout);
                if !fresh.is_null() {
                    std::ptr::copy_nonoverlapping(ptr, fresh, layout.size().min(new_size));
                    self.dealloc(ptr, layout);
                }
                fresh
            },
        }
    }
}
