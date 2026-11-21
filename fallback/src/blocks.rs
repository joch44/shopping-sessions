//! Block-list scatter buffers over one up-front address-space reservation.
//!
//! Each partition is a list of fixed blocks of `BLOCK` items carved from a per-thread bump
//! `Arena`; pushing is a store plus a counter bump, nothing is pre-sized, reallocated or
//! copied. Arenas take 2 MiB slabs from a single MAP_NORESERVE reservation by atomic bump,
//! so the hot phases make no mmap calls (each one takes mmap_lock for writing and stalls
//! every concurrent page fault on kernels before 6.4). A finished partition's blocks can be
//! handed to another scatter's arena (`recycle_partition`): those pages are already faulted
//! in and likely cached, so no fresh memory is touched for the next stage.
//! (Design after the 67 entry's arena; written for this pipeline.)

use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Items per block: 4 KiB for 32-byte records, 6 KiB for 48-byte clicks.
pub const BLOCK: usize = 128;
const HUGE: usize = 2 << 20;
const SLAB: usize = 2 << 20;
const FALLBACK_CHUNK: usize = 64 << 20;

static RES_NEXT: AtomicUsize = AtomicUsize::new(0);
static RES_END: AtomicUsize = AtomicUsize::new(0);

fn map(bytes: usize) -> usize {
    let len = bytes.div_ceil(HUGE) * HUGE;
    // SAFETY: anonymous private mapping; the result is checked.
    unsafe {
        let p = libc::mmap(std::ptr::null_mut(), len + HUGE, libc::PROT_READ | libc::PROT_WRITE,
                           libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE, -1, 0);
        assert!(p != libc::MAP_FAILED, "mmap failed");
        let aligned = (p as usize + HUGE - 1) & !(HUGE - 1);
        libc::madvise(aligned as *mut libc::c_void, len, libc::MADV_HUGEPAGE);
        aligned
    }
}

/// Reserves `bytes` of address space once, before any thread starts (nothing is committed
/// until touched).
pub fn reserve(bytes: usize) {
    let base = map(bytes);
    RES_NEXT.store(base, Ordering::Relaxed);
    RES_END.store(base + bytes, Ordering::Relaxed);
}

/// A 2 MiB-aligned range of at least `bytes` from the reservation (its own mapping only if
/// the reservation is exhausted or absent).
pub fn slab(bytes: usize) -> (usize, usize) {
    let size = bytes.div_ceil(SLAB) * SLAB;
    if RES_END.load(Ordering::Relaxed) != 0 {
        let p = RES_NEXT.fetch_add(size, Ordering::Relaxed);
        if p + size <= RES_END.load(Ordering::Relaxed) {
            return (p, p + size);
        }
    }
    let size = size.max(FALLBACK_CHUNK);
    let p = map(size);
    (p, p + size)
}

/// Per-thread bump allocator: recycled ranges first, then fresh slabs.
pub struct Arena {
    cur: usize,
    end: usize,
    free: Vec<(usize, usize)>,
}

impl Arena {
    fn new() -> Self {
        Arena { cur: 0, end: 0, free: Vec::new() }
    }

    fn alloc(&mut self, bytes: usize, align: usize) -> usize {
        let mut p = (self.cur + align - 1) & !(align - 1);
        if p + bytes > self.end {
            loop {
                match self.free.pop() {
                    Some((s, e)) => {
                        let q = (s + align - 1) & !(align - 1);
                        if q + bytes <= e {
                            self.cur = q + bytes;
                            self.end = e;
                            return q;
                        }
                    }
                    None => break,
                }
            }
            let (s, e) = slab(bytes + align);
            self.cur = s;
            self.end = e;
            p = (s + align - 1) & !(align - 1);
        }
        self.cur = p + bytes;
        p
    }
}

pub struct BlockScatter<T> {
    arena: Arena,
    /// per partition: the block being filled and its fill (BLOCK = full, so the first push
    /// allocates)
    cur: Vec<(*mut T, u32)>,
    /// per partition: every block, the one being filled last
    lists: Vec<Vec<*mut T>>,
    /// 32-byte items in paired mode: an even-indexed item waits here for its odd partner,
    /// then both go out as one non-temporal 64-byte line (blocks are 64-byte aligned and
    /// BLOCK is even, so pairs never straddle blocks). `flush` writes any item still waiting.
    pending: Vec<MaybeUninit<T>>,
    has_pending: Vec<bool>,
    paired: bool,
}

// SAFETY: the raw pointers are into memory this scatter owns exclusively (its arena's slabs
// or ranges handed to it); readers get shared slices only after the writer is done.
unsafe impl<T: Send> Send for BlockScatter<T> {}
unsafe impl<T: Sync> Sync for BlockScatter<T> {}

impl<T: Copy> BlockScatter<T> {
    pub fn new(parts: usize) -> Self {
        let mut pending = Vec::with_capacity(parts);
        // SAFETY: MaybeUninit needs no initialization; read only after written.
        unsafe { pending.set_len(parts) };
        BlockScatter {
            arena: Arena::new(),
            cur: vec![(std::ptr::null_mut(), BLOCK as u32); parts],
            lists: (0..parts).map(|_| Vec::new()).collect(),
            pending,
            has_pending: vec![false; parts],
            paired: false,
        }
    }

    /// Like `new`, but 32-byte items are stored in non-temporal pairs; call `flush` (and
    /// `fence`) before the blocks are read.
    pub fn new_paired(parts: usize) -> Self {
        BlockScatter { paired: true, ..Self::new(parts) }
    }

    #[cold]
    #[inline(never)]
    fn new_block(&mut self, part: usize) {
        let p = self.arena.alloc(BLOCK * std::mem::size_of::<T>(), 64) as *mut T;
        self.lists[part].push(p);
        self.cur[part] = (p, 0);
    }

    #[inline(always)]
    pub fn push(&mut self, part: usize, item: T) {
        if self.cur[part].1 as usize == BLOCK {
            self.new_block(part);
        }
        let (block, fill) = self.cur[part];
        #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
        if self.paired && std::mem::size_of::<T>() == 32 {
            if fill % 2 == 0 {
                self.pending[part].write(item);
                self.has_pending[part] = true;
                self.cur[part].1 = fill + 1;
                return;
            }
            // SAFETY: slots fill - 1 and fill are in this block (fill < BLOCK, fill odd), the
            // pair starts 64-byte aligned (aligned block, 32-byte items, even index), and the
            // two items' bytes form one __m512i.
            unsafe {
                use std::arch::x86_64::{__m256i, __m512i, _mm512_castsi256_si512, _mm512_inserti64x4, _mm512_stream_si512};
                let a = std::mem::transmute_copy::<T, __m256i>(self.pending[part].assume_init_ref());
                let b = std::mem::transmute_copy::<T, __m256i>(&item);
                let line: __m512i = _mm512_inserti64x4::<1>(_mm512_castsi256_si512(a), b);
                _mm512_stream_si512(block.add(fill as usize - 1).cast(), line);
            }
            self.has_pending[part] = false;
            self.cur[part].1 = fill + 1;
            return;
        }
        // SAFETY: fill < BLOCK, so the slot is inside this partition's current block.
        unsafe { block.add(fill as usize).write(item) };
        self.cur[part].1 = fill + 1;
    }

    /// Writes paired items still waiting for a partner. Call before `fence`.
    pub fn flush(&mut self) {
        for part in 0..self.cur.len() {
            if self.has_pending[part] {
                self.has_pending[part] = false;
                let (block, fill) = self.cur[part];
                // SAFETY: has_pending means slot fill - 1 of the current block was reserved
                // for pending[part], which `push` wrote.
                unsafe { block.add(fill as usize - 1).write(self.pending[part].assume_init()) };
            }
        }
    }

    pub fn len(&self, part: usize) -> usize {
        match self.lists[part].len() {
            0 => 0,
            n => (n - 1) * BLOCK + self.cur[part].1 as usize,
        }
    }

    /// The partition's blocks as slices, in push order.
    pub fn pieces(&self, part: usize) -> impl Iterator<Item = &[T]> + '_ {
        let n = self.lists[part].len();
        let last = self.cur[part].1 as usize;
        self.lists[part].iter().enumerate().map(move |(i, &p)| {
            let len = if i + 1 == n { last } else { BLOCK };
            // SAFETY: the first `len` slots of this block were written by `push`/`flush`.
            unsafe { std::slice::from_raw_parts(p as *const T, len) }
        })
    }

    /// Hands partition `part`'s blocks to `to`'s arena for reuse.
    ///
    /// # Safety
    /// Nothing may read this partition afterwards (its slices must be dead), and only one
    /// caller may recycle a given partition.
    pub unsafe fn recycle_partition<U: Copy>(&self, part: usize, to: &mut BlockScatter<U>) {
        let bytes = BLOCK * std::mem::size_of::<T>();
        for &p in &self.lists[part] {
            to.arena.free.push((p as usize, p as usize + bytes));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, PartialEq, Debug)]
    #[repr(C, align(32))]
    struct Item([u64; 4]);

    #[test]
    fn round_trip_plain_and_paired() {
        for paired in [false, true] {
            let mut s = if paired { BlockScatter::new_paired(3) } else { BlockScatter::new(3) };
            let item = |i: u64| Item([i, i + 1, i + 2, i + 3]);
            for i in 0..1000u64 {
                s.push((i % 3) as usize, item(i));
            }
            s.flush();
            for part in 0..3 {
                let got: Vec<Item> = s.pieces(part).flat_map(|b| b.iter().copied()).collect();
                let want: Vec<Item> = (0..1000).filter(|i| i % 3 == part as u64).map(item).collect();
                assert_eq!(got, want, "paired {paired} part {part}");
                assert_eq!(s.len(part), want.len());
            }
        }
    }

    #[test]
    fn recycled_blocks_are_reused() {
        let mut a: BlockScatter<Item> = BlockScatter::new(2);
        for i in 0..500u64 {
            a.push((i % 2) as usize, Item([i; 4]));
        }
        let mut b: BlockScatter<Item> = BlockScatter::new(1);
        let first = a.lists[0][0] as usize;
        unsafe { a.recycle_partition(0, &mut b) };
        for i in 0..10u64 {
            b.push(0, Item([i; 4]));
        }
        let reused = b.lists[0][0] as usize;
        assert!(a.lists[0].iter().any(|&p| p as usize == reused), "b took a recycled block");
        assert_eq!(b.pieces(0).next().unwrap()[3], Item([3; 4]));
        let _ = first;
        // partition 1 untouched
        assert_eq!(a.len(1), 250);
    }
}
