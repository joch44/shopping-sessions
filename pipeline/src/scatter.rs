//! Partitioned output buffers for one thread: one arena (a single allocation, so the
//! huge-page allocator backs it with 2 MiB pages when it is big enough) holding a fixed
//! region of `cap` items per partition, plus an overflow vector per partition for the rare
//! partition that outgrows its region. Readers get each partition as two slices, in place.
//! Regions are sized from the input estimate; a wrong estimate only costs speed.

use std::mem::MaybeUninit;

/// Orders this thread's earlier non-temporal stores before any later read by other threads.
#[inline]
pub fn fence() {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: sfence has no preconditions.
    unsafe {
        std::arch::x86_64::_mm_sfence()
    };
}

pub struct Scatter<T> {
    data: Vec<MaybeUninit<T>>,
    cap: usize,
    lens: Vec<usize>,
    overflow: Vec<Vec<T>>,
    /// 32-byte items: the even-indexed item of a partition waits here until its odd partner
    /// arrives, then both are stored as one non-temporal 64-byte line. `flush` writes any
    /// item still waiting; it must run before the arena is read.
    pending: Vec<MaybeUninit<T>>,
    has_pending: Vec<bool>,
    /// Pairing is opt-in (`new_paired`): only callers that `flush` may use it.
    paired: bool,
}

impl<T: Copy> Scatter<T> {
    pub fn new(parts: usize, cap: usize) -> Self {
        let mut data = Vec::with_capacity(parts * cap);
        // SAFETY: MaybeUninit needs no initialization; only written slots are ever read.
        unsafe { data.set_len(parts * cap) };
        let mut pending = Vec::with_capacity(parts);
        // SAFETY: MaybeUninit needs no initialization; a slot is read only after it was written.
        unsafe { pending.set_len(parts) };
        Self { data, cap, lens: vec![0; parts], overflow: (0..parts).map(|_| Vec::new()).collect(), pending, has_pending: vec![false; parts], paired: false }
    }

    /// Like `new`, but 32-byte items are stored in non-temporal pairs; the caller must call
    /// `flush` before the arena is read.
    pub fn new_paired(parts: usize, cap: usize) -> Self {
        Self { paired: true, ..Self::new(parts, cap) }
    }

    /// Writes 32-byte items still waiting for a partner (see `pending`). Call before `fence`.
    pub fn flush(&mut self) {
        for part in 0..self.lens.len() {
            if self.has_pending[part] {
                self.has_pending[part] = false;
                let at = part * self.cap + self.lens[part] - 1;
                // SAFETY: has_pending means pending[part] was written by `push`.
                self.data[at].write(unsafe { self.pending[part].assume_init() });
            }
        }
    }

    #[inline(always)]
    pub fn push(&mut self, part: usize, item: T) {
        let len = self.lens[part];
        if len < self.cap {
            // A 64-byte item at a 64-byte-aligned slot is one whole cache line: store it with a
            // non-temporal store, which skips reading the line first (read-for-ownership).
            // Measured on the mirror: parse events 0.77 -> 0.655 s at 20M. Callers must `fence`
            // before other threads read the arena.
            #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
            if self.paired && std::mem::size_of::<T>() == 32 {
                if len % 2 == 0 {
                    let slot = self.data.as_mut_ptr().wrapping_add(part * self.cap + len);
                    if len + 1 < self.cap && slot as usize % 64 == 0 {
                        self.pending[part].write(item);
                        self.has_pending[part] = true;
                        self.lens[part] = len + 1;
                        return;
                    }
                } else if self.has_pending[part] {
                    let slot = self.data.as_mut_ptr().wrapping_add(part * self.cap + len - 1);
                    // SAFETY: the pair (len - 1, len) is in bounds (len < cap) and 64-byte aligned
                    // (checked when the first item was held); T is a 32-byte Copy type, so the
                    // two items' bytes form one __m512i.
                    unsafe {
                        use std::arch::x86_64::{__m256i, __m512i, _mm512_inserti64x4, _mm512_castsi256_si512, _mm512_stream_si512};
                        let a = std::mem::transmute_copy::<T, __m256i>(self.pending[part].assume_init_ref());
                        let b = std::mem::transmute_copy::<T, __m256i>(&item);
                        let line: __m512i = _mm512_inserti64x4::<1>(_mm512_castsi256_si512(a), b);
                        _mm512_stream_si512(slot.cast(), line);
                    }
                    self.has_pending[part] = false;
                    self.lens[part] = len + 1;
                    return;
                }
            }
            #[cfg(all(target_arch = "x86_64", target_feature = "avx512f"))]
            if std::mem::size_of::<T>() == 64 {
                let slot = self.data.as_mut_ptr().wrapping_add(part * self.cap + len);
                if slot as usize % 64 == 0 {
                    // SAFETY: slot is in bounds (len < cap), 64-byte aligned, and T is a 64-byte
                    // Copy type, so its bytes form one __m512i.
                    unsafe {
                        use std::arch::x86_64::{__m512i, _mm512_stream_si512};
                        _mm512_stream_si512(slot.cast(), std::mem::transmute_copy::<T, __m512i>(&item));
                    }
                    self.lens[part] = len + 1;
                    return;
                }
            }
            // AVX2 build: the same non-temporal store as two 32-byte halves.
            #[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(target_feature = "avx512f")))]
            if std::mem::size_of::<T>() == 64 {
                let slot = self.data.as_mut_ptr().wrapping_add(part * self.cap + len);
                if slot as usize % 64 == 0 {
                    // SAFETY: as above; two aligned 32-byte stores cover the 64-byte item.
                    unsafe {
                        use std::arch::x86_64::{__m256i, _mm256_stream_si256};
                        let halves = std::mem::transmute_copy::<T, [__m256i; 2]>(&item);
                        _mm256_stream_si256(slot.cast(), halves[0]);
                        _mm256_stream_si256(slot.cast::<__m256i>().add(1), halves[1]);
                    }
                    self.lens[part] = len + 1;
                    return;
                }
            }
            self.data[part * self.cap + len].write(item);
            self.lens[part] = len + 1;
        } else {
            self.overflow[part].push(item);
        }
    }

    pub fn len(&self, part: usize) -> usize {
        self.lens[part] + self.overflow[part].len()
    }

    /// The partition's items: its region, then its overflow.
    pub fn pieces(&self, part: usize) -> [&[T]; 2] {
        let start = part * self.cap;
        let region = &self.data[start..start + self.lens[part]];
        // SAFETY: the first lens[part] slots of the region were written by `push`.
        let region = unsafe { std::slice::from_raw_parts(region.as_ptr().cast::<T>(), region.len()) };
        [region, &self.overflow[part]]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, PartialEq, Debug)]
    #[repr(C, align(64))]
    struct Line([u64; 8]);

    #[test]
    fn sixty_four_byte_items_round_trip() {
        for cap in [0, 1, 7, 64] {
            let mut scatter = Scatter::new(3, cap);
            let item = |i: u64| Line([i, i + 1, i + 2, i + 3, i + 4, i + 5, i + 6, i + 7]);
            for i in 0..200u64 {
                scatter.push((i % 3) as usize, item(i));
            }
            fence();
            for part in 0..3 {
                let got: Vec<Line> = scatter.pieces(part).concat();
                let want: Vec<Line> = (0..200).filter(|i| i % 3 == part as u64).map(item).collect();
                assert_eq!(got, want, "cap {cap} part {part}");
            }
        }
    }

    #[derive(Clone, Copy, PartialEq, Debug)]
    #[repr(C, align(32))]
    struct Half([u64; 4]);

    #[test]
    fn thirty_two_byte_items_round_trip() {
        for cap in [0, 1, 2, 3, 7, 64] {
            let mut scatter = Scatter::new_paired(3, cap);
            let item = |i: u64| Half([i, i + 1, i + 2, i + 3]);
            for i in 0..200u64 {
                scatter.push((i % 3) as usize, item(i));
                if i % 37 == 0 {
                    scatter.flush(); // a chunk boundary in the middle of a pair
                }
            }
            scatter.flush();
            fence();
            for part in 0..3 {
                let got: Vec<Half> = scatter.pieces(part).concat();
                let want: Vec<Half> = (0..200).filter(|i| i % 3 == part as u64).map(item).collect();
                assert_eq!(got, want, "cap {cap} part {part}");
            }
        }
    }

    #[test]
    fn keeps_every_item_in_its_partition() {
        for cap in [0, 1, 3, 100] {
            let mut scatter = Scatter::new(4, cap);
            for item in 0..50u32 {
                scatter.push((item % 4) as usize, item);
            }
            for part in 0..4 {
                let got: Vec<u32> = scatter.pieces(part).concat();
                let want: Vec<u32> = (0..50).filter(|i| i % 4 == part as u32).collect();
                assert_eq!(got, want, "cap {cap} part {part}");
                assert_eq!(scatter.len(part), want.len());
            }
        }
    }
}
