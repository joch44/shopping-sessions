//! Partitioned output buffers for one thread: one arena (a single allocation, so the
//! huge-page allocator backs it with 2 MiB pages when it is big enough) holding a fixed
//! region of `cap` items per partition, plus an overflow vector per partition for the rare
//! partition that outgrows its region. Readers get each partition as two slices, in place.
//! Regions are sized from the input estimate; a wrong estimate only costs speed.

use std::mem::MaybeUninit;

pub struct Scatter<T> {
    data: Vec<MaybeUninit<T>>,
    cap: usize,
    lens: Vec<usize>,
    overflow: Vec<Vec<T>>,
}

impl<T: Copy> Scatter<T> {
    pub fn new(parts: usize, cap: usize) -> Self {
        let mut data = Vec::with_capacity(parts * cap);
        // SAFETY: MaybeUninit needs no initialization; only written slots are ever read.
        unsafe { data.set_len(parts * cap) };
        Self { data, cap, lens: vec![0; parts], overflow: (0..parts).map(|_| Vec::new()).collect() }
    }

    #[inline(always)]
    pub fn push(&mut self, part: usize, item: T) {
        let len = self.lens[part];
        if len < self.cap {
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
