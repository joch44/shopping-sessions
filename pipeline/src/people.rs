//! user ID key -> person key, for the IDs that appear in links (rules.md "Linked IDs").
//! An ID not in the table is a person on its own, so `get` returns the ID itself.
//!
//! 64 open-addressing subtables chosen by the top 6 hash bits, each with a 1-byte tag per
//! slot (0 = empty, else 0x80 | 7 other hash bits) and interleaved 32-byte (id, person)
//! cells. A miss usually ends at its first tag byte without touching the cells. Subtables
//! are independent (probing wraps inside one), so `build` fills them in parallel; lookups
//! give the same answers whatever the insertion order. `prefetch` issues the cache misses
//! for a lookup some iterations before it is needed.

use crate::key::{Key, mix};

const SUB_BITS: u32 = 6;
const SUBS: usize = 1 << SUB_BITS;

pub struct People {
    tags: Vec<u8>,
    cells: Vec<(Key, Key)>,
    base: [usize; SUBS],
    mask: [usize; SUBS],
}

#[inline(always)]
fn sub_of(h: u64) -> usize {
    (h >> (64 - SUB_BITS)) as usize
}

#[inline(always)]
fn tag_of(h: u64) -> u8 {
    0x80 | ((h >> 8) as u8 & 0x7F)
}

impl People {
    /// Serial build (tests, small inputs).
    pub fn new(pairs: impl ExactSizeIterator<Item = (Key, Key)>) -> Self {
        let pairs: Vec<(Key, Key)> = pairs.collect();
        Self::build(&[pairs], 1)
    }

    /// Builds from (id, person) pairs given in pieces; `threads` workers fill the subtables.
    pub fn build(pieces: &[Vec<(Key, Key)>], threads: usize) -> Self {
        // Group by subtable, per piece (pieces are independent, so threads can split them).
        let grouped: Vec<Vec<Vec<(Key, Key)>>> = crate::run_tasks(threads.max(1), pieces.len(), Vec::new, |out: &mut Vec<(usize, Vec<Vec<(Key, Key)>>)>, at| {
            let mut subs: Vec<Vec<(Key, Key)>> = (0..SUBS).map(|_| Vec::new()).collect();
            for &(id, person) in &pieces[at] {
                subs[sub_of(mix(id))].push((id, person));
            }
            out.push((at, subs));
        })
        .into_iter()
        .flatten()
        .fold(vec![Vec::new(); pieces.len()], |mut all, (at, subs)| {
            all[at] = subs;
            all
        });
        let mut base = [0usize; SUBS];
        let mut mask = [0usize; SUBS];
        let mut total = 0;
        for s in 0..SUBS {
            let n: usize = grouped.iter().map(|g| g.get(s).map_or(0, Vec::len)).sum();
            let slots = (n * 2).next_power_of_two().max(16);
            base[s] = total;
            mask[s] = slots - 1;
            total += slots;
        }
        let mut tags = vec![0u8; total];
        let mut cells = vec![(0, 0); total];
        let (tags_at, cells_at) = (tags.as_mut_ptr() as usize, cells.as_mut_ptr() as usize);
        crate::run_tasks(threads.max(1), SUBS, || (), |_, s| {
            // SAFETY: subtable s owns slots base[s]..=base[s]+mask[s]; no other task touches them.
            let tags = unsafe { std::slice::from_raw_parts_mut((tags_at as *mut u8).add(base[s]), mask[s] + 1) };
            let cells = unsafe { std::slice::from_raw_parts_mut((cells_at as *mut (Key, Key)).add(base[s]), mask[s] + 1) };
            for piece in &grouped {
                let Some(entries) = piece.get(s) else { continue };
                for &(id, person) in entries {
                    let h = mix(id);
                    let mut slot = (h >> 20) as usize & mask[s];
                    while tags[slot] != 0 && cells[slot].0 != id {
                        slot = (slot + 1) & mask[s];
                    }
                    tags[slot] = tag_of(h);
                    cells[slot] = (id, person);
                }
            }
        });
        Self { tags, cells, base, mask }
    }

    #[inline(always)]
    fn start(&self, id: Key) -> (usize, usize, usize, u8) {
        let h = mix(id);
        let s = sub_of(h);
        (self.base[s], self.mask[s], (h >> 20) as usize & self.mask[s], tag_of(h))
    }

    #[inline]
    pub fn get(&self, id: Key) -> Key {
        let (base, mask, mut slot, tag) = self.start(id);
        loop {
            let held = self.tags[base + slot];
            if held == 0 {
                return id;
            }
            if held == tag {
                let cell = &self.cells[base + slot];
                if cell.0 == id {
                    return cell.1;
                }
            }
            slot = (slot + 1) & mask;
        }
    }

    #[inline]
    pub fn prefetch(&self, id: Key) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            let (base, _, slot, _) = self.start(id);
            _mm_prefetch(self.tags.as_ptr().add(base + slot).cast::<i8>(), _MM_HINT_T0);
            _mm_prefetch(self.cells.as_ptr().add(base + slot).cast::<i8>(), _MM_HINT_T0);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::key_of;

    #[test]
    fn maps_linked_ids_and_passes_others_through() {
        let ids: Vec<Key> = (0..5000u32).map(|n| key_of(format!("u{n}").as_bytes(), None)).collect();
        let table = People::new(ids.iter().map(|&id| (id, ids[0])));
        for &id in &ids {
            table.prefetch(id);
            assert_eq!(table.get(id), ids[0]);
        }
        let stranger = key_of(b"nobody", None);
        assert_eq!(table.get(stranger), stranger);
    }

    #[test]
    fn parallel_build_equals_serial_build() {
        let ids: Vec<Key> = (0..20000u32).map(|n| key_of(format!("k{n}x").as_bytes(), None)).collect();
        let pairs: Vec<(Key, Key)> = ids.iter().enumerate().map(|(i, &id)| (id, ids[i / 7 * 7])).collect();
        let serial = People::new(pairs.iter().copied());
        let pieces: Vec<Vec<(Key, Key)>> = pairs.chunks(997).map(<[_]>::to_vec).collect();
        for threads in [1, 3, 8] {
            let parallel = People::build(&pieces, threads);
            for &id in &ids {
                assert_eq!(parallel.get(id), serial.get(id));
            }
        }
    }
}
