//! user ID key -> person key, for the IDs that appear in links (rules.md "Linked IDs").
//! An ID not in the table is a person on its own, so `get` returns the ID itself.
//!
//! Open addressing with a 1-byte tag per slot (0 = empty, else 0x80 | 7 hash bits) and
//! interleaved 32-byte (id, person) cells. Many user_ids are in no link (a miss), and a
//! miss usually ends at its first tag byte without touching the cells. Layout after the
//! research session's measurement (0.40 -> 0.12 s for 64M lookups). `prefetch` issues
//! the cache misses for a lookup some iterations before it is needed.

use crate::key::{Key, mix};

pub struct People {
    tags: Vec<u8>,
    cells: Vec<(Key, Key)>,
    mask: usize,
}

impl People {
    pub fn new(pairs: impl ExactSizeIterator<Item = (Key, Key)>) -> Self {
        let slots = (pairs.len() * 2).next_power_of_two().max(16);
        let mut table = Self { tags: vec![0; slots], cells: vec![(0, 0); slots], mask: slots - 1 };
        for (id, person) in pairs {
            let (mut slot, tag) = table.start(id);
            while table.tags[slot] != 0 && table.cells[slot].0 != id {
                slot = (slot + 1) & table.mask;
            }
            table.tags[slot] = tag;
            table.cells[slot] = (id, person);
        }
        table
    }

    #[inline(always)]
    fn start(&self, id: Key) -> (usize, u8) {
        let h = mix(id);
        ((h >> 20) as usize & self.mask, 0x80 | (h >> 57) as u8)
    }

    #[inline]
    pub fn get(&self, id: Key) -> Key {
        let (mut slot, tag) = self.start(id);
        loop {
            let held = self.tags[slot];
            if held == 0 {
                return id;
            }
            if held == tag {
                let cell = &self.cells[slot];
                if cell.0 == id {
                    return cell.1;
                }
            }
            slot = (slot + 1) & self.mask;
        }
    }

    #[inline]
    pub fn prefetch(&self, id: Key) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            let (slot, _) = self.start(id);
            _mm_prefetch(self.tags.as_ptr().add(slot).cast::<i8>(), _MM_HINT_T0);
            _mm_prefetch(self.cells.as_ptr().add(slot).cast::<i8>(), _MM_HINT_T0);
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
}
