//! user ID key -> person key, for the IDs that appear in links (rules.md "Linked IDs").
//! An ID not in the table is a person on its own, so `get` returns the ID itself.
//!
//! Flat open addressing with linear probing: keys and values in two arrays sized to a
//! power of two at most half full. Key 0 marks an empty slot: no ID packs to 0 (a short
//! key's lowest byte is its length, 1..=15, and a long key's is 0xFF). `prefetch` lets
//! a caller issue the cache miss for a lookup some iterations before it needs the answer.

use crate::key::{Key, mix};

pub struct People {
    keys: Vec<Key>,
    values: Vec<Key>,
    mask: usize,
}

impl People {
    pub fn new(pairs: impl ExactSizeIterator<Item = (Key, Key)>) -> Self {
        let slots = (pairs.len() * 2).next_power_of_two().max(16);
        let mut table = Self { keys: vec![0; slots], values: vec![0; slots], mask: slots - 1 };
        for (id, person) in pairs {
            let mut slot = table.slot(id);
            while table.keys[slot] != 0 && table.keys[slot] != id {
                slot = (slot + 1) & table.mask;
            }
            table.keys[slot] = id;
            table.values[slot] = person;
        }
        table
    }

    #[inline]
    fn slot(&self, id: Key) -> usize {
        (mix(id) >> 20) as usize & self.mask
    }

    #[inline]
    pub fn get(&self, id: Key) -> Key {
        let mut slot = self.slot(id);
        loop {
            let held = self.keys[slot];
            if held == id {
                return self.values[slot];
            }
            if held == 0 {
                return id;
            }
            slot = (slot + 1) & self.mask;
        }
    }

    #[inline]
    pub fn prefetch(&self, id: Key) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            let slot = self.slot(id);
            _mm_prefetch(self.keys.as_ptr().add(slot).cast::<i8>(), _MM_HINT_T0);
            _mm_prefetch(self.values.as_ptr().add(slot).cast::<i8>(), _MM_HINT_T0);
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
