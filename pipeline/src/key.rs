//! IDs as fixed-width keys.
//!
//! An ID of 1..=15 bytes packs into a u128: its bytes big-endian in the top 15
//! bytes, zero-padded, and its length in the lowest byte. Integer order then
//! equals byte-wise order (shorter-prefix-first included, `ab` < `ab\0`), and
//! equal bytes give equal keys, so hashing, grouping and sorting need no strings.
//!
//! A longer ID (the rules allow any length; the generator never writes one) is
//! its 120-bit xxh3 hash with 0xFF in the lowest byte. Its bytes are kept in a
//! side table so comparisons and output stay exact. Two different long IDs
//! colliding in 120 bits is the one accepted risk (about 2^-120 per pair).

use std::cmp::Ordering;
use std::sync::Mutex;

use rustc_hash::FxHashMap;

pub type Key = u128;
const LONG: u8 = 0xFF;

pub fn is_long(key: Key) -> bool {
    key as u8 == LONG
}

/// Long IDs whose bytes may be needed later (user, click and link IDs).
#[derive(Default)]
pub struct LongIds(Mutex<FxHashMap<Key, Box<[u8]>>>);

impl LongIds {
    pub fn bytes(&self, key: Key) -> Vec<u8> {
        if is_long(key) {
            self.0.lock().unwrap().get(&key).map(|b| b.to_vec()).unwrap_or_default()
        } else {
            short_bytes(key)
        }
    }
}

fn short_bytes(key: Key) -> Vec<u8> {
    let bytes = key.to_be_bytes();
    bytes[..bytes[15] as usize].to_vec()
}

/// The key of a non-empty ID. `remember` stores a long ID's bytes for later.
#[inline]
pub fn key_of(id: &[u8], longs: Option<&LongIds>) -> Key {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512bw"))]
    if id.len() <= 15 {
        // Same key as the scalar path: one fault-suppressing masked load of the ID's bytes,
        // the length into byte 15, then a byte reversal so byte 0 is most significant.
        use std::arch::x86_64::*;
        // SAFETY: the masked load reads only id[..len]; avx512bw/avx512vl are enabled.
        return unsafe {
            let v = _mm_maskz_loadu_epi8(((1u32 << id.len()) - 1) as u16, id.as_ptr().cast());
            let v = _mm_mask_set1_epi8(v, 1 << 15, id.len() as i8);
            let reversed = _mm_shuffle_epi8(v, _mm_set_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15));
            std::mem::transmute::<__m128i, u128>(reversed)
        };
    }
    if id.len() <= 15 {
        let mut buffer = [0u8; 16];
        buffer[..id.len()].copy_from_slice(id);
        buffer[15] = id.len() as u8;
        u128::from_be_bytes(buffer)
    } else {
        let key = (xxhash_rust::xxh3::xxh3_128(id) << 8) | Key::from(LONG);
        if let Some(longs) = longs {
            longs.0.lock().unwrap().entry(key).or_insert_with(|| id.into());
        }
        key
    }
}

/// Byte-wise order of the IDs behind two keys.
pub fn cmp(a: Key, b: Key, longs: &LongIds) -> Ordering {
    if !is_long(a) && !is_long(b) {
        a.cmp(&b)
    } else {
        longs.bytes(a).cmp(&longs.bytes(b))
    }
}

/// A well-mixed 64-bit hash of a key, for partitioning.
#[inline]
pub fn mix(key: Key) -> u64 {
    let folded = (key as u64) ^ ((key >> 64) as u64).rotate_left(29);
    let mut h = folded.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^= h >> 32;
    h = h.wrapping_mul(0xD6E8_FEB8_6659_FD93);
    h ^ (h >> 32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_keys_order_like_bytes() {
        let ids: [&[u8]; 9] = [b"A\tB", b"U1", b"ab", b"ab\0", b"abc", b"b", b" u5", b"u1\r", b"\xef\xbc\x92x"];
        let longs = LongIds::default();
        for a in ids {
            for b in ids {
                assert_eq!(cmp(key_of(a, None), key_of(b, None), &longs), a.cmp(b), "{a:?} {b:?}");
            }
            assert_eq!(longs.bytes(key_of(a, None)), a);
        }
    }

    #[test]
    fn simd_packing_equals_portable_packing() {
        let portable = |id: &[u8]| {
            let mut buffer = [0u8; 16];
            buffer[..id.len()].copy_from_slice(id);
            buffer[15] = id.len() as u8;
            u128::from_be_bytes(buffer)
        };
        let mut ids: Vec<Vec<u8>> = vec![b"a".to_vec(), b"\0".to_vec(), b"A\tB".to_vec(), "\u{ff12}x".as_bytes().to_vec()];
        for n in 1..=15u8 {
            ids.push((0..n).map(|i| b'a' + (i * 7 % 26)).collect());
            ids.push((0..n).map(|i| 0x80 | i).collect());
        }
        for id in ids {
            assert_eq!(key_of(&id, None), portable(&id), "{id:?}");
        }
    }

    #[test]
    fn long_keys_compare_by_bytes() {
        let longs = LongIds::default();
        let a = b"aaaaaaaaaaaaaaaaaaaa".as_slice();
        let b = b"aaaaaaaaaaaaaaab".as_slice();
        let c = b"ab".as_slice();
        let (ka, kb, kc) = (key_of(a, Some(&longs)), key_of(b, Some(&longs)), key_of(c, Some(&longs)));
        assert_eq!(cmp(ka, kb, &longs), a.cmp(b));
        assert_eq!(cmp(kb, kc, &longs), b.cmp(c));
        assert_eq!(cmp(kc, ka, &longs), c.cmp(a));
        assert_eq!(longs.bytes(ka), a);
        assert_eq!(key_of(a, None), ka);
    }
}
