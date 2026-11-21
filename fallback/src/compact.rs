//! Compact (32-byte) event copies for the dedup stream.
//!
//! An event_id or user_id of 1..=10 bytes from [0-9A-Za-z] packs exactly into a u64 at 6
//! bits per character (`pack62`). Packability is a property of an ID's bytes, so every copy
//! of one event_id is either packed or not. Packed copies travel as 32-byte `EventCopy32`
//! (half the bytes of `EventCopy`); copies whose event_id does not pack keep the 64-byte
//! `EventCopy` and today's dedup, and the two streams never need to meet. A user_id that
//! does not pack is stored in a per-thread side table and referenced by a tagged index.
//!
//! Equal `ingest_ts` ties keep any one copy: rules.md (Events step 2) says copies with equal
//! `ingest_ts` are identical, so which one survives cannot change the output.

use crate::key::Key;

/// One packed copy: 32 bytes, two per cache line.
#[derive(Clone, Copy)]
#[repr(C, align(32))]
pub struct EventCopy32 {
    /// pack62(event_id)
    pub id: u64,
    /// pack62(user_id), or SLOW_USER | thread << 40 | index into that thread's side table.
    pub user: u64,
    /// exact epoch ms (NEVER when unparseable): no saturation, ties mean really equal
    pub ingest: i64,
    /// ms after window_start (only meaningful for valid copies)
    pub ts: u32,
    /// price << 8 | category, or INVALID32 for a copy that failed steps 3-4
    pub price_cat: u32,
}

pub const INVALID32: u32 = u32::MAX;
pub const SLOW_USER: u64 = 1 << 63;

/// Exact 6-bit packing: codes 1..=10 '0'..'9', 11..=36 'A'..'Z', 37..=62 'a'..'z',
/// character i at bits 54 - 6i, 0 = absent. None = take the exact slow path.
#[inline(always)]
pub fn pack62(id: &[u8]) -> Option<u64> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "avx512vl"))]
    {
        use std::arch::x86_64::*;
        let n = id.len();
        if n == 0 || n > 10 {
            return None;
        }
        // SAFETY: the masked load touches only id[..n]; avx512bw/vl are enabled.
        unsafe {
            let k: __mmask16 = (1u16 << n) - 1;
            let v = _mm_maskz_loadu_epi8(k, id.as_ptr().cast());
            let digit = _mm_mask_cmplt_epu8_mask(k, _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8)), _mm_set1_epi8(10));
            let upper = _mm_mask_cmplt_epu8_mask(k, _mm_sub_epi8(v, _mm_set1_epi8(b'A' as i8)), _mm_set1_epi8(26));
            let lower = _mm_mask_cmplt_epu8_mask(k, _mm_sub_epi8(v, _mm_set1_epi8(b'a' as i8)), _mm_set1_epi8(26));
            if digit | upper | lower != k {
                return None;
            }
            let c = _mm_maskz_sub_epi8(lower, v, _mm_set1_epi8(60));
            let c = _mm_mask_sub_epi8(c, upper, v, _mm_set1_epi8(54));
            let c = _mm_mask_sub_epi8(c, digit, v, _mm_set1_epi8(47));
            let w12 = _mm_maddubs_epi16(c, _mm_set1_epi16(0x0140));
            let w24 = _mm_madd_epi16(w12, _mm_set1_epi32(0x0001_1000));
            let l0 = _mm_cvtsi128_si32(w24) as u32 as u64;
            let l1 = _mm_extract_epi32::<1>(w24) as u32 as u64;
            let l2 = _mm_extract_epi32::<2>(w24) as u32 as u64;
            return Some(l0 << 36 | l1 << 12 | l2 >> 12);
        }
    }
    #[allow(unreachable_code)]
    pack62_scalar(id)
}

pub fn pack62_scalar(id: &[u8]) -> Option<u64> {
    if id.is_empty() || id.len() > 10 {
        return None;
    }
    let mut v = 0u64;
    for &b in id {
        let code = match b {
            b'0'..=b'9' => b - b'0' + 1,
            b'A'..=b'Z' => b - b'A' + 11,
            b'a'..=b'z' => b - b'a' + 37,
            _ => return None,
        };
        v = v << 6 | u64::from(code);
    }
    Some(v << (6 * (10 - id.len())))
}

/// 6-bit codes (one per byte, 0 = absent) to ASCII, eight bytes at a time.
#[inline(always)]
fn codes_to_ascii(x: u64) -> u64 {
    const H: u64 = 0x8080_8080_8080_8080;
    const ONE: u64 = 0x0101_0101_0101_0101;
    let nz = ((x | H).wrapping_sub(ONE) & H) >> 7; // code >= 1
    let ge11 = ((x | H).wrapping_sub(11 * ONE) & H) >> 7; // code >= 11
    let ge37 = ((x | H).wrapping_sub(37 * ONE) & H) >> 7; // code >= 37
    x + nz * 47 + ge11 * 7 + ge37 * 6
}

/// The `key::key_of` key of the ID behind a pack (so People and sessions are unchanged).
#[inline(always)]
pub fn unpack62_key(v: u64) -> Key {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        use std::arch::x86_64::_pdep_u64;
        // SAFETY: bmi2 is enabled.
        let (hi, lo) = unsafe { (_pdep_u64(v >> 12, 0x3F3F_3F3F_3F3F_3F3F), _pdep_u64(v & 0xFFF, 0x3F3F)) };
        let len = (((hi | 0x8080_8080_8080_8080).wrapping_sub(0x0101_0101_0101_0101) & 0x8080_8080_8080_8080).count_ones()
            + ((lo | 0x8080).wrapping_sub(0x0101) & 0x8080).count_ones()) as u128;
        return (codes_to_ascii(hi) as u128) << 64 | (codes_to_ascii(lo) as u128) << 48 | len;
    }
    #[allow(unreachable_code)]
    {
        let mut buffer = [0u8; 16];
        let mut n = 0;
        for i in 0..10 {
            let code = ((v >> (54 - 6 * i)) & 63) as u8;
            if code == 0 {
                break;
            }
            buffer[i] = match code {
                1..=10 => b'0' + code - 1,
                11..=36 => b'A' + code - 11,
                _ => b'a' + code - 37,
            };
            n += 1;
        }
        buffer[15] = n as u8;
        u128::from_be_bytes(buffer)
    }
}

/// Partition of a packed event_id (same role as `dedup_partition` for keys).
#[inline(always)]
pub fn mix64(id: u64) -> u64 {
    let mut h = id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^= h >> 32;
    h = h.wrapping_mul(0xD6E8_FEB8_6659_FD93);
    h ^ (h >> 32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::key_of;

    #[test]
    fn pack_unpack_matches_key_of() {
        let alnum = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        let mut r = 0x2545_F491_4F6C_DD1Du64;
        let mut next = || {
            r ^= r << 13;
            r ^= r >> 7;
            r ^= r << 17;
            r
        };
        for round in 0..300_000u32 {
            let n = (next() % 13) as usize;
            let mut id: Vec<u8> = (0..n).map(|_| alnum[(next() % 62) as usize]).collect();
            if round % 4 == 0 && n > 0 {
                let at = (next() % n as u64) as usize;
                id[at] = (next() % 256) as u8;
            }
            let (a, b) = (pack62(&id), pack62_scalar(&id));
            assert_eq!(a, b, "{id:?}");
            if let Some(p) = a {
                assert_eq!(unpack62_key(p), key_of(&id, None), "{id:?}");
                assert_eq!(p >> 60, 0);
            }
        }
    }
}
