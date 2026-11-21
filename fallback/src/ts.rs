//! Timestamps, rules.md "Timestamps": F1 `YYYY-MM-DDTHH:MM:SSZ`,
//! F2 `YYYY-MM-DDTHH:MM:SS+HH:MM` (offset subtracted), F3 digits (epoch ms,
//! leading zeros allowed). Anything else is bad.
//!
//! An F3 value too large for i64 is a valid number far outside any window; it
//! saturates to i64::MAX, which every window check rejects and every ingest
//! comparison treats as "later than any real time".

use crate::csv::ascii_digits;

pub const DAY_MS: i64 = 86_400_000;

fn is_leap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// Days since 1970-01-01 of a proleptic Gregorian date (Hinnant).
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Days since the epoch of each month's first day, in 16 direct-mapped slots keyed by
/// year*12+month: a window spans a few months, but rows are shuffled across its days,
/// so caching by month (not by date) hits nearly always.
pub struct DateCache {
    months: [i64; 16],
    first_days: [i64; 16],
}

impl Default for DateCache {
    fn default() -> Self {
        Self { months: [i64::MIN; 16], first_days: [0; 16] }
    }
}

impl DateCache {
    #[inline(always)]
    fn days(&mut self, year: i64, month: i64, day: i64) -> i64 {
        let key = year * 12 + month - 1;
        let slot = (key & 15) as usize;
        if self.months[slot] != key {
            self.months[slot] = key;
            self.first_days[slot] = days_from_civil(year, month, 1);
        }
        self.first_days[slot] + day - 1
    }
}

/// Epoch ms, or None for a bad timestamp.
pub fn parse(value: &[u8]) -> Option<i64> {
    parse_cached(value, &mut DateCache::default())
}

/// `parse` with a caller-owned date cache. With AVX-512BW/VL, the hot shapes (F1, F2, F3 of
/// 1..=16 digits) take a SIMD path with no length dispatch on the date-time fields; every
/// other input, and any input that fails a fast-path check, goes to `parse_exact`, so the
/// result is identical for every input.
#[inline]
pub fn parse_cached(value: &[u8], dates: &mut DateCache) -> Option<i64> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "avx512vl"))]
    return parse_simd(value, dates);
    #[allow(unreachable_code)]
    parse_exact(value, dates)
}

/// The exact parser (v11 body).
#[inline]
fn parse_exact(value: &[u8], dates: &mut DateCache) -> Option<i64> {
    let n = value.len();
    if n == 0 {
        return None;
    }
    // F3 first, so an all-digit value of F1's length (20) is still a number.
    if n <= 18 {
        // At most 18 digits cannot overflow i64: one branch-free pass.
        let mut result: i64 = 0;
        let mut digits = true;
        for &byte in value {
            let d = byte.wrapping_sub(b'0');
            digits &= d < 10;
            // Wrapping: a non-digit makes `result` garbage, discarded below (debug builds trap).
            result = result.wrapping_mul(10).wrapping_add(i64::from(d));
        }
        // Not all digits and shorter than F1/F2: not a timestamp.
        return digits.then_some(result);
    }
    if ascii_digits(value) {
        let mut result: i64 = 0;
        for &byte in value {
            result = match result.checked_mul(10).and_then(|r| r.checked_add(i64::from(byte - b'0'))) {
                Some(r) => r,
                None => return Some(i64::MAX), // a valid number, far outside any window
            };
        }
        return Some(result);
    }
    let digit = |i: usize| value[i].wrapping_sub(b'0');
    let offset_ms = match n {
        20 => {
            if value[19] != b'Z' {
                return None;
            }
            0
        }
        25 => {
            let sign = value[19];
            if (sign != b'+' && sign != b'-') || value[22] != b':' {
                return None;
            }
            let (h1, h2, m1, m2) = (digit(20), digit(21), digit(23), digit(24));
            if h1 >= 10 || h2 >= 10 || m1 >= 10 || m2 >= 10 {
                return None;
            }
            let (hours, minutes) = (i64::from(h1 * 10 + h2), i64::from(m1 * 10 + m2));
            if hours > 23 || minutes > 59 {
                return None;
            }
            let offset = (hours * 60 + minutes) * 60_000;
            if sign == b'-' { -offset } else { offset }
        }
        _ => return None,
    };
    if value[4] != b'-' || value[7] != b'-' || value[10] != b'T' || value[13] != b':' || value[16] != b':' {
        return None;
    }
    // All 14 date/time digits checked at once, branch-free.
    let all_digits = (digit(0) < 10) & (digit(1) < 10) & (digit(2) < 10) & (digit(3) < 10)
        & (digit(5) < 10) & (digit(6) < 10) & (digit(8) < 10) & (digit(9) < 10)
        & (digit(11) < 10) & (digit(12) < 10) & (digit(14) < 10) & (digit(15) < 10)
        & (digit(17) < 10) & (digit(18) < 10);
    if !all_digits {
        return None;
    }
    let two = |i: usize| i64::from(digit(i) * 10 + digit(i + 1));
    let year = two(0) * 100 + two(2);
    let (month, day) = (two(5), two(8));
    let (hour, minute, second) = (two(11), two(14), two(17));
    if !(1..=12).contains(&month) {
        return None;
    }
    const DAYS: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let month_days = if month == 2 && is_leap(year) { 29 } else { DAYS[(month - 1) as usize] };
    if day < 1 || day > month_days || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    let local = (dates.days(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second) * 1_000;
    Some(local - offset_ms)
}

#[inline(always)]
#[cfg(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "avx512vl"))]
fn parse_simd(v: &[u8], dates: &mut DateCache) -> Option<i64> {
    use std::arch::x86_64::*;
    let n = v.len();
    if n == 0 || n > 25 || (n > 16 && n != 20 && n != 25) {
        return parse_exact(v, dates);
    }
    unsafe {
        let k: __mmask32 = ((1u64 << n) - 1) as u32;
        let x = _mm256_maskz_loadu_epi8(k, v.as_ptr().cast());
        let d = _mm256_sub_epi8(x, _mm256_set1_epi8(b'0' as i8));
        let digits = _mm256_mask_cmplt_epu8_mask(k, d, _mm256_set1_epi8(10));
        if n <= 16 {
            // F3: all digits (else bad: shorter than F1, cannot be a date-time).
            if digits != k {
                return None;
            }
            // right-align the n digits in 16 lanes, then 10/100/10^4 multiply-adds
            let r = _mm_maskz_loadu_epi8((((1u32 << n) - 1) << (16 - n)) as u16, v.as_ptr().wrapping_sub(16 - n).cast());
            let r = _mm_maskz_sub_epi8((((1u32 << n) - 1) << (16 - n)) as u16, r, _mm_set1_epi8(b'0' as i8));
            let p2 = _mm_maddubs_epi16(r, _mm_set_epi8(1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10));
            let p4 = _mm_madd_epi16(p2, _mm_set_epi16(1, 100, 1, 100, 1, 100, 1, 100));
            let p4 = _mm_packus_epi32(p4, p4);
            let p8 = _mm_madd_epi16(p4, _mm_set_epi16(1, 10000, 1, 10000, 1, 10000, 1, 10000));
            let hi = _mm_cvtsi128_si32(p8) as u32 as i64;
            let lo = _mm_extract_epi32::<1>(p8) as u32 as i64;
            return Some(hi * 100_000_000 + lo);
        }
        if digits == k {
            return parse_exact(v, dates); // 20 or 25 digits: F3 with leading zeros (exact path)
        }
        // F1 / F2: separators and digit positions by mask.
        const DIG19: u32 = 0x6DB6F; // digit positions 0-3,5,6,8,9,11,12,14,15,17,18
        let pat = _mm256_setr_epi8(
            0, 0, 0, 0, b'-' as i8, 0, 0, b'-' as i8, 0, 0, b'T' as i8, 0, 0, b':' as i8, 0, 0, b':' as i8, 0, 0, b'Z' as i8, 0, 0, b':' as i8, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        );
        let eq = _mm256_cmpeq_epi8_mask(x, pat);
        const SEP19: u32 = 1 << 4 | 1 << 7 | 1 << 10 | 1 << 13 | 1 << 16;
        let mut ok = (digits & DIG19 == DIG19) & (eq & SEP19 == SEP19);
        let mut off = 0i64;
        if n == 20 {
            ok &= eq & (1 << 19) != 0;
        } else {
            let sign = v[19];
            ok &= (sign == b'+' || sign == b'-') & (eq & (1 << 22) != 0) & (digits & 0b1_1011 << 20 == 0b1_1011 << 20);
            if ok {
                let (oh, om) = (i64::from(v[20] - b'0') * 10 + i64::from(v[21] - b'0'), i64::from(v[23] - b'0') * 10 + i64::from(v[24] - b'0'));
                ok &= oh <= 23 && om <= 59;
                off = (oh * 60 + om) * 60_000;
                if sign == b'-' {
                    off = -off;
                }
            }
        }
        if !ok {
            return None;
        }
        // pairs at 0,2 (century, year), 5 (month), 8 (day), 11, 14, 17: gather the 14 digit
        // bytes into 7 adjacent pairs with one in-lane shuffle of the low 16 + high 3 bytes.
        let d128 = _mm256_castsi256_si128(d);
        let dhi = _mm256_extracti128_si256::<1>(d);
        // low 16 bytes hold positions 0..15: pairs 0-1,2-3,5-6,8-9,11-12,14-15
        let g = _mm_shuffle_epi8(d128, _mm_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1));
        // positions 17,18 are bytes 1,2 of the high lane
        let g = _mm_or_si128(g, _mm_bslli_si128::<12>(_mm_shuffle_epi8(dhi, _mm_setr_epi8(1, 2, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1))));
        let p = _mm_maddubs_epi16(g, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0));
        let mut f = [0u16; 8];
        _mm_storeu_si128(f.as_mut_ptr().cast(), p);
        let year = i64::from(f[0]) * 100 + i64::from(f[1]);
        let (month, day, hour, minute, second) = (i64::from(f[2]), i64::from(f[3]), i64::from(f[4]), i64::from(f[5]), i64::from(f[6]));
        if !(1..=12).contains(&month) {
            return None;
        }
        const DAYS: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let md = if month == 2 && leap { 29 } else { DAYS[(month - 1) as usize] };
        if day < 1 || day > md || hour > 23 || minute > 59 || second > 59 {
            return None;
        }
        // same DateCache arithmetic as cts (via its public parse on a canonical F1 is too slow):
        let local = (dates.days(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second) * 1_000;
        Some(local - off)
    }
}

/// `ingest_ts`, which rules.md guarantees is always valid F1 ("`ingest_ts` is always valid
/// F1"): a 20-byte value ending in `Z` is read at fixed offsets with no validation, its date
/// through the month cache. Anything else (an empty or odd field in hand-made input, an
/// all-digit value of F1's length) takes the exact parser.
#[inline]
pub fn parse_ingest(value: &[u8], dates: &mut DateCache) -> Option<i64> {
    if value.len() != 20 || value[19] != b'Z' {
        return parse_cached(value, dates);
    }
    let d = |i: usize| i64::from(value[i].wrapping_sub(b'0'));
    let year = d(0) * 1000 + d(1) * 100 + d(2) * 10 + d(3);
    let (month, day) = (d(5) * 10 + d(6), d(8) * 10 + d(9));
    let seconds = (d(11) * 10 + d(12)) * 3_600 + (d(14) * 10 + d(15)) * 60 + d(17) * 10 + d(18);
    Some((dates.days(year, month, day) * 86_400 + seconds) * 1_000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_fast_path_equals_exact_parser_on_valid_f1_and_falls_back_otherwise() {
        let mut a = DateCache::default();
        let mut b = DateCache::default();
        let mut x = 0x2545_F491_4F6C_DD1Du64;
        for _ in 0..200_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let year = 1970 + (x % 130) as i64;
            let month = 1 + ((x >> 8) % 12) as i64;
            let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
            let mdays = [31, if leap { 29 } else { 28 }, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][(month - 1) as usize];
            let day = 1 + ((x >> 16) % mdays) as i64;
            let (h, mi, s) = ((x >> 24) % 24, (x >> 32) % 60, (x >> 40) % 60);
            let v = format!("{year:04}-{month:02}-{day:02}T{h:02}:{mi:02}:{s:02}Z");
            assert_eq!(parse_ingest(v.as_bytes(), &mut a), parse_cached(v.as_bytes(), &mut b), "{v}");
        }
        for v in ["", "ingest_ts", "00000001791071890000", "18446744073709551616", "1791071890000", "2026-10-03T19:58:10-04:00", "2026-10-03T23:58:10z"] {
            assert_eq!(parse_ingest(v.as_bytes(), &mut a), parse_cached(v.as_bytes(), &mut b), "{v}");
        }
    }


    fn two(bytes: &[u8]) -> i64 {
        i64::from(bytes[0] - b'0') * 10 + i64::from(bytes[1] - b'0')
    }

    /// `parse` with a caller-owned date cache.
    fn parse_cached_v9(value: &[u8]) -> Option<i64> {
        if ascii_digits(value) {
            let mut result: i64 = 0;
            for &byte in value {
                result = match result.checked_mul(10).and_then(|r| r.checked_add(i64::from(byte - b'0'))) {
                    Some(r) => r,
                    None => return Some(i64::MAX),
                };
            }
            return Some(result);
        }
        let offset_ms = match value.len() {
            20 => {
                if value[19] != b'Z' {
                    return None;
                }
                0
            }
            25 => {
                let sign = value[19];
                if (sign != b'+' && sign != b'-') || value[22] != b':' {
                    return None;
                }
                if !ascii_digits(&value[20..22]) || !ascii_digits(&value[23..25]) {
                    return None;
                }
                let (hours, minutes) = (two(&value[20..22]), two(&value[23..25]));
                if hours > 23 || minutes > 59 {
                    return None;
                }
                let offset = (hours * 60 + minutes) * 60_000;
                if sign == b'-' { -offset } else { offset }
            }
            _ => return None,
        };
        if value[4] != b'-' || value[7] != b'-' || value[10] != b'T' || value[13] != b':' || value[16] != b':' {
            return None;
        }
        for range in [0..4, 5..7, 8..10, 11..13, 14..16, 17..19] {
            if !ascii_digits(&value[range]) {
                return None;
            }
        }
        let year = two(&value[0..2]) * 100 + two(&value[2..4]);
        let (month, day) = (two(&value[5..7]), two(&value[8..10]));
        let (hour, minute, second) = (two(&value[11..13]), two(&value[14..16]), two(&value[17..19]));
        if !(1..=12).contains(&month) {
            return None;
        }
        const DAYS: [i64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        let month_days = if month == 2 && is_leap(year) { 29 } else { DAYS[(month - 1) as usize] };
        if day < 1 || day > month_days || hour > 23 || minute > 59 || second > 59 {
            return None;
        }
        let local = (days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second) * 1_000;
        Some(local - offset_ms)
    }

    #[test]
    fn matches_previous_parser_on_random_inputs() {
        // A small xorshift so the test needs no dependency.
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || { state ^= state << 13; state ^= state >> 7; state ^= state << 17; state };
        let seeds: [&[u8]; 6] = [b"2026-10-03T23:58:10Z", b"2026-10-03T19:58:10-04:00", b"1791071890000",
                                 b"2028-02-29T00:00:00+23:59", b"00000001791071890000", b"9223372036854775807"];
        let alphabet = b"0123456789-+:TZtz .e";
        let mut dates = DateCache::default();
        for _ in 0..300_000 {
            let mut v = seeds[(next() % 6) as usize].to_vec();
            for _ in 0..(next() % 4) {
                let at = (next() as usize) % (v.len() + 1);
                match next() % 3 {
                    0 if at < v.len() => v[at] = alphabet[(next() as usize) % alphabet.len()],
                    1 if at < v.len() => { v.remove(at); }
                    _ => v.insert(at, alphabet[(next() as usize) % alphabet.len()]),
                }
            }
            assert_eq!(parse_cached(&v, &mut dates), parse_cached_v9(&v), "{}", String::from_utf8_lossy(&v));
        }
    }

    #[test]
    fn forms() {
        let f1 = parse(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(f1, 1_791_071_890_000);
        assert_eq!(parse(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse(b"1791071890000"), Some(f1));
        assert_eq!(parse(b"0001791071890000"), Some(f1));
        assert_eq!(parse(b"2026-10-03T23:58:10-00:00"), Some(f1));
        assert_eq!(parse(b"99999999999999999999999"), Some(i64::MAX));
        assert_eq!(parse(b"00000001791071890000"), Some(f1)); // 20 digits: F3, not F1
        assert_eq!(parse(b"999999999999999999"), Some(999_999_999_999_999_999)); // 18 digits
        assert_eq!(parse(b"9223372036854775807"), Some(i64::MAX)); // 19 digits, exact
        assert_eq!(parse(b"9223372036854775808"), Some(i64::MAX)); // saturates
        assert_eq!(parse(b"2026-10-03T19:58:10-0a:00"), None);
        assert_eq!(parse(b"2026-10-03T19:58:10-04:0a"), None);
        assert_eq!(parse(b"2026-1a-03T19:58:10Z"), None);
        assert_eq!(parse(b"17910718900x"), None);
        assert!(parse(b"2028-02-29T00:00:00Z").is_some());
        for bad in [
            &b""[..], b"2027-02-29T00:00:00Z", b"2026-10-03T23:58:10z", b"2026-10-03t23:58:10Z",
            b"2026-10-03 23:58:10Z", b"2026-10-03T23:58:10", b"2026-10-03T23:58:10.000Z", b"1.79e12",
            b"-5", b"2026-13-01T00:00:00Z", b"2026-02-30T00:00:00Z", b"2026-10-03T24:00:00Z",
            b"2026-10-03T23:59:60Z", b"2026-10-03T23:58:10+24:00", b"2026-10-03T23:58:10+05:60",
            b"2026-10-03T23:58:10+0200", b"1791071890000.0",
        ] {
            assert_eq!(parse(bad), None, "{}", String::from_utf8_lossy(bad));
        }
    }
}
