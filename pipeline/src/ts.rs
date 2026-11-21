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

/// The last date converted: a window holds few dates.
pub struct DateCache {
    packed: i64,
    days: i64,
}

impl Default for DateCache {
    fn default() -> Self {
        Self { packed: i64::MIN, days: 0 }
    }
}

impl DateCache {
    #[inline]
    fn days(&mut self, year: i64, month: i64, day: i64) -> i64 {
        let packed = (year << 9) | (month << 5) | day;
        if packed != self.packed {
            self.packed = packed;
            self.days = days_from_civil(year, month, day);
        }
        self.days
    }
}

/// Epoch ms, or None for a bad timestamp.
pub fn parse(value: &[u8]) -> Option<i64> {
    parse_cached(value, &mut DateCache::default())
}

/// `parse` with a caller-owned date cache.
#[inline]
pub fn parse_cached(value: &[u8], dates: &mut DateCache) -> Option<i64> {
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
            result = result * 10 + i64::from(d);
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
    const DIGITS: [usize; 14] = [0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18];
    if DIGITS.iter().any(|&i| digit(i) >= 10) {
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

#[cfg(test)]
mod tests {
    use super::*;

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
