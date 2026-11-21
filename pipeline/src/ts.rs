//! Timestamps, rules.md "Timestamps": F1 `YYYY-MM-DDTHH:MM:SSZ`,
//! F2 `YYYY-MM-DDTHH:MM:SS+HH:MM` (offset subtracted), F3 digits (epoch ms,
//! leading zeros allowed). Anything else is bad.
//!
//! An F3 value too large for i64 is a valid number far outside any window; it
//! saturates to i64::MAX, which every window check rejects and every ingest
//! comparison treats as "later than any real time".

use crate::csv::ascii_digits;

pub const DAY_MS: i64 = 86_400_000;

fn two(bytes: &[u8]) -> i64 {
    i64::from(bytes[0] - b'0') * 10 + i64::from(bytes[1] - b'0')
}

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

thread_local! {
    // A window holds few dates: remember the last one converted.
    static LAST_DATE: std::cell::Cell<(i64, i64)> = const { std::cell::Cell::new((i64::MIN, 0)) };
}

#[inline]
fn days_cached(year: i64, month: i64, day: i64) -> i64 {
    let packed = (year << 9) | (month << 5) | day;
    LAST_DATE.with(|last| {
        let (key, days) = last.get();
        if key == packed {
            days
        } else {
            let days = days_from_civil(year, month, day);
            last.set((packed, days));
            days
        }
    })
}

/// Epoch ms, or None for a bad timestamp.
pub fn parse(value: &[u8]) -> Option<i64> {
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
    let local = (days_cached(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second) * 1_000;
    Some(local - offset_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forms() {
        let f1 = parse(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(f1, 1_791_071_890_000);
        assert_eq!(parse(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse(b"1791071890000"), Some(f1));
        assert_eq!(parse(b"0001791071890000"), Some(f1));
        assert_eq!(parse(b"2026-10-03T23:58:10-00:00"), Some(f1));
        assert_eq!(parse(b"99999999999999999999999"), Some(i64::MAX));
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
