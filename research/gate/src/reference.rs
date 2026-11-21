//! FROZEN v1 reference (pipeline at commit cccd498): the scalar CSV dialect, timestamps
//! and ID keys, straight from rules.md. Never edit this to make the gate pass: it is the
//! oracle every fast path in the live pipeline must equal.

pub mod csv {
    /// Splits `line` (without its `\n`) into exactly `N` fields; false when malformed.
    pub fn parse_line<'a, const N: usize>(line: &'a [u8], out: &mut [Option<&'a [u8]>; N]) -> bool {
        if line.is_empty() {
            return false;
        }
        let mut count = 0;
        for field in line.split(|&byte| byte == b',') {
            if count == N {
                return false;
            }
            let value = if field.contains(&b'"') {
                let n = field.len();
                if n < 2 || field[0] != b'"' || field[n - 1] != b'"' || field[1..n - 1].contains(&b'"') {
                    return false;
                }
                &field[1..n - 1]
            } else {
                field
            };
            out[count] = if value.is_empty() { None } else { Some(value) };
            count += 1;
        }
        count == N
    }

    /// v1 `for_each_line` + `parse_line`: every well-formed line STARTING in start..end.
    pub fn for_each_record<'a, const N: usize>(data: &'a [u8], start: usize, end: usize, mut f: impl FnMut(&[Option<&'a [u8]>; N], usize)) {
        let first = if start == 0 { memchr::memchr(b'\n', data) } else { memchr::memchr(b'\n', &data[start - 1..]).map(|i| i + start - 1) };
        let Some(newline) = first else { return };
        let mut pos = newline + 1;
        let mut fields = [None; N];
        while pos < end && pos < data.len() {
            let line_end = memchr::memchr(b'\n', &data[pos..]).map_or(data.len(), |i| pos + i);
            if parse_line(&data[pos..line_end], &mut fields) {
                f(&fields, pos);
            }
            pos = line_end + 1;
        }
    }

    pub fn ascii_digits(value: &[u8]) -> bool {
        !value.is_empty() && value.iter().all(u8::is_ascii_digit)
    }
}

pub mod ts {
    use super::csv::ascii_digits;

    fn two(bytes: &[u8]) -> i64 {
        i64::from(bytes[0] - b'0') * 10 + i64::from(bytes[1] - b'0')
    }

    fn is_leap(year: i64) -> bool {
        year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
    }

    pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
        let year = if month <= 2 { year - 1 } else { year };
        let era = year.div_euclid(400);
        let year_of_era = year - era * 400;
        let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
        let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
        era * 146_097 + day_of_era - 719_468
    }

    /// Epoch ms, or None for a bad timestamp. F3 overflow saturates to i64::MAX.
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
        let local = (days_from_civil(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second) * 1_000;
        Some(local - offset_ms)
    }
}

pub mod key {
    /// v1 key: 1..=15 bytes big-endian in the top bytes + length in the low byte;
    /// longer IDs: 120-bit xxh3 with 0xFF in the low byte.
    pub fn key_of(id: &[u8]) -> u128 {
        if id.len() <= 15 {
            let mut buffer = [0u8; 16];
            buffer[..id.len()].copy_from_slice(id);
            buffer[15] = id.len() as u8;
            u128::from_be_bytes(buffer)
        } else {
            (xxhash_rust::xxh3::xxh3_128(id) << 8) | 0xFF
        }
    }
}
