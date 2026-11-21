//! The CSV dialect of rules.md "CSV dialect".
//!
//! Only `\n` ends a line (`\r` is an ordinary byte of its field). Every comma
//! separates fields. A field may be wrapped in double quotes, which are syntax;
//! a quote anywhere else makes the line malformed. An empty field is null.
//! Spaces are kept. An empty line is malformed. The first line of every file
//! (and only the first) is the header.

/// Splits `line` (without its `\n`) into exactly `N` fields. Returns false when
/// the line is malformed. Each field is `None` for null, else its unquoted bytes.
pub fn parse_line<'a, const N: usize>(line: &'a [u8], out: &mut [Option<&'a [u8]>; N]) -> bool {
    if line.is_empty() {
        return false;
    }
    // One pass: find the commas, and remember whether the current field held a quote.
    let mut count = 0;
    let mut start = 0;
    let mut quoted = false;
    let mut at = 0;
    while at <= line.len() {
        let byte = if at < line.len() { line[at] } else { b',' }; // the line end closes the last field
        if byte == b',' {
            if count == N {
                return false; // too many fields
            }
            let mut field = &line[start..at];
            if quoted {
                // Quotes are allowed only as the first and last byte of the whole field.
                let n = field.len();
                if n < 2 || field[0] != b'"' || field[n - 1] != b'"' || field[1..n - 1].contains(&b'"') {
                    return false;
                }
                field = &field[1..n - 1];
            }
            out[count] = if field.is_empty() { None } else { Some(field) };
            count += 1;
            start = at + 1;
            quoted = false;
        } else if byte == b'"' {
            quoted = true;
        }
        at += 1;
    }
    count == N
}

/// Calls `f(fields, offset)` for every well-formed data line that STARTS in `start..end`
/// (a line belongs to the chunk holding its first byte). The file's first line (the
/// header, starting at 0) is skipped; only `\n` ends a line; malformed lines are dropped.
///
/// With AVX-512BW, each line is split with three 128-byte compare masks (`\n`, `,`, `"`)
/// from masked loads, which never touch bytes past the end of `data`. A line of 128
/// bytes or more takes the scalar `parse_line`; both paths accept exactly the same lines.
#[inline]
pub fn for_each_record<'a, const N: usize>(
    data: &'a [u8],
    start: usize,
    end: usize,
    mut f: impl FnMut(&[Option<&'a [u8]>; N], usize),
) {
    let first = if start == 0 { memchr::memchr(b'\n', data) } else { memchr::memchr(b'\n', &data[start - 1..]).map(|i| i + start - 1) };
    let Some(newline) = first else { return };
    let mut pos = newline + 1;
    let mut fields: [Option<&'a [u8]>; N] = [None; N];
    while pos < end && pos < data.len() {
        #[cfg(all(target_arch = "x86_64", target_feature = "avx512bw"))]
        {
            let remaining = data.len() - pos;
            // SAFETY: masked loads read only bytes in pos..data.len(); avx512bw is enabled.
            let (newlines, commas, quotes) = unsafe { masks128(data.as_ptr().add(pos), remaining) };
            if newlines != 0 || remaining < 128 {
                let n = if newlines != 0 { newlines.trailing_zeros() as usize } else { remaining };
                let line_start = pos;
                pos += n + 1;
                if n > 0 && split_masked(&data[line_start..line_start + n], commas, quotes, &mut fields) {
                    f(&fields, line_start);
                }
                continue;
            }
            // A line of 128+ bytes: fall through to the scalar path.
        }
        let line_end = memchr::memchr(b'\n', &data[pos..]).map_or(data.len(), |i| pos + i);
        if parse_line(&data[pos..line_end], &mut fields) {
            f(&fields, pos);
        }
        pos = line_end + 1;
    }
}


/// `split_masked`'s rule, writing unquoted (start, end) offsets instead of slices.

#[cfg(all(target_arch = "x86_64", target_feature = "avx512bw"))]
#[inline(always)]
unsafe fn masks128(p: *const u8, remaining: usize) -> (u128, u128, u128) {
    use std::arch::x86_64::*;
    let lo_mask: u64 = if remaining >= 64 { u64::MAX } else { (1u64 << remaining) - 1 };
    let hi_mask: u64 = if remaining >= 128 { u64::MAX } else if remaining > 64 { (1u64 << (remaining - 64)) - 1 } else { 0 };
    unsafe {
        let lo = _mm512_maskz_loadu_epi8(lo_mask, p.cast());
        let hi = _mm512_maskz_loadu_epi8(hi_mask, p.add(64).cast());
        let eq = |byte: u8| {
            let x = _mm512_set1_epi8(byte as i8);
            u128::from(_mm512_mask_cmpeq_epi8_mask(lo_mask, lo, x)) | u128::from(_mm512_mask_cmpeq_epi8_mask(hi_mask, hi, x)) << 64
        };
        (eq(b'\n'), eq(b','), eq(b'"'))
    }
}

/// Fields of one line (n = line.len() < 128 bytes, no `\n`) from its comma and quote masks;
/// the same accept/reject and values as `parse_line`.
///
/// The quote rule is decided once per line in registers (no per-field branch). For field i
/// (start s_i, end e_i = its comma or n), the start markers FS = commas << 1 | 1 and the end
/// markers FE = commas | 1 << n hold exactly one bit per field, empty fields included.
/// Quotes at a start are SQ = quotes & FS; quotes at a field's last byte show up as
/// EQ = (quotes << 1) & FE. PEXT turns both into per-field bit vectors, which must be equal
/// (a field starts with a quote iff it ends with one). A 1-byte field `"` (its quote in both
/// SQ and EQ) is rejected outright, so no quote is counted twice; then every quote must be
/// one of those: popcount(quotes) == |SQ| + |EQ| rejects any other quote in the line.
#[cfg(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "bmi2"))]
#[inline(always)]
fn split_masked<'a, const N: usize>(line: &'a [u8], commas: u128, quotes: u128, out: &mut [Option<&'a [u8]>; N]) -> bool {
    use std::arch::x86_64::_pext_u64;
    #[inline(always)]
    fn pext128(x: u128, m: u128) -> u32 {
        // SAFETY: bmi2 is enabled (cfg above).
        unsafe {
            let lo = _pext_u64(x as u64, m as u64);
            let hi = _pext_u64((x >> 64) as u64, (m >> 64) as u64);
            (lo | hi << (m as u64).count_ones()) as u32
        }
    }
    let n = line.len();
    let in_line = (1u128 << n) - 1;
    let mut commas = commas & in_line;
    if commas.count_ones() as usize != N - 1 {
        return false;
    }
    let quotes = quotes & in_line;
    let fs = commas << 1 | 1;
    let fe = commas | 1u128 << n;
    let sq = quotes & fs;
    let eq = (quotes << 1) & fe;
    let start_q = pext128(sq, fs);
    let end_q = pext128(eq, fe);
    let ok = (start_q == end_q) & ((sq & (eq >> 1)) == 0) & (quotes.count_ones() == sq.count_ones() + eq.count_ones());
    let mut field_start = 0usize;
    for (i, slot) in out.iter_mut().enumerate() {
        let field_end = if i + 1 < N {
            let at = commas.trailing_zeros() as usize;
            commas &= commas - 1;
            at
        } else {
            n
        };
        let f = ((start_q >> i) & 1) as usize;
        let a = field_start + f;
        let b = (field_end - f).max(a);
        // SAFETY: field_start <= a <= b <= field_end <= n.
        *slot = if a == b { None } else { Some(unsafe { line.get_unchecked(a..b) }) };
        field_start = field_end + 1;
    }
    ok
}

/// Without BMI2 (never the case for cascadelake builds): v11's per-field quote check.
#[cfg(all(target_arch = "x86_64", target_feature = "avx512bw", not(target_feature = "bmi2")))]
#[inline(always)]
fn split_masked<'a, const N: usize>(line: &'a [u8], commas: u128, quotes: u128, out: &mut [Option<&'a [u8]>; N]) -> bool {
    let n = line.len();
    let in_line = (1u128 << n) - 1;
    let mut commas = commas & in_line;
    if commas.count_ones() as usize != N - 1 {
        return false;
    }
    let quotes = quotes & in_line;
    let mut field_start = 0usize;
    for (i, slot) in out.iter_mut().enumerate() {
        let field_end = if i + 1 < N {
            let at = commas.trailing_zeros() as usize;
            commas &= commas - 1;
            at
        } else {
            n
        };
        let (mut a, mut b) = (field_start, field_end);
        if quotes != 0 {
            let in_field = ((1u128 << field_end) - 1) & !((1u128 << field_start) - 1);
            let q = quotes & in_field;
            if q != 0 {
                if b - a < 2 || q != (1u128 << a | 1u128 << (b - 1)) {
                    return false;
                }
                a += 1;
                b -= 1;
            }
        }
        *slot = if a == b { None } else { Some(&line[a..b]) };
        field_start = field_end + 1;
    }
    true
}

/// Every data line of one file's bytes: the first line is skipped; the text
/// after the final `\n` (if any) is the last line.
pub fn data_lines(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    let body = match bytes.iter().position(|&byte| byte == b'\n') {
        Some(end) => &bytes[end + 1..],
        None => &bytes[bytes.len()..], // only a header, or nothing
    };
    let body = body.strip_suffix(b"\n").unwrap_or(body);
    // An empty body after stripping means no lines; `split` would yield one empty line.
    let has_lines = !body.is_empty() || bytes.ends_with(b"\n\n");
    body.split(|&byte| byte == b'\n').take(if has_lines { usize::MAX } else { 0 })
}

/// ASCII digits only, at least one.
pub fn ascii_digits(value: &[u8]) -> bool {
    !value.is_empty() && value.iter().all(u8::is_ascii_digit)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(bytes: &[u8]) -> Vec<&[u8]> {
        data_lines(bytes).collect()
    }

    #[test]
    fn splits_lines_like_the_rules() {
        assert_eq!(lines(b""), Vec::<&[u8]>::new());
        assert_eq!(lines(b"h"), Vec::<&[u8]>::new());
        assert_eq!(lines(b"h\n"), Vec::<&[u8]>::new());
        assert_eq!(lines(b"h\na"), vec![b"a" as &[u8]]);
        assert_eq!(lines(b"h\na\n"), vec![b"a" as &[u8]]);
        assert_eq!(lines(b"h\n\n"), vec![b"" as &[u8]]);
        assert_eq!(lines(b"h\na\n\nb\n"), vec![b"a" as &[u8], b"", b"b"]);
        assert_eq!(lines(b"h\na\r\n"), vec![b"a\r" as &[u8]]);
    }

    #[test]
    fn records_match_scalar_lines() {
        let lines: Vec<&[u8]> = vec![
            b"a,b,c", b"\"a\",,\"\"", b"a,b", b"a,b,c,d", b"a,b\"x\",c", b"a,\",c", b"a,\"\"\",c", b"",
            b"x\r,y ,\" z\"", b",,", b"\"\",\"\",\"\"", b"\"a,b,c", b"a,b,c\"",
        ];
        let long = [b'x'; 200];
        let mut long_line = long.to_vec();
        long_line.extend_from_slice(b",y,z");
        let mut all = lines.clone();
        all.push(&long_line);
        for last_newline in [true, false] {
            let mut file = b"header\n".to_vec();
            for line in &all {
                file.extend_from_slice(line);
                file.push(b'\n');
            }
            if !last_newline {
                file.pop();
            }
            let mut expected = Vec::new();
            let mut fields = [None; 3];
            for line in data_lines(&file) {
                if parse_line(line, &mut fields) {
                    expected.push(fields.map(|f| f.map(<[u8]>::to_vec)));
                }
            }
            let mut got = Vec::new();
            for_each_record::<3>(&file, 0, file.len(), |fields, _| got.push(fields.map(|f| f.map(<[u8]>::to_vec))));
            assert_eq!(got, expected, "last_newline={last_newline}");
        }
    }

    #[test]
    fn parses_fields() {
        let mut out = [None; 3];
        assert!(parse_line(b"a,\"b\",", &mut out));
        assert_eq!(out, [Some(b"a" as &[u8]), Some(b"b"), None]);
        assert!(parse_line(b"\"\",x ,\r", &mut out));
        assert_eq!(out, [None, Some(b"x " as &[u8]), Some(b"\r")]);
        assert!(!parse_line(b"a,b", &mut out));
        assert!(!parse_line(b"a,b,c,d", &mut out));
        assert!(!parse_line(b"a,b\"x\",c", &mut out));
        assert!(!parse_line(b"a,\",c", &mut out));
        assert!(!parse_line(b"a,\"\"\",c", &mut out));
        assert!(!parse_line(b"", &mut out));
    }
}
