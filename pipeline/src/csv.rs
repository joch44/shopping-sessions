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
const PREFETCH_AHEAD: usize = 1024;

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
        #[cfg(all(target_arch = "x86_64", any(target_feature = "avx512bw", target_feature = "avx2"), target_feature = "bmi2"))]
        {
            let remaining = data.len() - pos;
            // Software prefetch ~PREFETCH_AHEAD bytes ahead (a hint: never faults, may be past the end).
            // SAFETY: _mm_prefetch never dereferences; wrapping_add keeps the pointer arithmetic defined.
            unsafe {
                use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
                _mm_prefetch::<_MM_HINT_T0>(data.as_ptr().wrapping_add(pos + PREFETCH_AHEAD).cast());
            }
            // SAFETY: masks128 reads only bytes in pos..data.len() (see its two versions).
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
            // A line of 128..256 bytes (36-byte UUID IDs give ~136-byte event lines): masks
            // for the next 128 bytes, then the same split rules over 256-bit masks.
            // SAFETY: as above, masks128 reads only bytes in pos + 128..data.len().
            let (newlines2, commas2, quotes2) = unsafe { masks128(data.as_ptr().add(pos + 128), remaining - 128) };
            if newlines2 != 0 {
                let n = 128 + newlines2.trailing_zeros() as usize;
                let line_start = pos;
                pos += n + 1;
                if split_wide(&data[line_start..line_start + n], [commas, commas2], [quotes, quotes2], &mut fields) {
                    f(&fields, line_start);
                }
                continue;
            }
            // A line of 256+ bytes: fall through to the scalar path.
        }
        let line_end = memchr::memchr(b'\n', &data[pos..]).map_or(data.len(), |i| pos + i);
        if parse_line(&data[pos..line_end], &mut fields) {
            f(&fields, pos);
        }
        pos = line_end + 1;
    }
}


/// AVX2 version (builds without AVX-512): four 32-byte compares. AVX2 has no byte-masked
/// loads, so a line within 128 bytes of the end of `data` is copied into a zeroed buffer
/// first (zero bytes match none of `\n` `,` `"`); everything else is loaded in place.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(target_feature = "avx512bw")))]
#[inline(always)]
unsafe fn masks128(p: *const u8, remaining: usize) -> (u128, u128, u128) {
    use std::arch::x86_64::*;
    let mut tail = [0u8; 128];
    let base = if remaining >= 128 {
        p
    } else {
        // SAFETY: p..p+remaining is in bounds; tail holds 128 bytes.
        unsafe { std::ptr::copy_nonoverlapping(p, tail.as_mut_ptr(), remaining) };
        tail.as_ptr()
    };
    unsafe {
        let blocks = [0, 32, 64, 96].map(|at| _mm256_loadu_si256(base.add(at).cast()));
        let eq = |byte: u8| {
            let x = _mm256_set1_epi8(byte as i8);
            blocks.iter().enumerate().fold(0u128, |mask, (i, block)| {
                mask | u128::from(_mm256_movemask_epi8(_mm256_cmpeq_epi8(*block, x)) as u32) << (32 * i)
            })
        };
        (eq(b'\n'), eq(b','), eq(b'"'))
    }
}

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
#[cfg(all(target_arch = "x86_64", any(target_feature = "avx512bw", target_feature = "avx2"), target_feature = "bmi2"))]
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

/// Fields of one line of 128..256 bytes (no `\n`) from its comma and quote masks, as two
/// 128-bit words each; the same accept/reject and values as `parse_line`. A field is quoted
/// when it has 2+ bytes with a quote at both ends; the line is well-formed when the commas
/// number N - 1 and every quote in it is one of those (popcount check), which is exactly
/// parse_line's rule (a quote elsewhere, a lone `"` field, or a quote at one end only all
/// leave an uncounted quote).
#[cfg(all(target_arch = "x86_64", any(target_feature = "avx512bw", target_feature = "avx2"), target_feature = "bmi2"))]
#[inline(always)]
fn split_wide<'a, const N: usize>(line: &'a [u8], commas: [u128; 2], quotes: [u128; 2], out: &mut [Option<&'a [u8]>; N]) -> bool {
    let n = line.len();
    debug_assert!((128..256).contains(&n));
    let in_hi = (1u128 << (n - 128)) - 1;
    let mut c = [commas[0], commas[1] & in_hi];
    let q = [quotes[0], quotes[1] & in_hi];
    if (c[0].count_ones() + c[1].count_ones()) as usize != N - 1 {
        return false;
    }
    let quote_at = |at: usize| (q[at >> 7] >> (at & 127)) & 1 == 1;
    let mut counted = 0u32;
    let mut field_start = 0usize;
    for (i, slot) in out.iter_mut().enumerate() {
        let field_end = if i + 1 < N {
            let w = usize::from(c[0] == 0);
            let at = (w << 7) + c[w].trailing_zeros() as usize;
            c[w] &= c[w] - 1;
            at
        } else {
            n
        };
        let quoted = field_end - field_start >= 2 && quote_at(field_start) && quote_at(field_end - 1);
        let (a, b) = if quoted { (field_start + 1, field_end - 1) } else { (field_start, field_end) };
        counted += 2 * u32::from(quoted);
        // SAFETY: field_start <= a <= b <= field_end <= n.
        *slot = if a == b { None } else { Some(unsafe { line.get_unchecked(a..b) }) };
        field_start = field_end + 1;
    }
    counted == q[0].count_ones() + q[1].count_ones()
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

#[cfg(all(test, target_arch = "x86_64", any(target_feature = "avx512bw", target_feature = "avx2"), target_feature = "bmi2"))]
mod split_wide_tests {
    use super::*;

    fn masks(line: &[u8]) -> ([u128; 2], [u128; 2]) {
        let (mut c, mut q) = ([0u128; 2], [0u128; 2]);
        for (i, &b) in line.iter().enumerate() {
            if b == b',' {
                c[i >> 7] |= 1 << (i & 127);
            }
            if b == b'"' {
                q[i >> 7] |= 1 << (i & 127);
            }
        }
        (c, q)
    }

    #[test]
    fn split_wide_matches_parse_line() {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let alphabet = b"ab0-_ \"\",,,\r\xc3\xbc";
        let (mut accepted, mut total) = (0, 0);
        for _ in 0..2_000_000 {
            let n = 128 + (next() % 128) as usize;
            // Mostly well-formed 7-field lines with random quoting, plus random noise.
            let mut line = Vec::with_capacity(n);
            if next() % 4 != 0 {
                let mut fields: Vec<Vec<u8>> = (0..7).map(|_| (0..(next() % 40) as usize).map(|_| b"abcdef0123-_ \xc3"[(next() % 14) as usize]).collect()).collect();
                for f in fields.iter_mut() {
                    if next() % 3 == 0 {
                        f.insert(0, b'"');
                        f.push(b'"');
                    }
                    if next() % 50 == 0 {
                        let at = (next() as usize) % (f.len() + 1);
                        f.insert(at, b'"');
                    }
                }
                line = fields.join(&b',');
                while line.len() < 128 {
                    line.insert(0, b'x');
                }
                line.truncate(255);
            } else {
                for _ in 0..n {
                    line.push(alphabet[(next() % alphabet.len() as u64) as usize]);
                }
            }
            if line.len() < 128 {
                continue;
            }
            let (c, q) = masks(&line);
            let mut a: [Option<&[u8]>; 7] = [None; 7];
            let mut b: [Option<&[u8]>; 7] = [None; 7];
            let ra = split_wide(&line, c, q, &mut a);
            let rb = parse_line(&line, &mut b);
            total += 1;
            assert_eq!(ra, rb, "accept differs on {:?}", String::from_utf8_lossy(&line));
            if ra {
                accepted += 1;
                assert_eq!(a, b, "fields differ on {:?}", String::from_utf8_lossy(&line));
            }
        }
        eprintln!("split_wide: {accepted} of {total} lines accepted, all equal to parse_line");
    }
}
