//! Our event kernel (stage 6 of the merge): decodes event CSV rows into the
//! `EvRow` contract defined by `iface` and `k_ref`.
//!
//! Design: one line at a time. Two 64-byte AVX-512 loads cover the line; byte
//! compares give 128-bit masks of newlines, commas and quotes, and the field
//! spans fall out of the comma mask. Every field has a fast decoder that only
//! accepts the canonical form; anything else is decoded by the reference
//! function for that field (`k_ref`), and any line that does not fit the fast
//! layout (too long, wrong field count, stray quotes, near the buffer end) is
//! handed to the reference line parser. Output is therefore identical to
//! `k_ref` by construction; `tests` checks it on generated and mutated data.
//!
//! Clicks still use the `k_free` kernel.

use crate::iface::*;
use crate::k_ref;
use core::arch::x86_64::*;

pub struct Ours;

impl Kernel for Ours {
    const NAME: &'static str = "ours";
    fn events<S: EventSink>(buf: &[u8], sink: &mut S) {
        unsafe { events(buf, sink) }
        sink.ev_flush();
    }
    fn clicks<S: ClickSink>(buf: &[u8], sink: &mut S) {
        <crate::k_free::Free as Kernel>::clicks(buf, sink)
    }
}

/// Bytes the fast path may read from a line start: two 64-byte vectors.
const SPAN: usize = 128;

#[inline(always)]
unsafe fn masks128(p: *const u8) -> (u128, u128, u128) {
    let a = _mm512_loadu_si512(p as *const _);
    let b = _mm512_loadu_si512(p.add(64) as *const _);
    let m = |v: __m512i, c: u8| _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(c as i8));
    let j = |lo: u64, hi: u64| lo as u128 | (hi as u128) << 64;
    (j(m(a, b'\n'), m(b, b'\n')), j(m(a, b','), m(b, b',')), j(m(a, b'"'), m(b, b'"')))
}

#[inline(always)]
unsafe fn events<S: EventSink>(buf: &[u8], sink: &mut S) {
    let n = buf.len();
    let p = buf.as_ptr();
    let mut s = 0usize;
    while s < n {
        if s + SPAN > n {
            slow_lines(buf, s, sink);
            return;
        }
        let (nl, cm, qt) = masks128(p.add(s));
        if nl == 0 {
            // Line longer than 128 bytes: the reference parser takes it.
            let e = s + buf[s..].iter().position(|&b| b == b'\n').unwrap_or(n - s);
            slow_line(buf, s, e, sink);
            s = e + 1;
            continue;
        }
        let len = nl.trailing_zeros() as usize;
        let below = (1u128 << len) - 1;
        let cm = cm & below;
        let qt = qt & below;
        if len == 0 {
            s += 1;
            continue;
        }
        if cm.count_ones() != 6 {
            slow_line(buf, s, s + len, sink);
            s += len + 1;
            continue;
        }
        // Field i spans [st[i], en[i]) relative to s.
        let mut st = [0usize; 7];
        let mut en = [0usize; 7];
        let mut c = cm;
        let mut i = 0;
        while c != 0 {
            let k = c.trailing_zeros() as usize;
            en[i] = k;
            st[i + 1] = k + 1;
            c &= c - 1;
            i += 1;
        }
        en[6] = len;
        if qt != 0 {
            // Each field is either unquoted or wrapped in exactly one pair of quotes.
            let mut ok = true;
            for f in 0..7 {
                let a = st[f];
                let b = en[f];
                let fm = if b > a { (qt >> a) & ((1u128 << (b - a)) - 1) } else { 0 };
                if fm == 0 {
                    continue;
                }
                let w = b - a;
                if w >= 2 && fm == (1 | 1u128 << (w - 1)) {
                    st[f] = a + 1;
                    en[f] = b - 1;
                } else {
                    ok = false;
                    break;
                }
            }
            if !ok {
                slow_line(buf, s, s + len, sink);
                s += len + 1;
                continue;
            }
        }
        if st[0] == en[0] {
            s += len + 1;
            continue;
        }
        let q = p.add(s);
        let fld = |f: usize| core::slice::from_raw_parts(q.add(st[f]), en[f] - st[f]);
        let kind = kind_of(fld(4));
        let row = EvRow {
            eid: id_fast(fld(0), b'e').unwrap_or_else(|| k_ref::parse_id(buf, (s + st[0], s + en[0]), b'e')),
            uid: id_fast(fld(1), b'u').unwrap_or_else(|| k_ref::parse_id(buf, (s + st[1], s + en[1]), b'u')),
            ts: ts_fast(fld(2)).unwrap_or_else(|| k_ref::parse_ts(fld(2))),
            ingest: iso_z(fld(3)).unwrap_or_else(|| k_ref::parse_f1(fld(3))),
            kind,
            product: prod_fast(fld(5)).unwrap_or_else(|| k_ref::parse_product(fld(5))),
            price: if kind == KIND_PURCHASE { price_fast(fld(6)).unwrap_or_else(|| k_ref::parse_price(fld(6))) } else { 0 },
        };
        sink.event(&row);
        s += len + 1;
    }
}

/// The reference parser for one line [s, e) of `buf`.
#[cold]
#[inline(never)]
fn slow_line<S: EventSink>(buf: &[u8], s: usize, e: usize, sink: &mut S) {
    let Some(v) = k_ref::split_line::<7>(buf, s, e) else { return };
    if v[0].0 == v[0].1 {
        return;
    }
    let val = |i: usize| &buf[v[i].0..v[i].1];
    let kind = k_ref::parse_kind(val(4));
    sink.event(&EvRow {
        eid: k_ref::parse_id(buf, v[0], b'e'),
        uid: k_ref::parse_id(buf, v[1], b'u'),
        ts: k_ref::parse_ts(val(2)),
        ingest: k_ref::parse_f1(val(3)),
        kind,
        product: k_ref::parse_product(val(5)),
        price: if kind == KIND_PURCHASE { k_ref::parse_price(val(6)) } else { 0 },
    });
}

/// The reference parser for all lines from `s` to the end of `buf`.
#[cold]
#[inline(never)]
fn slow_lines<S: EventSink>(buf: &[u8], mut s: usize, sink: &mut S) {
    while s < buf.len() {
        let e = s + buf[s..].iter().position(|&b| b == b'\n').unwrap_or(buf.len() - s);
        slow_line(buf, s, e, sink);
        s = e + 1;
    }
}

/// Canonical ID: `prefix` then 1..=8 base-36 digits (0-9, a-z), no leading zero.
#[inline(always)]
fn id_fast(v: &[u8], prefix: u8) -> Option<u64> {
    let n = v.len();
    if n < 2 || n > 9 || v[0] != prefix || (v[1] == b'0' && n > 2) {
        return None;
    }
    let mut x = 0u64;
    for &b in &v[1..] {
        let d = if b.wrapping_sub(b'0') < 10 {
            b - b'0'
        } else if b.wrapping_sub(b'a') < 26 {
            b - b'a' + 10
        } else {
            return None;
        };
        x = x * 36 + d as u64;
    }
    Some(x)
}

/// Days since 1970-01-01 for a valid civil date (Howard Hinnant's algorithm).
#[inline(always)]
fn days(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[inline(always)]
fn mdays(y: i64, m: i64) -> i64 {
    match m {
        2 => 28 + (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) as i64,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// `YYYY-MM-DDTHH:MM:SS` (19 bytes) to epoch milliseconds, validated with one
/// 16-byte compare for the layout and digit ranges.
#[inline(always)]
fn civil19(v: &[u8]) -> Option<i64> {
    debug_assert!(v.len() >= 19);
    unsafe {
        let x = _mm_loadu_si128(v.as_ptr() as *const __m128i);
        // Separators at 4, 7, 10, 13 must match; every other byte must be a digit.
        let sep = _mm_setr_epi8(0, 0, 0, 0, b'-' as i8, 0, 0, b'-' as i8, 0, 0, b'T' as i8, 0, 0, b':' as i8, 0, 0);
        let is_sep: u16 = 0b0010_0100_1001_0000;
        let eq = _mm_movemask_epi8(_mm_cmpeq_epi8(x, sep)) as u16;
        let d = _mm_sub_epi8(x, _mm_set1_epi8(b'0' as i8));
        let dig = _mm_movemask_epi8(_mm_cmplt_epi8(_mm_xor_si128(d, _mm_set1_epi8(-128)), _mm_set1_epi8(-128 + 10))) as u16;
        if eq & is_sep != is_sep || dig | is_sep != 0xFFFF || v[16] != b':' || !(v[14].is_ascii_digit() && v[15].is_ascii_digit() && v[17].is_ascii_digit() && v[18].is_ascii_digit()) {
            return None;
        }
        let g = |i: usize| (v[i] - b'0') as i64;
        let y = g(0) * 1000 + g(1) * 100 + g(2) * 10 + g(3);
        let mo = g(5) * 10 + g(6);
        let dd = g(8) * 10 + g(9);
        let h = g(11) * 10 + g(12);
        let mi = g(14) * 10 + g(15);
        let se = g(17) * 10 + g(18);
        if mo < 1 || mo > 12 || dd < 1 || dd > mdays(y, mo) || h > 23 || mi > 59 || se > 59 {
            return None;
        }
        Some(days(y, mo, dd) * 86_400_000 + ((h * 60 + mi) * 60 + se) * 1000)
    }
}

/// The `...Z` form (20 bytes).
#[inline(always)]
fn iso_z(v: &[u8]) -> Option<i64> {
    if v.len() != 20 || v[19] != b'Z' {
        return None;
    }
    civil19(v)
}

/// Event timestamp: ISO `...Z`, or epoch milliseconds of 1..=18 digits.
#[inline(always)]
fn ts_fast(v: &[u8]) -> Option<i64> {
    if v.len() == 20 && v[19] == b'Z' {
        return civil19(v);
    }
    if v.is_empty() || v.len() > 18 {
        return None;
    }
    let mut x = 0i64;
    for &b in v {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        x = x * 10 + d as i64;
    }
    Some(x)
}

#[inline(always)]
fn kind_of(v: &[u8]) -> u8 {
    match v.len() {
        4 => match v {
            b"view" => KIND_VIEW,
            b"cart" => KIND_CART,
            _ => KIND_OTHER,
        },
        8 if v == b"purchase" => KIND_PURCHASE,
        _ => KIND_OTHER,
    }
}

/// `p` then a canonical decimal of 1..=8 digits.
#[inline(always)]
fn prod_fast(v: &[u8]) -> Option<u32> {
    let n = v.len();
    if n < 2 || n > 9 || v[0] != b'p' || (v[1] == b'0' && n > 2) {
        return None;
    }
    let mut x = 0u32;
    for &b in &v[1..] {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        x = x * 10 + d as u32;
    }
    Some(x)
}

/// Price: 1..=8 digits without a leading zero, value 1..=10_000_000.
#[inline(always)]
fn price_fast(v: &[u8]) -> Option<u32> {
    let n = v.len();
    if n == 0 || n > 8 || v[0] == b'0' {
        return None;
    }
    let mut x = 0u32;
    for &b in v {
        let d = b.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        x = x * 10 + d as u32;
    }
    if x > 10_000_000 {
        return Some(0);
    }
    Some(x)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Rows(Box<EvBatch>, Vec<EvRow>);
    impl EventSink for Rows {
        fn ev(&mut self) -> *mut EvBatch {
            &mut *self.0
        }
        fn ev_flush(&mut self) {
            for i in 0..self.0.n {
                let r = self.0.row(i);
                self.1.push(r);
            }
            self.0.n = 0;
        }
    }
    fn ours(buf: &[u8]) -> Vec<EvRow> {
        let mut r = Rows(EvBatch::new(), Vec::new());
        <Ours as Kernel>::events(buf, &mut r);
        r.ev_flush();
        r.1
    }
    fn reference(buf: &[u8]) -> Vec<EvRow> {
        let mut r = Rows(EvBatch::new(), Vec::new());
        <k_ref::Ref as Kernel>::events(buf, &mut r);
        r.ev_flush();
        r.1
    }

    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, m: u64) -> u64 {
            self.next() % m
        }
    }

    /// Lines in the generator's style, with every field sometimes quoted, and
    /// a share of them mutated byte by byte.
    fn corpus(seed: u64, lines: usize) -> Vec<u8> {
        let mut r = Rng(seed | 1);
        let b36 = b"0123456789abcdefghijklmnopqrstuvwxyz";
        let mut out = Vec::new();
        for _ in 0..lines {
            let mut f: Vec<Vec<u8>> = Vec::new();
            for pre in [b'e', b'u'] {
                let mut v = vec![pre];
                for _ in 0..1 + r.below(9) {
                    v.push(b36[r.below(36) as usize]);
                }
                f.push(v);
            }
            for k in 0..2 {
                let day = 1 + r.below(31);
                let mo = 1 + r.below(12);
                let v = match (k, r.below(6)) {
                    (0, 0) => format!("{}", 1_780_000_000_000 + r.below(20_000_000_000)),
                    (0, 1) => format!("2026-{mo:02}-{day:02}T{:02}:{:02}:{:02}+0{}:30", r.below(25), r.below(61), r.below(61), r.below(10)),
                    _ => format!("2026-{mo:02}-{day:02}T{:02}:{:02}:{:02}Z", r.below(25), r.below(61), r.below(61)),
                };
                f.push(v.into_bytes());
            }
            f.push(["view", "cart", "purchase", "other", "", "View"][r.below(6) as usize].as_bytes().to_vec());
            f.push(match r.below(8) {
                0 => vec![],
                1 => b"p0".to_vec(),
                2 => b"p007".to_vec(),
                _ => format!("p{}", r.below(100_000_000)).into_bytes(),
            });
            f.push(match r.below(6) {
                0 => vec![],
                1 => b"0".to_vec(),
                2 => b"00123".to_vec(),
                3 => format!("{}", r.below(20_000_000)).into_bytes(),
                _ => format!("{}", 1 + r.below(100_000)).into_bytes(),
            });
            let mut line = Vec::new();
            for (i, v) in f.iter().enumerate() {
                if i > 0 {
                    line.push(b',');
                }
                let quote = r.below(4) == 0;
                if quote {
                    line.push(b'"');
                }
                line.extend_from_slice(v);
                if quote {
                    line.push(b'"');
                }
            }
            if r.below(10) == 0 {
                for _ in 0..1 + r.below(3) {
                    let i = r.below(line.len() as u64 + 1) as usize;
                    let c = b"\",xZ09-:T \re"[r.below(12) as usize];
                    match r.below(3) {
                        0 if i < line.len() => line[i] = c,
                        1 => line.insert(i.min(line.len()), c),
                        _ if i < line.len() => {
                            line.remove(i);
                        }
                        _ => {}
                    }
                }
            }
            if r.below(200) == 0 {
                line.extend(std::iter::repeat(b'x').take(100));
            }
            out.extend_from_slice(&line);
            out.push(b'\n');
        }
        out
    }

    fn same(b: &[u8], what: &str) {
        let (a, r) = (ours(b), reference(b));
        if a == r {
            return;
        }
        let i = a.iter().zip(&r).position(|(x, y)| x != y).unwrap_or(a.len().min(r.len()));
        let lines: Vec<&[u8]> = b.split(|&c| c == b'\n').filter(|l| k_ref::split_line::<7>(l, 0, l.len()).map_or(false, |v| v[0].0 != v[0].1)).collect();
        panic!("{what}: {} vs {} rows; first diff at {i}\n ours {:?}\n  ref {:?}\n line {:?}", a.len(), r.len(), a.get(i), r.get(i), lines.get(i).map(|l| String::from_utf8_lossy(l)));
    }

    #[test]
    fn matches_reference_on_generated_lines() {
        for seed in 1..40u64 {
            let buf = corpus(seed * 7919, 5000);
            for cut in [buf.len(), buf.len() - 1, buf.len() / 2] {
                let b = &buf[..cut];
                same(b, &format!("seed {seed} cut {cut}"));
            }
        }
    }

    #[test]
    fn matches_reference_on_data_files() {
        let Ok(dir) = std::env::var("KTEST_DATA") else { return };
        for e in std::fs::read_dir(format!("{dir}/events")).unwrap().take(3) {
            let buf = std::fs::read(e.unwrap().path()).unwrap();
            let body = &buf[buf.iter().position(|&b| b == b'\n').unwrap() + 1..];
            same(body, "data file");
        }
    }
}
