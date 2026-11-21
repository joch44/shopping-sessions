//! Parse gate: the LIVE pipeline's `csv::for_each_record`, `key::key_of`, `ts::parse` and
//! `ts::parse_cached` must equal the FROZEN v1 reference (src/reference.rs) on
//!   1. every line, key and timestamp of the given dataset dirs, at several chunkings;
//!   2. synthetic stress files aimed at the SIMD paths: line lengths around 64/128 bytes,
//!      a last line without `\n`, stray/edge quotes, `\r`, empty lines, every chunk start;
//!   3. hand-made timestamp and ID edge cases plus single-byte mutations.
//! Exit 0 = all equal; exit 1 at the first difference, printed with its context.
//!
//!   research/gate/run_gate.sh <pipeline dir> <dataset dirs...>

#[path = "live/csv.rs"]
#[allow(dead_code)]
mod csv;
#[path = "live/key.rs"]
#[allow(dead_code)]
mod key;
#[path = "live/ts.rs"]
#[allow(dead_code)]
mod ts;
mod reference;

use std::path::Path;

type Line = (usize, Vec<Option<Vec<u8>>>);

fn fail(what: &str) -> ! {
    eprintln!("PARSE GATE FAILED: {what}");
    std::process::exit(1);
}

fn show(b: &[u8]) -> String {
    format!("{:?}", String::from_utf8_lossy(b))
}

fn collect<const N: usize>(data: &[u8], start: usize, end: usize, live: bool) -> Vec<Line> {
    let mut out = Vec::new();
    let push = |f: &[Option<&[u8]>; N], off: usize, out: &mut Vec<Line>| out.push((off, f.iter().map(|x| x.map(<[u8]>::to_vec)).collect()));
    if live {
        csv::for_each_record::<N>(data, start, end, |f, off| push(f, off, &mut out));
    } else {
        reference::csv::for_each_record::<N>(data, start, end, |f, off| push(f, off, &mut out));
    }
    out
}

/// Compares live vs reference over `data` cut into chunks of `size` bytes.
fn compare_chunks<const N: usize>(name: &str, data: &[u8], size: usize) -> Vec<Line> {
    let mut all = Vec::new();
    let mut start = 0;
    while start < data.len() || start == 0 {
        let end = (start + size).min(data.len());
        let (a, b) = (collect::<N>(data, start, end, true), collect::<N>(data, start, end, false));
        if a != b {
            let i = a.iter().zip(&b).position(|(x, y)| x != y).unwrap_or(a.len().min(b.len()));
            let off = a.get(i).or(b.get(i)).map_or(start, |l| l.0);
            let line_end = memchr::memchr(b'\n', &data[off.min(data.len())..]).map_or(data.len(), |e| off + e);
            fail(&format!(
                "{name}: splitter differs (N={N}, chunk {start}..{end}, size {size}); live {} lines vs reference {}; first difference at byte {off}: line {}\n  live: {:?}\n  ref:  {:?}",
                a.len(), b.len(), show(&data[off.min(data.len())..line_end]), a.get(i), b.get(i)
            ));
        }
        all.extend(a);
        if end == data.len() {
            break;
        }
        start = end;
    }
    all
}

fn check_ts(v: &[u8], cache: &mut ts::DateCache, ctx: &str) {
    let want = reference::ts::parse(v);
    let (a, b) = (ts::parse(v), ts::parse_cached(v, cache));
    if a != want || b != want {
        fail(&format!("{ctx}: timestamp {} -> live parse {a:?}, parse_cached {b:?}, reference {want:?}", show(v)));
    }
}

fn check_key(v: &[u8], ctx: &str) {
    let (a, want) = (key::key_of(v, None), reference::key::key_of(v));
    if a != want {
        fail(&format!("{ctx}: key_of({}) = {a:032x}, reference {want:032x}", show(v)));
    }
}

/// (folder, field count, id columns, timestamp columns)
const TABLES: [(&str, usize, &[usize], &[usize]); 3] = [("events", 7, &[0, 1, 5], &[2, 3]), ("clicks", 4, &[0, 1, 2], &[3]), ("links", 2, &[0, 1], &[])];

fn check_file(name: &str, data: &[u8], n: usize, ids: &[usize], tss: &[usize], stats: &mut [u64; 3]) {
    let mut sizes = vec![data.len().max(1), 1 << 20];
    if data.len() < 4 << 20 {
        sizes.push(997);
    }
    if data.len() < 64 << 10 {
        sizes.extend([1, 63, 64, 127, 128, 129]);
    }
    let mut lines = Vec::new();
    for &size in &sizes {
        lines = match n {
            7 => compare_chunks::<7>(name, data, size),
            4 => compare_chunks::<4>(name, data, size),
            _ => compare_chunks::<2>(name, data, size),
        };
    }
    let mut cache = ts::DateCache::default();
    for (off, fields) in &lines {
        for &c in ids {
            if let Some(v) = &fields[c] {
                check_key(v, &format!("{name} @{off}"));
                stats[1] += 1;
            }
        }
        for &c in tss {
            if let Some(v) = &fields[c] {
                check_ts(v, &mut cache, &format!("{name} @{off}"));
                stats[2] += 1;
            }
        }
    }
    stats[0] += lines.len() as u64;
}

fn datasets(dirs: &[String], stats: &mut [u64; 3]) {
    for dir in dirs {
        for (folder, n, ids, tss) in TABLES {
            let Ok(entries) = std::fs::read_dir(Path::new(dir).join(folder)) else { continue };
            let mut files: Vec<_> = entries.filter_map(|e| e.ok().map(|e| e.path())).filter(|p| p.extension().is_some_and(|x| x == "csv")).collect();
            files.sort();
            for path in files {
                let file = std::fs::File::open(&path).unwrap();
                if file.metadata().unwrap().len() == 0 {
                    continue;
                }
                let map = unsafe { memmap2::Mmap::map(&file).unwrap() };
                check_file(&path.display().to_string(), &map, n, ids, tss, stats);
            }
        }
    }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// A synthetic CSV file: header, then lines of mostly N fields with dirt, lengths spread
/// over 0..~300 bytes so lines cross the 64/128-byte SIMD boundaries and the file end.
fn synthetic(seed: u64, n: usize) -> Vec<u8> {
    let mut r = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut out = b"h1,h2\n".to_vec();
    let lines = 50 + r.below(400);
    for _ in 0..lines {
        match r.below(40) {
            0 => {
                out.push(b'\n'); // empty line
                continue;
            }
            1 => {
                out.extend_from_slice(b"\r\n");
                continue;
            }
            _ => {}
        }
        let fields = match r.below(10) {
            0 => n - 1,
            1 => n + 1,
            _ => n,
        };
        // target a line length: often near 63/64/127/128/129
        let target = match r.below(4) {
            0 => [62u64, 63, 64, 65, 126, 127, 128, 129, 130][r.below(9) as usize],
            1 => r.below(300),
            _ => r.below(80),
        } as usize;
        let per = (target / fields.max(1)).max(1);
        for i in 0..fields {
            if i > 0 {
                out.push(b',');
            }
            let len = (per as u64).saturating_sub(1 + r.below(2)) as usize;
            let quote = r.below(5) == 0;
            if quote {
                out.push(b'"');
            }
            for _ in 0..len {
                out.push(match r.below(60) {
                    0 => b'"',
                    1 => b'\r',
                    2 => b' ',
                    3 => b'\t',
                    4 => 0xC3,
                    _ => b"abcdefxyz0123456789"[r.below(19) as usize],
                });
            }
            if quote && r.below(10) != 0 {
                out.push(b'"');
            }
        }
        if r.below(30) == 0 {
            out.push(b'"');
        }
        out.push(b'\n');
    }
    if r.below(2) == 0 {
        out.pop(); // last line without a newline
    }
    out
}

fn synthetic_suite(stats: &mut [u64; 3]) {
    for seed in 0..300u64 {
        for (n, sizes) in [(2usize, &[1usize, 64, 128, 4096][..]), (4, &[1, 129, 4096]), (7, &[1, 127, 4096])] {
            let data = synthetic(seed * 3 + n as u64, n);
            for &size in sizes {
                let lines = match n {
                    7 => compare_chunks::<7>(&format!("synthetic seed {seed}"), &data, size),
                    4 => compare_chunks::<4>(&format!("synthetic seed {seed}"), &data, size),
                    _ => compare_chunks::<2>(&format!("synthetic seed {seed}"), &data, size),
                };
                if size == 4096 {
                    stats[0] += lines.len() as u64;
                }
            }
            // also every exact tail cut: the same file truncated at each of its last 200 bytes
            for cut in data.len().saturating_sub(200)..data.len() {
                let d = &data[..cut];
                match n {
                    7 => drop(compare_chunks::<7>("synthetic truncated", d, 1 << 20)),
                    4 => drop(compare_chunks::<4>("synthetic truncated", d, 1 << 20)),
                    _ => drop(compare_chunks::<2>("synthetic truncated", d, 1 << 20)),
                }
            }
        }
    }
}

fn edge_suite(stats: &mut [u64; 3]) {
    let mut cache = ts::DateCache::default();
    let cases: &[&str] = &[
        "2026-10-03T23:58:10Z", "2026-10-03T19:58:10-04:00", "1791071890000", "0001791071890000", "2026-10-03T23:58:10-00:00",
        "99999999999999999999999", "18446744073709551616", "00000001791071890000", "0000000000001791071890000", "1234567890123456789012345",
        "2028-02-29T00:00:00Z", "2027-02-29T00:00:00Z", "2100-02-29T00:00:00Z", "2000-02-29T00:00:00Z", "2026-10-03T23:58:10z",
        "2026-10-03t23:58:10Z", "2026-10-03 23:58:10Z", "2026-10-03T23:58:10", "2026-10-03T23:58:10.000Z", "1.79e12", "-5",
        "2026-13-01T00:00:00Z", "2026-00-03T23:58:10Z", "2026-10-00T23:58:10Z", "2026-02-30T00:00:00Z", "2026-10-03T24:00:00Z",
        "2026-10-03T23:59:60Z", "2026-10-03T23:58:10+24:00", "2026-10-03T23:58:10+05:60", "2026-10-03T23:58:10+0200",
        "2026-10-03T23:58:10+23:59", "2026-10-03T23:58:10-23:59", "1791071890000.0", "2026-1a-03T23:58:10Z",
        "2026-10-03T23:58:1\u{0662}Z", "179107189000\u{0662}", "1791071890\u{ff12}0", "0000-01-01T00:00:00Z", "9999-12-31T23:59:59Z",
        "2026-10-03T23:58:10Z\r", "/791071890000", ":791071890000", "2026/10/03T23:58:10Z", "", "0", "86400000",
    ];
    for c in cases {
        check_ts(c.as_bytes(), &mut cache, "edge case");
        stats[2] += 1;
    }
    for base in [&b"1791071890000"[..], b"2026-10-03T19:58:10-04:00", b"2026-10-03T23:58:10Z", b"2028-02-29T23:59:59+12:00"] {
        let mut v = base.to_vec();
        for i in 0..v.len() {
            for &ch in b"0912/:\x00\xff.e-+TZz \r" {
                let keep = v[i];
                v[i] = ch;
                check_ts(&v, &mut cache, "mutation");
                for cut in [13usize, 19, 20, 25] {
                    if cut <= v.len() {
                        check_ts(&v[..cut], &mut cache, "mutation prefix");
                    }
                }
                v[i] = keep;
                stats[2] += 1;
            }
        }
    }
    let mut r = Rng(42);
    for len in 1..=24usize {
        for _ in 0..200 {
            let id: Vec<u8> = (0..len).map(|_| [b'a', b'z', b'0', b'9', b'_', b' ', b'\t', b'\r', 0, 0x7F, 0x80, 0xFF][r.below(12) as usize]).collect();
            check_key(&id, "edge id");
            stats[1] += 1;
        }
    }
}

fn main() {
    let dirs: Vec<String> = std::env::args().skip(1).collect();
    let mut stats = [0u64; 3];
    edge_suite(&mut stats);
    synthetic_suite(&mut stats);
    datasets(&dirs, &mut stats);
    println!("PARSE GATE OK: {} lines, {} keys, {} timestamps identical to the frozen v1 reference ({} dataset dirs)", stats[0], stats[1], stats[2], dirs.len());
}
