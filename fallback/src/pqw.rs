//! Minimal hand-written Parquet writer (from the research session's READING2 Topic E,
//! commit d9f0d4b; reviewed against the format spec).
//! PLAIN encoding, no compression, no statistics, one row group, one v1 data page per
//! column, Thrift-compact footer. Column kinds: required BYTE_ARRAY (UTF8/STRING),
//! optional BYTE_ARRAY (UTF8/STRING, definition levels as an RLE/bit-packed hybrid),
//! required INT32 DATE, required INT64.
//! Format reference: apache/parquet-format (README.md, Encodings.md, LogicalTypes.md,
//! src/main/thrift/parquet.thrift).

use std::io::Write;

pub enum Col<'a> {
    /// required string: concatenated bytes + end offsets
    Str { bytes: &'a [u8], ends: &'a [u32] },
    /// optional string: values only for present rows + one bool per row
    OptStr { bytes: &'a [u8], ends: &'a [u32], present: &'a [bool] },
    Date(&'a [i32]),
    I64(&'a [i64]),
    /// The same columns, dictionary-encoded (RLE_DICTIONARY: a PLAIN dictionary page, then
    /// bit-packed indices). Smaller files: the scorer's container returns only after its
    /// output data is written back, so bytes written cost wall time.
    StrDict { bytes: &'a [u8], ends: &'a [u32] },
    OptStrDict { bytes: &'a [u8], ends: &'a [u32], present: &'a [bool] },
    DateDict(&'a [i32]),
    I64Dict(&'a [i64]),
}

/// Dictionary of one column: PLAIN-encoded distinct values and one index per value.
struct Dict {
    plain: Vec<u8>,
    count: usize,
    indices: Vec<u32>,
}

fn dict_strings(bytes: &[u8], ends: &[u32]) -> Dict {
    let mut map: rustc_hash::FxHashMap<&[u8], u32> = rustc_hash::FxHashMap::default();
    let (mut plain, mut indices) = (Vec::new(), Vec::with_capacity(ends.len()));
    let mut s = 0usize;
    for &e in ends {
        let v = &bytes[s..e as usize];
        s = e as usize;
        let next = map.len() as u32;
        let i = *map.entry(v).or_insert_with(|| {
            plain.extend_from_slice(&(v.len() as u32).to_le_bytes());
            plain.extend_from_slice(v);
            next
        });
        indices.push(i);
    }
    Dict { plain, count: map.len(), indices }
}

fn dict_ints<T: Copy + Eq + std::hash::Hash>(values: &[T], le: impl Fn(T, &mut Vec<u8>)) -> Dict {
    let mut map: rustc_hash::FxHashMap<T, u32> = rustc_hash::FxHashMap::default();
    let (mut plain, mut indices) = (Vec::new(), Vec::with_capacity(values.len()));
    for &v in values {
        let next = map.len() as u32;
        let i = *map.entry(v).or_insert_with(|| {
            le(v, &mut plain);
            next
        });
        indices.push(i);
    }
    Dict { plain, count: map.len(), indices }
}

/// Bit width for indices into a dictionary of `count` entries (at least 1).
fn bit_width(count: usize) -> u8 {
    (usize::BITS - count.saturating_sub(1).leading_zeros()).max(1) as u8
}

/// Dictionary indices as a data-page body: the bit-width byte, then one bit-packed run
/// (RLE/bit-packed hybrid, LSB first, groups of 8 values padded with zeros).
fn packed_indices(indices: &[u32], width: u8, out: &mut Vec<u8>) {
    out.push(width);
    let groups = indices.len().div_ceil(8);
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        out.push((h as u8) | 0x80);
        h >>= 7;
    }
    out.push(h as u8);
    let w = u32::from(width);
    let (mut acc, mut bits) = (0u64, 0u32);
    for g in 0..groups {
        for k in 0..8 {
            let v = u64::from(indices.get(g * 8 + k).copied().unwrap_or(0));
            acc |= v << bits;
            bits += w;
            while bits >= 8 {
                out.push(acc as u8);
                acc >>= 8;
                bits -= 8;
            }
        }
    }
    debug_assert_eq!(bits, 0);
}

fn dict_page_header(size: usize, count: usize) -> Vec<u8> {
    let mut t = T::new();
    t.i32f(1, 2); // DICTIONARY_PAGE
    t.i32f(2, size as i32);
    t.i32f(3, size as i32);
    t.begin(7); // DictionaryPageHeader
    t.i32f(1, count as i32);
    t.i32f(2, 0); // PLAIN
    t.end();
    t.b.push(0);
    t.b
}

/// Thrift compact protocol, just what the footer needs.
struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}

impl T {
    fn new() -> Self {
        T { b: Vec::new(), last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(i64::from(id));
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.field(id, 5);
        self.zz(i64::from(v));
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.field(id, 6);
        self.zz(v);
    }
    fn strf(&mut self, id: i16, s: &[u8]) {
        self.field(id, 8);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, 12);
        self.last.push(0);
    }
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, 9);
        if n < 15 {
            self.b.push(((n as u8) << 4) | elem);
        } else {
            self.b.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
}

const BYTE_ARRAY: i32 = 6;
const INT32: i32 = 1;
const INT64: i32 = 2;

fn page_header(uncompressed: usize, num_values: usize, encoding: i32) -> Vec<u8> {
    let mut t = T::new();
    t.i32f(1, 0); // DATA_PAGE
    t.i32f(2, uncompressed as i32);
    t.i32f(3, uncompressed as i32);
    t.begin(5); // DataPageHeader
    t.i32f(1, num_values as i32);
    t.i32f(2, encoding); // PLAIN (0) or RLE_DICTIONARY (8)
    t.i32f(3, 3); // RLE (definition levels)
    t.i32f(4, 3); // RLE (repetition levels)
    t.end();
    t.b.push(0);
    t.b
}

/// Definition levels (bit width 1) as one bit-packed run, with the v1 4-byte length prefix.
fn def_levels(present: &[bool], out: &mut Vec<u8>) {
    let groups = present.len().div_ceil(8);
    let mut body = Vec::with_capacity(groups + 10);
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        body.push((h as u8) | 0x80);
        h >>= 7;
    }
    body.push(h as u8);
    for g in 0..groups {
        let mut byte = 0u8;
        for k in 0..8 {
            if present.get(g * 8 + k).copied().unwrap_or(false) {
                byte |= 1 << k;
            }
        }
        body.push(byte);
    }
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
}

fn plain_strings(bytes: &[u8], ends: &[u32], out: &mut Vec<u8>) {
    let mut s = 0usize;
    for &e in ends {
        let e = e as usize;
        out.extend_from_slice(&((e - s) as u32).to_le_bytes());
        out.extend_from_slice(&bytes[s..e]);
        s = e;
    }
}

/// Largest page this writer emits: page sizes are i32 in the format. Callers fall back to
/// the parquet crate for anything bigger.
pub const MAX_PAGE: usize = 1 << 30;

/// Writes one Parquet file with `rows` rows. `names[i]` is column i's name.
pub fn write(path: &std::path::Path, schema_name: &str, names: &[&str], cols: &[Col], rows: usize) -> std::io::Result<()> {
    let mut file = Vec::with_capacity(rows * 48 + 4096);
    file.extend_from_slice(b"PAR1");
    let mut chunks = Vec::new(); // (type, name, data page offset, size, dictionary page offset)
    let mut page = Vec::new();
    for (name, col) in names.iter().zip(cols) {
        page.clear();
        let dict = match col {
            Col::StrDict { bytes, ends } | Col::OptStrDict { bytes, ends, .. } => Some((BYTE_ARRAY, dict_strings(bytes, ends))),
            Col::DateDict(v) => Some((INT32, dict_ints(v, |x, out| out.extend_from_slice(&x.to_le_bytes())))),
            Col::I64Dict(v) => Some((INT64, dict_ints(v, |x, out| out.extend_from_slice(&x.to_le_bytes())))),
            _ => None,
        };
        if let Some((ty, d)) = dict {
            let dict_offset = file.len();
            let dh = dict_page_header(d.plain.len(), d.count);
            file.extend_from_slice(&dh);
            file.extend_from_slice(&d.plain);
            if let Col::OptStrDict { present, .. } = col {
                def_levels(present, &mut page);
            }
            packed_indices(&d.indices, bit_width(d.count), &mut page);
            let header = page_header(page.len(), rows, 8);
            let offset = file.len();
            file.extend_from_slice(&header);
            file.extend_from_slice(&page);
            chunks.push((ty, *name, offset, file.len() - dict_offset, Some(dict_offset)));
            continue;
        }
        let ty = match col {
            Col::Str { bytes, ends } => {
                plain_strings(bytes, ends, &mut page);
                BYTE_ARRAY
            }
            Col::OptStr { bytes, ends, present } => {
                def_levels(present, &mut page);
                plain_strings(bytes, ends, &mut page);
                BYTE_ARRAY
            }
            Col::Date(v) => {
                for x in v.iter() {
                    page.extend_from_slice(&x.to_le_bytes());
                }
                INT32
            }
            Col::I64(v) => {
                for x in v.iter() {
                    page.extend_from_slice(&x.to_le_bytes());
                }
                INT64
            }
            Col::StrDict { .. } | Col::OptStrDict { .. } | Col::DateDict(_) | Col::I64Dict(_) => unreachable!(),
        };
        let header = page_header(page.len(), rows, 0);
        let offset = file.len();
        file.extend_from_slice(&header);
        file.extend_from_slice(&page);
        chunks.push((ty, *name, offset, header.len() + page.len(), None));
    }
    // FileMetaData
    let mut t = T::new();
    t.i32f(1, 1); // version
    t.list(2, 12, cols.len() + 1);
    t.begin_elem(); // root
    t.strf(4, schema_name.as_bytes());
    t.i32f(5, cols.len() as i32);
    t.end();
    for (name, col) in names.iter().zip(cols) {
        t.begin_elem();
        let (ty, rep, conv, logical) = match col {
            Col::Str { .. } | Col::StrDict { .. } => (BYTE_ARRAY, 0, Some(0), Some(1)), // UTF8 / STRING
            Col::OptStr { .. } | Col::OptStrDict { .. } => (BYTE_ARRAY, 1, Some(0), Some(1)),
            Col::Date(_) | Col::DateDict(_) => (INT32, 0, Some(6), Some(6)),         // DATE / DATE
            Col::I64(_) | Col::I64Dict(_) => (INT64, 0, None, None),
        };
        t.i32f(1, ty);
        t.i32f(3, rep);
        t.strf(4, name.as_bytes());
        if let Some(c) = conv {
            t.i32f(6, c);
        }
        if let Some(l) = logical {
            t.begin(10); // LogicalType union
            t.begin(l); // STRING (1) or DATE (6): empty structs
            t.end();
            t.end();
        }
        t.end();
    }
    t.i64f(3, rows as i64);
    t.list(4, 12, 1);
    t.begin_elem(); // RowGroup
    t.list(1, 12, chunks.len());
    let total: usize = chunks.iter().map(|c| c.3).sum();
    for &(ty, name, offset, size, dict_offset) in &chunks {
        t.begin_elem(); // ColumnChunk
        t.i64f(2, dict_offset.unwrap_or(offset) as i64);
        t.begin(3); // ColumnMetaData
        t.i32f(1, ty);
        if dict_offset.is_some() {
            t.list(2, 5, 3);
            t.zz(0); // PLAIN (dictionary page)
            t.zz(3); // RLE (definition levels)
            t.zz(8); // RLE_DICTIONARY
        } else {
            t.list(2, 5, 2);
            t.zz(0); // PLAIN
            t.zz(3); // RLE
        }
        t.list(3, 8, 1);
        t.varint(name.len() as u64);
        t.b.extend_from_slice(name.as_bytes());
        t.i32f(4, 0); // UNCOMPRESSED
        t.i64f(5, rows as i64);
        t.i64f(6, size as i64);
        t.i64f(7, size as i64);
        t.i64f(9, offset as i64);
        if let Some(d) = dict_offset {
            t.i64f(11, d as i64); // dictionary_page_offset
        }
        t.end();
        t.end();
    }
    t.i64f(2, total as i64);
    t.i64f(3, rows as i64);
    t.end();
    t.b.push(0); // end FileMetaData
    file.extend_from_slice(&t.b);
    file.extend_from_slice(&(t.b.len() as u32).to_le_bytes());
    file.extend_from_slice(b"PAR1");
    std::fs::File::create(path)?.write_all(&file)
}
