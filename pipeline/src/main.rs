//! Shopping Sessions pipeline: parallel version.
//!
//! Stages (rules.md order; see NOTES.md / DATA_PROFILE.md for the reasons):
//!   A. lookups; links parsed in parallel, union-find, person = key of its smallest ID
//!   B. every event and click file cut into byte chunks aligned to line starts; threads
//!      parse + validate each line once, events to partitions by event_id, clicks by person
//!   C. per event_id partition: keep the copy with the smallest (ingest_ts, file position),
//!      then survivors to partitions by person
//!   D. per person partition: sort, sessionize, credit, assist, aggregate
//!   E. Parquet: one user_daily file per thread, one campaign_daily file
//!
//! Env: DATA_DIR, OUTPUT_DIR (or the two arguments), NCPU (threads), TIMING=1 (stage
//! times on stderr). Nothing is printed to stdout.

mod csv;
mod hugealloc;
mod key;
mod output;
mod people;
mod pqw;
mod scatter;
mod ts;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use memmap2::Mmap;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::csv::{ascii_digits, data_lines, for_each_record, parse_line};
use crate::key::{Key, LongIds, key_of, mix};
use crate::people::People;
use crate::scatter::Scatter;
use crate::ts::DAY_MS;

#[global_allocator]
static ALLOCATOR: hugealloc::HugeAlloc = hugealloc::HugeAlloc;

const SESSION_GAP_MS: i64 = 1_800_000; // a gap OVER this starts a new session
const CLICK_REACH_MS: i64 = 7 * DAY_MS; // a click reaches sessions up to 7 days later
const LATE_MS: i128 = DAY_MS as i128; // ingest_ts - ts OVER this is late
const PRICE_MAX: u64 = 10_000_000;
const NEVER: i64 = i64::MAX; // an unparseable ingest_ts never wins dedup and is late
pub const ORGANIC: u32 = u32::MAX; // campaign index of `organic`

// Partition counts, chosen once at startup from the input size (see `set_partitions`): each
// partition's working set should fit in L2. Powers of two; read through these masks.
static DEDUP_PARTITIONS: AtomicUsize = AtomicUsize::new(1024); // by event_id
static PERSON_PARTITIONS: AtomicUsize = AtomicUsize::new(1024); // by person

/// 1024 partitions up to ~30M events, 4096 above. Mirror, 60M: dedup 0.81 -> 0.69 s with 4096
/// event_id partitions, sessions 0.71 -> 0.64 s with 4096 person partitions, parse unchanged;
/// at 20M 4096 is neutral (parse +0.01 s), so the smaller count stays there.
/// PIPELINE_PARTITIONS (a power of two) overrides it, so tests can exercise any count.
fn set_partitions(estimated_events: usize) {
    let forced = std::env::var("PIPELINE_PARTITIONS").ok().and_then(|v| v.parse::<usize>().ok());
    let count = match forced {
        Some(n) if n.is_power_of_two() && n <= 8192 => n,
        _ if estimated_events > 30_000_000 => 4096,
        _ => 1024,
    };
    DEDUP_PARTITIONS.store(count, Ordering::Relaxed);
    PERSON_PARTITIONS.store(count, Ordering::Relaxed);
}

#[inline(always)]
fn dedup_partitions() -> usize {
    DEDUP_PARTITIONS.load(Ordering::Relaxed)
}

#[inline(always)]
fn person_partitions() -> usize {
    PERSON_PARTITIONS.load(Ordering::Relaxed)
}
const CHUNK_BYTES: usize = 16 << 20;
const CLICK_CHUNK_BYTES: usize = 4 << 20;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Timer {
    on: bool,
    start: Instant,
    last: Instant,
}

impl Timer {
    fn new() -> Self {
        let now = Instant::now();
        Self { on: std::env::var_os("TIMING").is_some(), start: now, last: now }
    }
    fn mark(&mut self, stage: &str) {
        if self.on {
            let now = Instant::now();
            eprintln!(
                "[timing] {stage:<34} {:8.3}s  (total {:8.3}s)",
                (now - self.last).as_secs_f64(),
                (now - self.start).as_secs_f64()
            );
            self.last = now;
        }
    }
}

/// Runs `count` tasks on `threads` threads; each thread keeps one state, returned at the end.
fn run_tasks<S: Send>(
    threads: usize,
    count: usize,
    make: impl Fn() -> S + Sync,
    work: impl Fn(&mut S, usize) + Sync,
) -> Vec<S> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    let mut state = make();
                    loop {
                        let task = next.fetch_add(1, Ordering::Relaxed);
                        if task >= count {
                            break;
                        }
                        work(&mut state, task);
                    }
                    state
                })
            })
            .collect();
        handles.into_iter().map(|handle| handle.join().expect("worker panicked")).collect()
    })
}

// ---------------------------------------------------------------- inputs

fn csv_files(folder: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(folder) else { return Vec::new() };
    let mut files: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.extension().is_some_and(|ext| ext == "csv")
                && std::fs::metadata(path).is_ok_and(|meta| meta.is_file())
        })
        .collect();
    files.sort();
    files
}

/// Memory-maps every CSV file of a folder in sorted order (zero-byte files become empty).
fn map_folder(folder: &Path) -> Result<Vec<Option<Mmap>>> {
    let mut maps = Vec::new();
    for path in csv_files(folder) {
        let file = File::open(&path)?;
        if file.metadata()?.len() == 0 {
            maps.push(None);
            continue;
        }
        // SAFETY: /data is read-only for the whole run; nothing truncates the files.
        maps.push(Some(unsafe { Mmap::map(&file)? }));
    }
    Ok(maps)
}

fn bytes_of(map: &Option<Mmap>) -> &[u8] {
    map.as_deref().unwrap_or(&[])
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Event,
    Click,
}

/// A byte range of one file. A line belongs to the chunk holding its first byte.
#[derive(Clone, Copy)]
struct Chunk {
    kind: Kind,
    file: usize, // index within its folder, in sorted order
    start: usize,
    end: usize,
}

/// Chunk size; PIPELINE_CHUNK_BYTES overrides it so tests can put boundaries everywhere.
fn chunk_bytes() -> usize {
    std::env::var("PIPELINE_CHUNK_BYTES").ok().and_then(|v| v.parse().ok()).filter(|&n| n > 0).unwrap_or(CHUNK_BYTES)
}

fn chunks_of(kind: Kind, maps: &[Option<Mmap>], out: &mut Vec<Chunk>) {
    // Clicks are ~12% of the input in their own pool: smaller chunks balance its tail.
    let chunk_bytes = match kind {
        Kind::Click if std::env::var_os("PIPELINE_CHUNK_BYTES").is_none() => CLICK_CHUNK_BYTES,
        _ => chunk_bytes(),
    };
    for (file, map) in maps.iter().enumerate() {
        let len = bytes_of(map).len();
        let mut start = 0;
        while start < len {
            let end = (start + chunk_bytes).min(len);
            out.push(Chunk { kind, file, start, end });
            start = end;
        }
    }
}

/// products.csv / campaigns.csv: first column -> second column (first occurrence wins).
fn load_lookup(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let Ok(bytes) = std::fs::read(path) else { return Vec::new() };
    let mut seen = FxHashSet::default();
    let mut rows = Vec::new();
    let mut fields = [None; 2];
    for line in data_lines(&bytes) {
        if parse_line(line, &mut fields) {
            if let Some(key) = fields[0] {
                if seen.insert(key.to_vec()) {
                    rows.push((key.to_vec(), fields[1].unwrap_or_default().to_vec()));
                }
            }
        }
    }
    rows
}

/// products.csv: product key -> category index (first occurrence of a product wins, as in
/// `load_lookup`), plus the category names in index order. It borrows from the file bytes
/// and allocates only for new category names: the generic loader's ~4 allocations per row
/// made this the whole of startup (17 ms of 17 at 20M; the other lookups and mmaps < 2 ms).
fn load_products(path: &Path) -> (FxHashMap<Key, u32>, Vec<Vec<u8>>) {
    let Ok(bytes) = std::fs::read(path) else { return (FxHashMap::default(), Vec::new()) };
    let mut products: FxHashMap<Key, u32> = FxHashMap::default();
    products.reserve(bytes.len() / 16);
    let mut category_names: Vec<Vec<u8>> = Vec::new();
    let mut category_index: FxHashMap<&[u8], u32> = FxHashMap::default();
    let mut fields = [None; 2];
    for line in data_lines(&bytes) {
        if !parse_line(line, &mut fields) {
            continue;
        }
        let Some(product) = fields[0] else { continue };
        let std::collections::hash_map::Entry::Vacant(slot) = products.entry(key_of(product, None)) else { continue };
        let category = fields[1].unwrap_or_default();
        let next = category_names.len() as u32;
        let index = *category_index.entry(category).or_insert(next);
        if index == next {
            category_names.push(category.to_vec());
        }
        slot.insert(index);
    }
    (products, category_names)
}

fn load_window(data: &Path) -> Result<(i64, i64)> {
    let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(data.join("manifest.json"))?)?;
    let get = |key: &str| -> Result<i64> {
        let text = manifest[key].as_str().ok_or_else(|| format!("manifest has no {key}"))?;
        ts::parse(text.as_bytes()).ok_or_else(|| format!("manifest {key} is not a timestamp: {text}").into())
    };
    Ok((get("window_start")?, get("window_end")?))
}

// ---------------------------------------------------------------- people (rules.md "Linked IDs")

/// ID key -> key of its person's name (the byte-wise smallest ID). IDs absent are their own person.
/// rules.md "Linked IDs" on all threads. Link files are cut into byte chunks and parsed in
/// parallel (same splitter and line rule as events). IDs are interned to dense indices per
/// hash bucket. A lock-free union-find always links the larger root index under the
/// smaller, so the components never depend on thread timing. Each component's name is its
/// byte-wise smallest ID (key::cmp, long IDs included), picked by a CAS minimum, and the
/// People table is built in parallel. The table's answers are identical for any NCPU.
fn resolve_people(maps: &[Option<Mmap>], threads: usize, longs: &LongIds) -> People {
    use std::sync::atomic::AtomicU32;
    const BUCKET_BITS: u32 = 8;
    const BUCKETS: usize = 1 << BUCKET_BITS;
    const SPAN: usize = 1 << 16; // pairs / nodes per union and naming task

    // 1. Parse: byte chunks of every link file (at most 1 MiB, so all threads get work).
    let chunk = chunk_bytes().min(1 << 20);
    let mut ranges = Vec::new();
    for (file, map) in maps.iter().enumerate() {
        let len = bytes_of(map).len();
        let mut start = 0;
        while start < len {
            let end = (start + chunk).min(len);
            ranges.push((file, start, end));
            start = end;
        }
    }
    let mut pieces: Vec<Vec<(Key, Key)>> = vec![Vec::new(); ranges.len()];
    for (at, pairs) in run_tasks(threads, ranges.len(), Vec::new, |out: &mut Vec<(usize, Vec<(Key, Key)>)>, at| {
        let (file, start, end) = ranges[at];
        let mut pairs = Vec::new();
        for_each_record::<2>(bytes_of(&maps[file]), start, end, |fields, _| {
            // Step 1: malformed lines and lines with an empty side are dropped.
            if let (Some(a), Some(b)) = (fields[0], fields[1]) {
                pairs.push((key_of(a, Some(longs)), key_of(b, Some(longs))));
            }
        });
        out.push((at, pairs));
    })
    .into_iter()
    .flatten()
    {
        pieces[at] = pairs;
    }
    let mut first_pair = Vec::with_capacity(pieces.len() + 1);
    let mut pair_count = 0usize;
    for piece in &pieces {
        first_pair.push(pair_count);
        pair_count += piece.len();
    }
    if pair_count == 0 {
        return People::new(std::iter::empty());
    }
    assert!(pair_count < (1 << 31), "over 2^31 links");

    // 2. Intern: every endpoint (pair * 2 + side) to its ID's hash bucket, then per bucket
    // a small table gives each distinct ID a local index; bucket offsets make them global.
    let bucket_of = |key: Key| (mix(key) >> (64 - BUCKET_BITS)) as usize;
    let mut by_piece: Vec<Vec<Vec<(Key, u32)>>> = vec![Vec::new(); pieces.len()];
    for (at, buckets) in run_tasks(threads, pieces.len(), Vec::new, |out: &mut Vec<(usize, Vec<Vec<(Key, u32)>>)>, at| {
        let mut buckets: Vec<Vec<(Key, u32)>> = (0..BUCKETS).map(|_| Vec::new()).collect();
        for (i, &(a, b)) in pieces[at].iter().enumerate() {
            let end = ((first_pair[at] + i) * 2) as u32;
            buckets[bucket_of(a)].push((a, end));
            buckets[bucket_of(b)].push((b, end + 1));
        }
        out.push((at, buckets));
    })
    .into_iter()
    .flatten()
    {
        by_piece[at] = buckets;
    }
    drop(pieces);
    let mut bucket_keys: Vec<Vec<Key>> = vec![Vec::new(); BUCKETS];
    let mut bucket_ends: Vec<Vec<(u32, u32)>> = vec![Vec::new(); BUCKETS];
    for (b, keys, ends) in run_tasks(threads, BUCKETS, Vec::new, |out: &mut Vec<(usize, Vec<Key>, Vec<(u32, u32)>)>, b| {
        let entries: usize = by_piece.iter().map(|piece| piece.get(b).map_or(0, Vec::len)).sum();
        let mask = (entries * 2).next_power_of_two().max(16) - 1;
        let mut table = vec![u32::MAX; mask + 1];
        let mut keys: Vec<Key> = Vec::new();
        let mut ends: Vec<(u32, u32)> = Vec::with_capacity(entries);
        for piece in &by_piece {
            let Some(list) = piece.get(b) else { continue };
            for &(key, end) in list {
                let mut slot = mix(key) as usize & mask;
                let local = loop {
                    match table[slot] {
                        u32::MAX => {
                            table[slot] = keys.len() as u32;
                            keys.push(key);
                            break keys.len() as u32 - 1;
                        }
                        local if keys[local as usize] == key => break local,
                        _ => slot = (slot + 1) & mask,
                    }
                };
                ends.push((end, local));
            }
        }
        out.push((b, keys, ends));
    })
    .into_iter()
    .flatten()
    {
        bucket_keys[b] = keys;
        bucket_ends[b] = ends;
    }
    drop(by_piece);
    let mut bucket_base = Vec::with_capacity(BUCKETS);
    let mut nodes = 0usize;
    for keys in &bucket_keys {
        bucket_base.push(nodes as u32);
        nodes += keys.len();
    }
    let node_keys: Vec<Key> = bucket_keys.into_iter().flatten().collect();
    let ends: Vec<AtomicU32> = (0..pair_count * 2).map(|_| AtomicU32::new(0)).collect();
    run_tasks(threads, BUCKETS, || (), |_, b| {
        for &(end, local) in &bucket_ends[b] {
            ends[end as usize].store(bucket_base[b] + local, Ordering::Relaxed);
        }
    });
    drop(bucket_ends);

    // 3. Union-find over node indices. A root is only ever re-pointed (by CAS) from itself
    // to a smaller index, and path halving only moves pointers to ancestors, so the forest
    // stays acyclic and every component's root ends as its smallest node index.
    let parent: Vec<AtomicU32> = (0..nodes as u32).map(AtomicU32::new).collect();
    fn find(parent: &[AtomicU32], mut node: u32) -> u32 {
        loop {
            let up = parent[node as usize].load(Ordering::Acquire);
            if up == node {
                return node;
            }
            let grand = parent[up as usize].load(Ordering::Acquire);
            if grand != up {
                let _ = parent[node as usize].compare_exchange_weak(up, grand, Ordering::AcqRel, Ordering::Relaxed);
            }
            node = grand;
        }
    }
    run_tasks(threads, pair_count.div_ceil(SPAN), || (), |_, at| {
        for pair in at * SPAN..((at + 1) * SPAN).min(pair_count) {
            let (mut a, mut b) = (ends[pair * 2].load(Ordering::Relaxed), ends[pair * 2 + 1].load(Ordering::Relaxed));
            loop {
                a = find(&parent, a);
                b = find(&parent, b);
                if a == b {
                    break;
                }
                let (low, high) = if a < b { (a, b) } else { (b, a) };
                if parent[high as usize].compare_exchange(high, low, Ordering::AcqRel, Ordering::Acquire).is_ok() {
                    break;
                }
            }
        }
    });
    drop(ends);

    // 4. Name: per component, the node with the byte-wise smallest ID (a CAS minimum; IDs
    // are distinct, so the minimum is unique whatever the order).
    let best: Vec<AtomicU32> = (0..nodes).map(|_| AtomicU32::new(u32::MAX)).collect();
    let roots: Vec<AtomicU32> = (0..nodes).map(|_| AtomicU32::new(0)).collect();
    run_tasks(threads, nodes.div_ceil(SPAN), || (), |_, at| {
        for node in at * SPAN..((at + 1) * SPAN).min(nodes) {
            let root = find(&parent, node as u32);
            roots[node].store(root, Ordering::Relaxed);
            let slot = &best[root as usize];
            let mut held = slot.load(Ordering::Acquire);
            loop {
                if held != u32::MAX && key::cmp(node_keys[held as usize], node_keys[node], longs).is_le() {
                    break;
                }
                match slot.compare_exchange_weak(held, node as u32, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => break,
                    Err(now) => held = now,
                }
            }
        }
    });

    // 5. The table: (ID, name) for every linked ID, built in parallel.
    let pieces: Vec<Vec<(Key, Key)>> = run_tasks(threads, nodes.div_ceil(SPAN), Vec::new, |out: &mut Vec<(usize, Vec<(Key, Key)>)>, at| {
        let range = at * SPAN..((at + 1) * SPAN).min(nodes);
        let named = range
            .map(|node| {
                let root = roots[node].load(Ordering::Relaxed) as usize;
                (node_keys[node], node_keys[best[root].load(Ordering::Relaxed) as usize])
            })
            .collect();
        out.push((at, named));
    })
    .into_iter()
    .flatten()
    .map(|(_, named)| named)
    .collect();
    People::build(&pieces, threads)
}

// ---------------------------------------------------------------- records

/// One parsed copy of an event: what dedup needs, plus the verdict of steps 3-4.
#[derive(Clone, Copy)]
struct EventCopy {
    id: Key,
    user: Key, // the copy's user_id; its person is resolved after links, in dedup
    seq: u64, // file and byte offset: the reference keeps the first copy seen on an ingest tie
    ingest: i64,
    ts: i64,
    price: u32, // INVALID marks a copy that failed steps 3-4 (valid prices are 0..=10,000,000)
    category: u32,
}

const INVALID: u32 = u32::MAX;

#[derive(Clone, Copy)]
pub struct Event {
    pub person: Key,
    pub ts: i64,
    pub price: u32, // 0 unless a purchase (valid purchase prices are 1..=10,000,000)
    pub category: u32,
}

#[derive(Clone, Copy)]
pub struct Click {
    pub person: Key,
    pub click_id: Key,
    pub ts: i64,
    pub campaign: u32,
}

struct Context {
    window: (i64, i64),
    products: FxHashMap<Key, u32>,
    unknown_category: u32,
    campaigns: FxHashMap<Key, u32>,
    longs: LongIds,
}



/// Events steps 3 (validate) and 4 (late) for one copy: Some((user key, ts, price, category)).
#[inline]
fn judge(context: &Context, fields: &[Option<&[u8]>; 7], ingest: i64, dates: &mut ts::DateCache) -> Option<(Key, i64, u32, u32)> {
    let [_, user, ts, _, kind, product, price] = *fields;
    let (user, product, kind) = (user?, product?, kind?);
    let purchase = match kind {
        b"purchase" => true,
        b"view" | b"cart" => false,
        _ => return None,
    };
    let ts = ts::parse_cached(ts?, dates)?;
    if ts < context.window.0 || ts >= context.window.1 {
        return None;
    }
    let price = if purchase {
        let price = price.filter(|p| ascii_digits(p))?;
        let value = price.iter().try_fold(0u64, |acc, &b| {
            let next = acc * 10 + u64::from(b - b'0');
            (next <= PRICE_MAX).then_some(next)
        })?;
        if value < 1 {
            return None;
        }
        value as u32
    } else {
        0
    };
    if ingest == NEVER || i128::from(ingest) - i128::from(ts) > LATE_MS {
        return None;
    }
    // Categories only matter for purchases (top_category sums purchase prices); views and
    // carts only need a non-empty product_id, checked above. Skip ~97% of the lookups.
    let category = if purchase {
        *context.products.get(&key_of(product, None)).unwrap_or(&context.unknown_category)
    } else {
        0
    };
    Some((key_of(user, Some(&context.longs)), ts, price, category))
}

#[inline]
fn dedup_partition(id: Key) -> usize {
    (mix(id) as usize) & (dedup_partitions() - 1)
}

#[inline]
fn person_partition(person: Key) -> usize {
    (mix(person) as usize) & (person_partitions() - 1)
}

struct ParseState {
    events: Scatter<EventCopy>, // by event_id partition
    clicks: Scatter<Click>,     // by person partition
    dates: ts::DateCache,
}

fn parse_chunk(
    context: &Context,
    people: Option<&People>,
    event_maps: &[Option<Mmap>],
    click_maps: &[Option<Mmap>],
    chunk: Chunk,
    state: &mut ParseState,
) {
    let (start_ms, end_ms) = (context.window.0.saturating_sub(CLICK_REACH_MS), context.window.1);
    match chunk.kind {
        Kind::Event => {
            let file_seq = (chunk.file as u64) << 40;
            // Step 1: for_each_record drops malformed lines.
            for_each_record::<7>(bytes_of(&event_maps[chunk.file]), chunk.start, chunk.end, |fields, offset| {
                let Some(event_id) = fields[0] else { return }; // step 1: empty event_id
                let id = key_of(event_id, None);
                let ingest = fields[3].and_then(|v| ts::parse_ingest(v, &mut state.dates)).unwrap_or(NEVER);
                let seq = file_seq | offset as u64;
                let copy = match judge(context, fields, ingest, &mut state.dates) {
                    Some((user, ts, price, category)) => EventCopy { id, user, seq, ingest, ts, price, category },
                    None => EventCopy { id, user: 0, seq, ingest, ts: 0, price: INVALID, category: 0 },
                };
                state.events.push(dedup_partition(id), copy);
            });
            crate::scatter::fence(); // the arena is read by other threads in dedup
        }
        Kind::Click => {
            let people = people.expect("clicks are parsed after links");
            let mut pending: Vec<(Key, Click)> = Vec::with_capacity(CLICK_BATCH);
            for_each_record::<4>(bytes_of(&click_maps[chunk.file]), chunk.start, chunk.end, |fields, _| {
                // Steps 2-3. Dedup is skipped: copies of a click have identical values, so a
                // duplicate can change neither the credited click nor the set of assisting
                // campaigns (rules.md Clicks step 2).
                let [Some(click_id), Some(user), Some(campaign), Some(ts)] = *fields else { return };
                let Some(&campaign) = context.campaigns.get(&key_of(campaign, None)) else { return };
                let Some(ts) = ts::parse_cached(ts, &mut state.dates) else { return };
                if ts < start_ms || ts >= end_ms {
                    return;
                }
                // The person lookup misses the cache: prefetch now, resolve when the batch fills.
                let user = key_of(user, Some(&context.longs));
                people.prefetch(user);
                pending.push((user, Click { person: 0, click_id: key_of(click_id, Some(&context.longs)), ts, campaign }));
                if pending.len() == CLICK_BATCH {
                    flush_clicks(people, &mut pending, &mut state.clicks);
                }
            });
            flush_clicks(people, &mut pending, &mut state.clicks);
        }
    }
}

const CLICK_BATCH: usize = 64; // research sweep: 16/32/64/128 -> 0.125/0.118/0.116/0.116 s (mirror, 20M)

/// Resolves a batch of clicks' persons (prefetched when they entered the batch) and
/// scatters them by person.
#[inline]
fn flush_clicks(people: &People, pending: &mut Vec<(Key, Click)>, out: &mut Scatter<Click>) {
    for (user, mut click) in pending.drain(..) {
        click.person = people.get(user);
        out.push(person_partition(click.person), click);
    }
}

/// Events step 2 for one event_id partition: per id, the copy with the smallest
/// (ingest_ts, file position) is kept; if it is valid it survives. `pieces` are this
/// partition's slices from every thread, read in place.
///
/// The table holds (32-bit hash << 32 | copy index), so a probe compares hashes and only
/// touches a record when they match (in practice only real duplicates). Winners are marked
/// per copy and emitted in record order: both passes over the records are sequential.
fn dedup(pieces: Vec<&[EventCopy]>, people: &People, scratch: &mut DedupScratch, out: &mut Scatter<Event>) {
    let total: usize = pieces.iter().map(|piece| piece.len()).sum();
    assert!(total < u32::MAX as usize, "dedup partition over 4G copies");
    let starts: Vec<usize> = pieces.iter().scan(0, |at, piece| { let start = *at; *at += piece.len(); Some(start) }).collect();
    let copy_at = |index: u32| {
        let index = index as usize;
        let piece = starts.partition_point(|&start| start <= index) - 1;
        &pieces[piece][index - starts[piece]]
    };
    const EMPTY: u64 = u64::MAX;
    let slots = (total * 2).next_power_of_two().max(16);
    let mask = slots - 1;
    let DedupScratch { table, winner, survivors } = scratch;
    table.clear();
    table.resize(slots, EMPTY);
    winner.clear();
    winner.resize(total, false);
    let mut index: u32 = 0;
    for copy in pieces.iter().copied().flatten() {
        let h = mix(copy.id);
        let tag = h >> 32;
        // Within one partition the low (partition) bits are equal: index the table above them.
        let mut slot = (h >> 20) as usize & mask;
        loop {
            let held = table[slot];
            if held == EMPTY {
                table[slot] = (tag << 32) | u64::from(index);
                winner[index as usize] = true;
                break;
            }
            if held >> 32 == tag {
                let other_index = held as u32;
                let other = copy_at(other_index);
                if other.id == copy.id {
                    if (copy.ingest, copy.seq) < (other.ingest, other.seq) {
                        table[slot] = (tag << 32) | u64::from(index);
                        winner[other_index as usize] = false;
                        winner[index as usize] = true;
                    }
                    break;
                }
            }
            slot = (slot + 1) & mask;
        }
        index += 1;
    }
    // Survivors in record order, with each person lookup prefetched 16 survivors ahead.
    survivors.clear();
    for (copy, &won) in pieces.iter().copied().flatten().zip(winner.iter()) {
        if won && copy.price != INVALID {
            survivors.push(*copy);
        }
    }
    const AHEAD: usize = 16;
    for copy in survivors.iter().take(AHEAD) {
        people.prefetch(copy.user);
    }
    for at in 0..survivors.len() {
        if let Some(ahead) = survivors.get(at + AHEAD) {
            people.prefetch(ahead.user);
        }
        let copy = &survivors[at];
        let person = people.get(copy.user);
        out.push(person_partition(person), Event { person, ts: copy.ts, price: copy.price, category: copy.category });
    }
}

#[derive(Default)]
struct DedupScratch {
    table: Vec<u64>,
    winner: Vec<bool>,
    survivors: Vec<EventCopy>,
}

// ---------------------------------------------------------------- sessions, attribution, aggregation

/// A timestamp stored as an offset from window_start within a person's group: u32 (4 bytes)
/// whenever the window spans fewer than 2^32 ms (~49.7 days; every real dataset), i64 for a
/// longer window, which the rules do not forbid. One session implementation serves both.
trait TsOffset: Copy + Ord + Default {
    fn from_rel(rel: i64) -> Self;
    fn rel(self) -> i64;
    /// This offset type's scratch in the per-thread state.
    fn scratch(state: &mut PeopleState) -> &mut GroupScratch<Self>;
}

impl TsOffset for u32 {
    #[inline(always)]
    fn from_rel(rel: i64) -> Self {
        rel as u32 // callers use u32 only when every rel fits: 0 <= rel < 2^32
    }
    #[inline(always)]
    fn rel(self) -> i64 {
        i64::from(self)
    }
    fn scratch(state: &mut PeopleState) -> &mut GroupScratch<Self> {
        &mut state.scratch
    }
}

impl TsOffset for i64 {
    #[inline(always)]
    fn from_rel(rel: i64) -> Self {
        rel
    }
    #[inline(always)]
    fn rel(self) -> i64 {
        self
    }
    fn scratch(state: &mut PeopleState) -> &mut GroupScratch<Self> {
        &mut state.scratch_wide
    }
}

/// One purchase within a person's group (events store only their ts offset).
#[derive(Clone, Copy, Default)]
struct Purchase<T> {
    pub ts: T, // ms since window_start
    pub price: u32,
    pub category: u32,
}


/// One click within a person's group (32 bytes instead of 48).
#[derive(Clone, Copy)]
pub struct SessionClick {
    pub click_id: Key,
    pub ts: i64,
    pub campaign: u32,
}

pub struct UserRow {
    pub person: Key,
    pub day: i32,
    pub sessions: u64,
    pub events: u64,
    pub revenue: u64,
    pub top_category: Option<u32>,
}

#[derive(Default, Clone, Copy)]
pub struct CampaignCounts {
    pub sessions: u64,
    pub purchasing: u64,
    pub revenue: u64,
    pub assisted: u64,
}

/// Per-thread campaign_daily counters: a dense array indexed by (campaign, day) when the
/// window is a sane size (every session starts inside it), with a map for anything else.
/// `organic` is the index after the last campaign.
struct CampaignCounters {
    dense: Vec<CampaignCounts>,
    first_day: i64,
    days: usize,
    organic: u32,
    sparse: FxHashMap<(u32, i32), CampaignCounts>,
}

impl CampaignCounters {
    fn new(campaigns: usize, window: (i64, i64)) -> Self {
        let first_day = window.0.div_euclid(DAY_MS);
        let days = ((window.1 - 1).div_euclid(DAY_MS) - first_day + 1).max(1) as usize;
        let cells = (campaigns + 1).saturating_mul(days);
        let dense = if cells <= 8 << 20 { vec![CampaignCounts::default(); cells] } else { Vec::new() };
        Self { dense, first_day, days, organic: campaigns as u32, sparse: FxHashMap::default() }
    }

    #[inline(always)]
    fn at(&mut self, campaign: u32, day: i32) -> &mut CampaignCounts {
        let index = if campaign == ORGANIC { self.organic } else { campaign };
        let offset = i64::from(day) - self.first_day;
        if !self.dense.is_empty() && (0..self.days as i64).contains(&offset) {
            &mut self.dense[index as usize * self.days + offset as usize]
        } else {
            self.sparse.entry((campaign, day)).or_default()
        }
    }

    /// Adds these counts into `total`, keyed as (campaign or ORGANIC, day); only touched cells.
    fn merge_into(self, total: &mut FxHashMap<(u32, i32), CampaignCounts>) {
        let add = |total: &mut FxHashMap<(u32, i32), CampaignCounts>, key, c: &CampaignCounts| {
            let row = total.entry(key).or_default();
            row.sessions += c.sessions;
            row.purchasing += c.purchasing;
            row.revenue += c.revenue;
            row.assisted += c.assisted;
        };
        for (cell, counts) in self.dense.iter().enumerate() {
            if counts.sessions == 0 && counts.assisted == 0 {
                continue; // never credited nor assisted: no row
            }
            let (index, offset) = ((cell / self.days) as u32, (cell % self.days) as i64);
            let campaign = if index == self.organic { ORGANIC } else { index };
            add(total, (campaign, (self.first_day + offset) as i32), counts);
        }
        for (key, counts) in &self.sparse {
            add(total, *key, counts);
        }
    }
}

struct PeopleState {
    win_count: Vec<u32>,
    win_pos: Vec<u32>,
    active: Vec<u32>,
    scratch: GroupScratch<u32>,
    scratch_wide: GroupScratch<i64>,
    user_rows: Vec<UserRow>,
    campaign_daily: CampaignCounters,
    session_number: u64,
}

struct DayAccumulator {
    day: i32,
    sessions: u64,
    events: u64,
    revenue: u64,
    by_category: Vec<(u32, u64)>,
}

fn flush(person: Key, acc: &mut Option<DayAccumulator>, rows: &mut Vec<UserRow>, category_names: &[Vec<u8>]) {
    if let Some(day) = acc.take() {
        // Highest summed purchase price; a tie goes to the byte-wise smallest name.
        let top_category = day
            .by_category
            .iter()
            .max_by(|a, b| a.1.cmp(&b.1).then_with(|| category_names[b.0 as usize].cmp(&category_names[a.0 as usize])))
            .map(|&(category, _)| category);
        rows.push(UserRow { person, day: day.day, sessions: day.sessions, events: day.events, revenue: day.revenue, top_category });
    }
}

/// One person: their events sorted by ts, their clicks sorted by (ts, click_id).
fn person_sessions<T: TsOffset>(person: Key, ts_rel: &[T], purchases: &[Purchase<T>], window_start: i64, clicks: &[SessionClick], state: &mut PeopleState, category_names: &[Vec<u8>]) {
    let mut day_acc: Option<DayAccumulator> = None;
    let mut first = 0;
    let (mut low, mut high) = (0usize, 0usize);
    let mut next_purchase = 0usize;
    while first < ts_rel.len() {
        // Events step 5: a gap OVER 30 min starts a new session (equal ts: gap 0).
        let mut last = first + 1;
        while last < ts_rel.len() && ts_rel[last].rel() - ts_rel[last - 1].rel() <= SESSION_GAP_MS {
            last += 1;
        }
        let start = window_start + ts_rel[first].rel();
        let session_len = last - first;
        let end_ts = ts_rel[last - 1];
        first = last;
        state.session_number += 1;

        let day = start.div_euclid(DAY_MS) as i32; // step 6: UTC date of the start
        // The session's purchases: every purchase is an event, so each one's ts lies inside
        // exactly one session's [first ts, last ts] range; take them in ts order.
        let purchase_first = next_purchase;
        while next_purchase < purchases.len() && purchases[next_purchase].ts <= end_ts {
            next_purchase += 1;
        }
        let session_purchases = &purchases[purchase_first..next_purchase];
        let revenue: u64 = session_purchases.iter().map(|purchase| u64::from(purchase.price)).sum();
        let purchased = !session_purchases.is_empty();

        if day_acc.as_ref().is_some_and(|acc| acc.day != day) {
            flush(person, &mut day_acc, &mut state.user_rows, category_names);
        }
        let acc = day_acc.get_or_insert_with(|| DayAccumulator { day, sessions: 0, events: 0, revenue: 0, by_category: Vec::new() });
        acc.sessions += 1;
        acc.events += session_len as u64;
        acc.revenue += revenue;
        for event in session_purchases {
            match acc.by_category.iter_mut().find(|(c, _)| *c == event.category) {
                Some(entry) => entry.1 += u64::from(event.price),
                None => acc.by_category.push((event.category, u64::from(event.price))),
            }
        }

        // Clicks step 4: qualifying clicks: start - 7 d <= ts <= start. Session starts only
        // increase, so the window [low, high) slides; the distinct campaigns inside it are kept
        // incrementally (count per campaign + active list) for step 7.
        while high < clicks.len() && clicks[high].ts <= start {
            let c = clicks[high].campaign as usize;
            if state.win_count[c] == 0 {
                state.win_pos[c] = state.active.len() as u32;
                state.active.push(clicks[high].campaign);
            }
            state.win_count[c] += 1;
            high += 1;
        }
        while low < high && clicks[low].ts < start - CLICK_REACH_MS {
            let c = clicks[low].campaign as usize;
            state.win_count[c] -= 1;
            if state.win_count[c] == 0 {
                let at = state.win_pos[c] as usize;
                let last = state.active.pop().expect("active campaign");
                if at < state.active.len() {
                    state.active[at] = last;
                    state.win_pos[last as usize] = at as u32;
                }
            }
            low += 1;
        }
        let credited = if high > low {
            // The latest wins; on a tie the byte-wise smallest click_id (clicks are sorted by
            // (ts, click_id)): the first of the clicks sharing the latest ts.
            let latest = clicks[high - 1].ts;
            let mut first_latest = high - 1;
            while first_latest > low && clicks[first_latest - 1].ts == latest {
                first_latest -= 1;
            }
            // Clicks step 7: every campaign among the same clicks assists once.
            for &campaign in &state.active {
                state.campaign_daily.at(campaign, day).assisted += 1;
            }
            clicks[first_latest].campaign
        } else {
            ORGANIC // step 5
        };
        let row = state.campaign_daily.at(credited, day);
        row.sessions += 1;
        row.purchasing += u64::from(purchased);
        row.revenue += revenue;
    }
    flush(person, &mut day_acc, &mut state.user_rows, category_names);
    // Reset the window for the next person.
    for &campaign in &state.active {
        state.win_count[campaign as usize] = 0;
    }
    state.active.clear();
}

/// Groups one person partition by person (hash + counting sort, no full sort), then runs
/// each person's events (sorted by ts) and clicks (sorted by ts, click_id) through
/// `person_sessions`. Clicks of a person with no surviving events can credit nothing.
/// Reads the per-thread pieces in place; all temporary vectors live in `state.scratch`
/// and are reused across partitions.
fn people_partition<T: TsOffset>(
    event_pieces: Vec<&[Event]>,
    click_pieces: Vec<&[Click]>,
    longs: &LongIds,
    state: &mut PeopleState,
    category_names: &[Vec<u8>],
    window_start: i64,
) {
    let total: usize = event_pieces.iter().map(|piece| piece.len()).sum();
    if total == 0 {
        return;
    }
    let mut scratch = std::mem::take(T::scratch(state));
    let GroupScratch { table, group_keys, event_start, cursor, click_group, click_start, grouped_clicks, grouped_ts, purchase_start, grouped_purchases, keyed, keyed_purchases } =
        &mut scratch;
    // person -> group number, open addressing over group numbers (u32::MAX = empty).
    let slots = (total * 2).next_power_of_two().max(16);
    let mask = slots - 1;
    table.clear();
    table.resize(slots, u32::MAX);
    group_keys.clear();
    let find = |table: &[u32], group_keys: &[Key], person: Key| -> (usize, Option<u32>) {
        let mut slot = (mix(person) >> 24) as usize & mask;
        loop {
            match table[slot] {
                u32::MAX => return (slot, None),
                group if group_keys[group as usize] == person => return (slot, Some(group)),
                _ => slot = (slot + 1) & mask,
            }
        }
    };
    // One pass over the survivors (they come from DRAM): each event's group and ts offset go
    // into an L2-sized temporary, purchases with their group into another; the counting sorts
    // below read only these, not the survivors a second time (mirror: sessions -0.013 s @20M,
    // -0.043 s @60M).
    keyed.clear();
    keyed_purchases.clear();
    for event in event_pieces.iter().copied().flatten() {
        let group = match find(table, group_keys, event.person) {
            (_, Some(group)) => group,
            (slot, None) => {
                table[slot] = group_keys.len() as u32;
                group_keys.push(event.person);
                (group_keys.len() - 1) as u32
            }
        };
        let ts_rel = T::from_rel(event.ts - window_start);
        keyed.push((group, ts_rel));
        if event.price > 0 {
            keyed_purchases.push((group, Purchase { ts: ts_rel, price: event.price, category: event.category }));
        }
    }
    let groups = group_keys.len();
    // Counting sort of events, then of clicks, into group order.
    event_start.clear();
    event_start.resize(groups + 1, 0);
    for &(group, _) in keyed.iter() {
        event_start[group as usize + 1] += 1;
    }
    for g in 0..groups {
        event_start[g + 1] += event_start[g];
    }
    // Timestamps as u32 offsets (4 B per event); purchases (~3%) in their own list.
    cursor.clear();
    cursor.extend_from_slice(event_start);
    purchase_start.clear();
    purchase_start.resize(groups + 1, 0);
    grouped_ts.clear();
    grouped_ts.reserve(total);
    let spare = grouped_ts.spare_capacity_mut();
    for &(group, ts_rel) in keyed.iter() {
        let at = &mut cursor[group as usize];
        spare[*at as usize].write(ts_rel);
        *at += 1;
    }
    for &(group, _) in keyed_purchases.iter() {
        purchase_start[group as usize + 1] += 1;
    }
    // SAFETY: the counting sort wrote every slot in 0..total exactly once.
    unsafe { grouped_ts.set_len(total) };
    for g in 0..groups {
        purchase_start[g + 1] += purchase_start[g];
    }
    let purchase_total = purchase_start[groups] as usize;
    cursor.clear();
    cursor.extend_from_slice(purchase_start);
    grouped_purchases.clear();
    grouped_purchases.reserve(purchase_total);
    let spare = grouped_purchases.spare_capacity_mut();
    for &(group, purchase) in keyed_purchases.iter() {
        let at = &mut cursor[group as usize];
        spare[*at as usize].write(purchase);
        *at += 1;
    }
    // SAFETY: as above, for the purchases.
    unsafe { grouped_purchases.set_len(purchase_total) };
    click_group.clear();
    click_start.clear();
    click_start.resize(groups + 1, 0);
    for click in click_pieces.iter().copied().flatten() {
        let group = find(table, group_keys, click.person).1.unwrap_or(u32::MAX);
        click_group.push(group);
        if group != u32::MAX {
            click_start[group as usize + 1] += 1;
        }
    }
    for g in 0..groups {
        click_start[g + 1] += click_start[g];
    }
    let kept_clicks = click_start[groups] as usize;
    cursor.clear();
    cursor.extend_from_slice(click_start);
    grouped_clicks.clear();
    grouped_clicks.reserve(kept_clicks);
    let spare = grouped_clicks.spare_capacity_mut();
    for (click, &group) in click_pieces.iter().copied().flatten().zip(click_group.iter()) {
        if group != u32::MAX {
            let at = &mut cursor[group as usize];
            spare[*at as usize].write(SessionClick { click_id: click.click_id, ts: click.ts, campaign: click.campaign });
            *at += 1;
        }
    }
    // SAFETY: the counting sort wrote every slot in 0..kept_clicks exactly once.
    unsafe { grouped_clicks.set_len(kept_clicks) };
    for g in 0..groups {
        let person_ts = &mut grouped_ts[event_start[g] as usize..event_start[g + 1] as usize];
        person_ts.sort_unstable();
        let person_purchases = &mut grouped_purchases[purchase_start[g] as usize..purchase_start[g + 1] as usize];
        if person_purchases.len() > 1 {
            person_purchases.sort_unstable_by_key(|purchase| purchase.ts);
        }
        let person_clicks = &mut grouped_clicks[click_start[g] as usize..click_start[g + 1] as usize];
        if person_clicks.iter().all(|click| !key::is_long(click.click_id)) {
            // No long click_id: key order is byte-wise order, so sort on (ts, key) directly.
            person_clicks.sort_unstable_by_key(|click| (click.ts, click.click_id));
        } else {
            person_clicks.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| key::cmp(a.click_id, b.click_id, longs)));
        }
        person_sessions(group_keys[g], person_ts, person_purchases, window_start, person_clicks, state, category_names);
    }
    *T::scratch(state) = scratch;
}

/// Temporary vectors of `people_partition`, kept per thread and reused.
#[derive(Default)]
struct GroupScratch<T> {
    table: Vec<u32>,
    group_keys: Vec<Key>,
    event_start: Vec<u32>,
    cursor: Vec<u32>,
    grouped_ts: Vec<T>,
    purchase_start: Vec<u32>,
    grouped_purchases: Vec<Purchase<T>>,
    keyed: Vec<(u32, T)>,
    keyed_purchases: Vec<(u32, Purchase<T>)>,
    click_group: Vec<u32>,
    click_start: Vec<u32>,
    grouped_clicks: Vec<SessionClick>,
}

// ---------------------------------------------------------------- main

fn run() -> Result<()> {
    let mut timer = Timer::new();
    let args: Vec<String> = std::env::args().collect();
    let data = PathBuf::from(std::env::var("DATA_DIR").ok().or_else(|| args.get(1).cloned()).unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").ok().or_else(|| args.get(2).cloned()).unwrap_or("/output".into()));
    let threads = std::env::var("NCPU")
        .ok()
        .and_then(|n| n.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, usize::from));

    // A. lookups
    // The lookups and the three folders' mmaps are independent: one thread each, so
    // startup is the slowest of them rather than their sum.
    let data_ref = &data;
    let (window, (products, category_names), (campaign_rows, campaigns), event_maps, click_maps, link_maps) =
        std::thread::scope(|scope| -> Result<_> {
            let products = scope.spawn(move || load_products(&data_ref.join("products.csv")));
            let campaigns = scope.spawn(move || {
                let campaign_rows = load_lookup(&data_ref.join("campaigns.csv"));
                let campaigns: FxHashMap<Key, u32> =
                    campaign_rows.iter().enumerate().map(|(index, (id, _))| (key_of(id, None), index as u32)).collect();
                (campaign_rows, campaigns)
            });
            let map = |folder: &'static str| scope.spawn(move || map_folder(&data_ref.join(folder)).map_err(|e| e.to_string()));
            let (events, clicks, links) = (map("events"), map("clicks"), map("links"));
            let window = load_window(data_ref)?;
            let join_maps = |h: std::thread::ScopedJoinHandle<'_, std::result::Result<Vec<Option<Mmap>>, String>>| -> Result<Vec<Option<Mmap>>> {
                Ok(h.join().expect("mmap thread panicked")?)
            };
            Ok((
                window,
                products.join().expect("products thread panicked"),
                campaigns.join().expect("campaigns thread panicked"),
                join_maps(events)?,
                join_maps(clicks)?,
                join_maps(links)?,
            ))
        })?;
    let mut category_names = category_names;
    // `unknown`: a name no category in the file uses (rules.md), so a fresh index.
    let unknown_category = category_names.len() as u32;
    category_names.push(b"unknown".to_vec());
    timer.mark("lookups + mmap");

    let context = Context { window, products, unknown_category, campaigns, longs: LongIds::default() };

    // A+B. Task 0 resolves links while the other threads parse events (largest chunks
    // first). resolve_people fans its phases out over `threads` workers of its own, so links
    // stay off the critical path and finish in a fraction of the serial time (the brief
    // oversubscription does not change the total work). Clicks need people: parsed after.
    let mut event_chunks = Vec::new();
    chunks_of(Kind::Event, &event_maps, &mut event_chunks);
    event_chunks.sort_by_key(|chunk| std::cmp::Reverse(chunk.end - chunk.start));
    let people_slot: Mutex<Option<People>> = Mutex::new(None);
    // Pre-size each thread's partition buffers from the input size (~77 bytes per event
    // line, ~47 per click line, 15% slack), so they rarely regrow; a wrong guess only costs speed.
    let total_bytes = |maps: &[Option<Mmap>]| maps.iter().map(|m| bytes_of(m).len()).sum::<usize>();
    let per_buffer = |bytes: usize, line: usize, parts: usize| bytes / line * 115 / 100 / threads / parts;
    set_partitions(total_bytes(&event_maps) / 77);
    let (dedup_parts, person_parts) = (dedup_partitions(), person_partitions());
    let event_cap = per_buffer(total_bytes(&event_maps), 77, dedup_parts);
    let click_cap = per_buffer(total_bytes(&click_maps), 47, person_parts);
    let survivor_cap = per_buffer(total_bytes(&event_maps), 77, person_parts);
    let new_events = || ParseState {
        events: Scatter::new(dedup_parts, event_cap),
        clicks: Scatter::new(person_parts, 0),
        dates: ts::DateCache::default(),
    };
    let new_clicks = || ParseState {
        events: Scatter::new(dedup_parts, 0),
        clicks: Scatter::new(person_parts, click_cap),
        dates: ts::DateCache::default(),
    };
    let parsed = run_tasks(threads, event_chunks.len() + 1, new_events, |state, at| {
        if at == 0 {
            *people_slot.lock().unwrap() = Some(resolve_people(&link_maps, threads, &context.longs));
        } else {
            parse_chunk(&context, None, &event_maps, &click_maps, event_chunks[at - 1], state);
        }
    });
    let people = people_slot.into_inner().unwrap().expect("links resolved");
    timer.mark("links || parse events");
    let mut click_chunks = Vec::new();
    chunks_of(Kind::Click, &click_maps, &mut click_chunks);
    click_chunks.sort_by_key(|chunk| std::cmp::Reverse(chunk.end - chunk.start));
    let clicked = run_tasks(threads, click_chunks.len(), new_clicks, |state, at| {
        parse_chunk(&context, Some(&people), &event_maps, &click_maps, click_chunks[at], state)
    });
    let event_parts: Vec<Scatter<EventCopy>> = parsed.into_iter().map(|s| s.events).collect();
    let click_parts: Vec<Scatter<Click>> = clicked.into_iter().map(|s| s.clicks).collect();
    timer.mark("parse clicks");

    // C. dedup per event_id partition.
    let deduped = run_tasks(
        threads,
        dedup_parts,
        || (DedupScratch::default(), Scatter::new(person_parts, survivor_cap)),
        |(scratch, out), p| {
            let pieces: Vec<&[EventCopy]> = event_parts.iter().flat_map(|scatter| scatter.pieces(p)).filter(|s| !s.is_empty()).collect();
            dedup(pieces, &people, scratch, out)
        },
    );
    let person_events: Vec<Scatter<Event>> = deduped.into_iter().map(|(_, out)| out).collect();
    timer.mark("dedup");

    // D. per person partition, heaviest first.
    let mut order: Vec<usize> = (0..person_parts).collect();
    let weight = |q: usize| person_events.iter().map(|scatter| scatter.len(q)).sum::<usize>();
    let weights: Vec<usize> = (0..person_parts).map(weight).collect();
    order.sort_by_key(|&q| std::cmp::Reverse(weights[q]));
    let campaign_count = campaign_rows.len();
    let states = run_tasks(
        threads,
        person_parts,
        || PeopleState {
            scratch: GroupScratch::default(),
            scratch_wide: GroupScratch::default(),
            user_rows: Vec::new(),
            campaign_daily: CampaignCounters::new(campaign_count, context.window),
            session_number: 0,
            win_count: vec![0; campaign_count],
            win_pos: vec![0; campaign_count],
            active: Vec::new(),
        },
        |state, at| {
            let q = order[at];
            let event_pieces: Vec<&[Event]> = person_events.iter().flat_map(|scatter| scatter.pieces(q)).filter(|s| !s.is_empty()).collect();
            let click_pieces: Vec<&[Click]> = click_parts.iter().flat_map(|scatter| scatter.pieces(q)).filter(|s| !s.is_empty()).collect();
            // Every event ts lies in [window_start, window_end): u32 offsets fit when the window does.
            if context.window.1 - context.window.0 <= i64::from(u32::MAX) {
                people_partition::<u32>(event_pieces, click_pieces, &context.longs, state, &category_names, context.window.0);
            } else {
                people_partition::<i64>(event_pieces, click_pieces, &context.longs, state, &category_names, context.window.0);
            }
        },
    );
    timer.mark("sessions + attribution");

    // E. output.
    output::prepare(&out)?;
    // The per-thread campaign counters are merged on the campaign_daily writer's thread,
    // in parallel with the user_daily writers, instead of serially before them.
    let mut counters = Vec::with_capacity(states.len());
    let mut user_parts = Vec::new();
    for state in states {
        counters.push(state.campaign_daily);
        if !state.user_rows.is_empty() {
            user_parts.push(state.user_rows);
        }
    }
    let name = |person: Key| context.longs.bytes(person);
    let failures: Mutex<Vec<String>> = Mutex::new(Vec::new());
    std::thread::scope(|scope| {
        for (n, rows) in user_parts.iter().enumerate() {
            let (out, name, category_names, failures) = (&out, &name, &category_names, &failures);
            scope.spawn(move || {
                let path = out.join(format!("user_daily/part-{n:05}.parquet"));
                if let Err(error) = output::write_user_daily_fast(&path, rows, name, category_names) {
                    failures.lock().unwrap().push(error.to_string());
                }
            });
        }
        let (out, failures) = (&out, &failures);
        let campaign_rows = &campaign_rows;
        scope.spawn(move || {
            let mut campaign_daily: FxHashMap<(u32, i32), CampaignCounts> = FxHashMap::default();
            for counter in counters {
                counter.merge_into(&mut campaign_daily);
            }
            let path = out.join("campaign_daily/part-00000.parquet");
            if let Err(error) = output::write_campaign_daily_fast(&path, &campaign_daily, campaign_rows) {
                failures.lock().unwrap().push(error.to_string());
            }
        });
    });
    if let Some(error) = failures.into_inner().unwrap().into_iter().next() {
        return Err(error.into());
    }
    if user_parts.is_empty() {
        output::write_user_daily_fast(&out.join("user_daily/part-00000.parquet"), &[], &name, &category_names)?;
    }
    timer.mark("parquet");
    // Every output is written and closed. Exit here, before this frame's locals drop:
    // returning would unmap the arenas and input mappings one by one on this thread.
    // (v26's parallel MADV_DONTNEED teardown is gone: it released every /proc/self/maps region
    // over 64 MiB, and the kernel merges adjacent mappings with equal flags, so such a region
    // can include glibc heaps or thread stacks still in use; a no-THP build segfaulted
    // (jump to address 0) on the GCP VM. It saved only 0.01-0.035 s.)
    std::process::exit(0);
}


fn main() {
    // Before any thread starts: one malloc arena, grown in 64 MiB steps. glibc otherwise grows
    // each thread's arena heap 4-16 KiB at a time with mprotect (~11k calls per 20M run), each
    // taking mmap_lock for write; on kernels without per-VMA locks (6.1, GCP) every page fault of
    // every thread queues behind them (26.8k context switches vs 7.1k; 20M 1.39 -> 1.12 s on a
    // GCP n2 Cascade Lake VM). Output is unaffected.
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    unsafe {
        libc::mallopt(libc::M_ARENA_MAX, 1);
        libc::mallopt(libc::M_TOP_PAD, 64 << 20);
    }
    match run() {
        // Outputs are written and closed: exit at once instead of freeing gigabytes.
        Ok(()) => std::process::exit(0),
        Err(error) => {
            eprintln!("pipeline: {error}");
            std::process::exit(1);
        }
    }
}
