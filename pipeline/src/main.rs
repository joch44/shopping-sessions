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

const DEDUP_PARTITIONS: usize = 1024; // by event_id; small enough per partition to stay in L2
const PERSON_PARTITIONS: usize = 1024; // by person; a partition's events + group table fit L2
const CHUNK_BYTES: usize = 16 << 20;

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
    let chunk_bytes = chunk_bytes();
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
/// Runs on one thread while the others parse events (links are ~1% of the input).
fn resolve_people(maps: &[Option<Mmap>], longs: &LongIds) -> People {
    let mut pairs: Vec<(Key, Key)> = Vec::new();
    for map in maps {
        let data = bytes_of(map);
        for_each_record::<2>(data, 0, data.len(), |fields, _| {
            if let (Some(a), Some(b)) = (fields[0], fields[1]) {
                pairs.push((key_of(a, Some(longs)), key_of(b, Some(longs))));
            }
        });
    }

    let mut index: FxHashMap<Key, u32> = FxHashMap::default();
    let mut keys: Vec<Key> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    let mut size: Vec<u32> = Vec::new();
    fn find(parent: &mut [u32], mut node: u32) -> u32 {
        while parent[node as usize] != node {
            let grand = parent[parent[node as usize] as usize];
            parent[node as usize] = grand;
            node = grand;
        }
        node
    }
    for (a, b) in pairs {
        let mut intern = |k: Key| {
            *index.entry(k).or_insert_with(|| {
                keys.push(k);
                parent.push(parent.len() as u32);
                size.push(1);
                (keys.len() - 1) as u32
            })
        };
        let (a, b) = (intern(a), intern(b));
        let (mut ra, mut rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra != rb {
            if size[ra as usize] < size[rb as usize] {
                std::mem::swap(&mut ra, &mut rb);
            }
            parent[rb as usize] = ra;
            size[ra as usize] += size[rb as usize];
        }
    }
    let mut smallest: Vec<Key> = keys.clone();
    for node in 0..keys.len() {
        let root = find(&mut parent, node as u32) as usize;
        if key::cmp(keys[node], smallest[root], longs).is_lt() {
            smallest[root] = keys[node];
        }
    }
    let persons: Vec<Key> = (0..keys.len()).map(|node| smallest[find(&mut parent, node as u32) as usize]).collect();
    People::new(keys.iter().copied().zip(persons))
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
    (mix(id) as usize) & (DEDUP_PARTITIONS - 1)
}

#[inline]
fn person_partition(person: Key) -> usize {
    (mix(person) as usize) & (PERSON_PARTITIONS - 1)
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
                let ingest = fields[3].and_then(|v| ts::parse_cached(v, &mut state.dates)).unwrap_or(NEVER);
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

const CLICK_BATCH: usize = 32;

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
        // Within one partition the low hash bits are equal: index the table by middle bits.
        let mut slot = (h >> 10) as usize & mask;
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

/// One event within a person's group: the person is the group's, so only what the session
/// sweep reads (16 bytes, half an Event) is moved by the per-person sort.
#[derive(Clone, Copy)]
pub struct SessionEvent {
    pub ts: i64,
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
    scratch: GroupScratch,
    user_rows: Vec<UserRow>,
    campaign_daily: CampaignCounters,
    assist_stamp: Vec<u64>,
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
fn person_sessions(person: Key, events: &[SessionEvent], clicks: &[SessionClick], state: &mut PeopleState, category_names: &[Vec<u8>]) {
    let mut day_acc: Option<DayAccumulator> = None;
    let mut first = 0;
    while first < events.len() {
        // Events step 5: a gap OVER 30 min starts a new session (equal ts: gap 0).
        let mut last = first + 1;
        while last < events.len() && events[last].ts - events[last - 1].ts <= SESSION_GAP_MS {
            last += 1;
        }
        let session = &events[first..last];
        first = last;
        state.session_number += 1;

        let start = session[0].ts;
        let day = start.div_euclid(DAY_MS) as i32; // step 6: UTC date of the start
        let revenue: u64 = session.iter().map(|event| u64::from(event.price)).sum();
        let purchased = session.iter().any(|event| event.price > 0);

        if day_acc.as_ref().is_some_and(|acc| acc.day != day) {
            flush(person, &mut day_acc, &mut state.user_rows, category_names);
        }
        let acc = day_acc.get_or_insert_with(|| DayAccumulator { day, sessions: 0, events: 0, revenue: 0, by_category: Vec::new() });
        acc.sessions += 1;
        acc.events += session.len() as u64;
        acc.revenue += revenue;
        for event in session.iter().filter(|event| event.price > 0) {
            match acc.by_category.iter_mut().find(|(c, _)| *c == event.category) {
                Some(entry) => entry.1 += u64::from(event.price),
                None => acc.by_category.push((event.category, u64::from(event.price))),
            }
        }

        // Clicks step 4: qualifying clicks: start - 7 d <= ts <= start. The latest wins; on a
        // tie the byte-wise smallest click_id (clicks are sorted by (ts, click_id)).
        let low = clicks.partition_point(|click| click.ts < start - CLICK_REACH_MS);
        let high = clicks.partition_point(|click| click.ts <= start);
        let credited = if high > low {
            let latest = clicks[high - 1].ts;
            let first_latest = low + clicks[low..high].partition_point(|click| click.ts < latest);
            // Clicks step 7: every campaign among the same clicks assists once.
            for click in &clicks[low..high] {
                let stamp = &mut state.assist_stamp[click.campaign as usize];
                if *stamp != state.session_number {
                    *stamp = state.session_number;
                    state.campaign_daily.at(click.campaign, day).assisted += 1;
                }
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
}

/// Groups one person partition by person (hash + counting sort, no full sort), then runs
/// each person's events (sorted by ts) and clicks (sorted by ts, click_id) through
/// `person_sessions`. Clicks of a person with no surviving events can credit nothing.
/// Reads the per-thread pieces in place; all temporary vectors live in `state.scratch`
/// and are reused across partitions.
fn people_partition(
    event_pieces: Vec<&[Event]>,
    click_pieces: Vec<&[Click]>,
    longs: &LongIds,
    state: &mut PeopleState,
    category_names: &[Vec<u8>],
) {
    let total: usize = event_pieces.iter().map(|piece| piece.len()).sum();
    if total == 0 {
        return;
    }
    let mut scratch = std::mem::take(&mut state.scratch);
    let GroupScratch { table, group_keys, event_group, event_start, cursor, grouped, click_group, click_start, grouped_clicks } =
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
    event_group.clear();
    for event in event_pieces.iter().copied().flatten() {
        let group = match find(table, group_keys, event.person) {
            (_, Some(group)) => group,
            (slot, None) => {
                table[slot] = group_keys.len() as u32;
                group_keys.push(event.person);
                (group_keys.len() - 1) as u32
            }
        };
        event_group.push(group);
    }
    let groups = group_keys.len();
    // Counting sort of events, then of clicks, into group order.
    event_start.clear();
    event_start.resize(groups + 1, 0);
    for &group in event_group.iter() {
        event_start[group as usize + 1] += 1;
    }
    for g in 0..groups {
        event_start[g + 1] += event_start[g];
    }
    cursor.clear();
    cursor.extend_from_slice(event_start);
    grouped.clear();
    grouped.reserve(total);
    let spare = grouped.spare_capacity_mut();
    for (event, &group) in event_pieces.iter().copied().flatten().zip(event_group.iter()) {
        let at = &mut cursor[group as usize];
        spare[*at as usize].write(SessionEvent { ts: event.ts, price: event.price, category: event.category });
        *at += 1;
    }
    // SAFETY: the counting sort wrote every slot in 0..total exactly once.
    unsafe { grouped.set_len(total) };
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
        let person_events = &mut grouped[event_start[g] as usize..event_start[g + 1] as usize];
        person_events.sort_unstable_by_key(|event| event.ts);
        let person_clicks = &mut grouped_clicks[click_start[g] as usize..click_start[g + 1] as usize];
        person_clicks.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| key::cmp(a.click_id, b.click_id, longs)));
        person_sessions(group_keys[g], person_events, person_clicks, state, category_names);
    }
    state.scratch = scratch;
}

/// Temporary vectors of `people_partition`, kept per thread and reused.
#[derive(Default)]
struct GroupScratch {
    table: Vec<u32>,
    group_keys: Vec<Key>,
    event_group: Vec<u32>,
    event_start: Vec<u32>,
    cursor: Vec<u32>,
    grouped: Vec<SessionEvent>,
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
    let window = load_window(&data)?;
    let mut category_names: Vec<Vec<u8>> = Vec::new();
    let mut category_index: FxHashMap<Vec<u8>, u32> = FxHashMap::default();
    let mut products = FxHashMap::default();
    for (product, category) in load_lookup(&data.join("products.csv")) {
        let next = category_names.len() as u32;
        let index = *category_index.entry(category.clone()).or_insert(next);
        if index == next {
            category_names.push(category);
        }
        products.insert(key_of(&product, None), index);
    }
    // `unknown`: a name no category in the file uses (rules.md), so a fresh index.
    let unknown_category = category_names.len() as u32;
    category_names.push(b"unknown".to_vec());
    let campaign_rows = load_lookup(&data.join("campaigns.csv"));
    let campaigns: FxHashMap<Key, u32> =
        campaign_rows.iter().enumerate().map(|(index, (id, _))| (key_of(id, None), index as u32)).collect();
    let event_maps = map_folder(&data.join("events"))?;
    let click_maps = map_folder(&data.join("clicks"))?;
    let link_maps = map_folder(&data.join("links"))?;
    timer.mark("lookups + mmap");

    let context = Context { window, products, unknown_category, campaigns, longs: LongIds::default() };

    // A+B. Task 0 resolves links on one thread while the other threads parse events
    // (largest chunks first); clicks need people, so they are parsed after.
    let mut event_chunks = Vec::new();
    chunks_of(Kind::Event, &event_maps, &mut event_chunks);
    event_chunks.sort_by_key(|chunk| std::cmp::Reverse(chunk.end - chunk.start));
    let people_slot: Mutex<Option<People>> = Mutex::new(None);
    // Pre-size each thread's partition buffers from the input size (~77 bytes per event
    // line, ~47 per click line, 15% slack), so they rarely regrow; a wrong guess only costs speed.
    let total_bytes = |maps: &[Option<Mmap>]| maps.iter().map(|m| bytes_of(m).len()).sum::<usize>();
    let per_buffer = |bytes: usize, line: usize, parts: usize| bytes / line * 115 / 100 / threads / parts;
    let event_cap = per_buffer(total_bytes(&event_maps), 77, DEDUP_PARTITIONS);
    let click_cap = per_buffer(total_bytes(&click_maps), 47, PERSON_PARTITIONS);
    let survivor_cap = per_buffer(total_bytes(&event_maps), 77, PERSON_PARTITIONS);
    let new_events = || ParseState {
        events: Scatter::new(DEDUP_PARTITIONS, event_cap),
        clicks: Scatter::new(PERSON_PARTITIONS, 0),
        dates: ts::DateCache::default(),
    };
    let new_clicks = || ParseState {
        events: Scatter::new(DEDUP_PARTITIONS, 0),
        clicks: Scatter::new(PERSON_PARTITIONS, click_cap),
        dates: ts::DateCache::default(),
    };
    let parsed = run_tasks(threads, event_chunks.len() + 1, new_events, |state, at| {
        if at == 0 {
            *people_slot.lock().unwrap() = Some(resolve_people(&link_maps, &context.longs));
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
        DEDUP_PARTITIONS,
        || (DedupScratch::default(), Scatter::new(PERSON_PARTITIONS, survivor_cap)),
        |(scratch, out), p| {
            let pieces: Vec<&[EventCopy]> = event_parts.iter().flat_map(|scatter| scatter.pieces(p)).filter(|s| !s.is_empty()).collect();
            dedup(pieces, &people, scratch, out)
        },
    );
    let person_events: Vec<Scatter<Event>> = deduped.into_iter().map(|(_, out)| out).collect();
    timer.mark("dedup");

    // D. per person partition, heaviest first.
    let mut order: Vec<usize> = (0..PERSON_PARTITIONS).collect();
    let weight = |q: usize| person_events.iter().map(|scatter| scatter.len(q)).sum::<usize>();
    let weights: Vec<usize> = (0..PERSON_PARTITIONS).map(weight).collect();
    order.sort_by_key(|&q| std::cmp::Reverse(weights[q]));
    let campaign_count = campaign_rows.len();
    let states = run_tasks(
        threads,
        PERSON_PARTITIONS,
        || PeopleState {
            scratch: GroupScratch::default(),
            user_rows: Vec::new(),
            campaign_daily: CampaignCounters::new(campaign_count, context.window),
            assist_stamp: vec![u64::MAX; campaign_count],
            session_number: 0,
        },
        |state, at| {
            let q = order[at];
            let event_pieces: Vec<&[Event]> = person_events.iter().flat_map(|scatter| scatter.pieces(q)).filter(|s| !s.is_empty()).collect();
            let click_pieces: Vec<&[Click]> = click_parts.iter().flat_map(|scatter| scatter.pieces(q)).filter(|s| !s.is_empty()).collect();
            people_partition(event_pieces, click_pieces, &context.longs, state, &category_names);
        },
    );
    timer.mark("sessions + attribution");

    // E. output.
    output::prepare(&out)?;
    let mut campaign_daily: FxHashMap<(u32, i32), CampaignCounts> = FxHashMap::default();
    let mut user_parts = Vec::new();
    for state in states {
        state.campaign_daily.merge_into(&mut campaign_daily);
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
        let campaign_daily = &campaign_daily;
        let campaign_rows = &campaign_rows;
        scope.spawn(move || {
            let path = out.join("campaign_daily/part-00000.parquet");
            if let Err(error) = output::write_campaign_daily_fast(&path, campaign_daily, campaign_rows) {
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
    Ok(())
}

fn main() {
    match run() {
        // Outputs are written and closed: exit at once instead of freeing gigabytes.
        Ok(()) => std::process::exit(0),
        Err(error) => {
            eprintln!("pipeline: {error}");
            std::process::exit(1);
        }
    }
}
