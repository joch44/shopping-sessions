//! Shopping Sessions pipeline (baseline: single-threaded, straight from rules.md).
//!
//! Env: DATA_DIR, OUTPUT_DIR (also accepted as the two arguments). TIMING=1
//! prints per-stage times to stderr. Nothing is ever printed to stdout.

mod csv;
mod output;
mod ts;

use std::path::{Path, PathBuf};
use std::time::Instant;

use rustc_hash::{FxHashMap, FxHashSet};

use crate::csv::{ascii_digits, data_lines, parse_line};
use crate::ts::DAY_MS;

const SESSION_GAP_MS: i64 = 1_800_000; // a gap OVER this starts a new session
const CLICK_REACH_MS: i64 = 7 * DAY_MS; // a click reaches sessions up to 7 days later
const LATE_MS: i128 = DAY_MS as i128; // ingest_ts - ts OVER this is late
const PRICE_MAX: u64 = 10_000_000;
const NEVER: i64 = i64::MAX; // an unparseable ingest_ts never wins dedup and is late
pub const ORGANIC: u32 = u32::MAX; // campaign index of `organic`

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
                "[timing] {stage:<28} {:8.3}s  (total {:8.3}s)",
                (now - self.last).as_secs_f64(),
                (now - self.start).as_secs_f64()
            );
            self.last = now;
        }
    }
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

/// user_id -> person index; person index -> name (the byte-wise smallest of its IDs).
struct People {
    index: FxHashMap<Box<[u8]>, u32>,
    names: Vec<Box<[u8]>>,
}

impl People {
    fn from_links(data: &Path) -> Self {
        // Intern every ID in a surviving link line; union-find with path halving, union by size.
        let mut ids: FxHashMap<Box<[u8]>, u32> = FxHashMap::default();
        let mut id_names: Vec<Box<[u8]>> = Vec::new();
        let mut parent: Vec<u32> = Vec::new();
        let mut size: Vec<u32> = Vec::new();
        let mut intern = |id: &[u8], ids: &mut FxHashMap<Box<[u8]>, u32>, parent: &mut Vec<u32>, size: &mut Vec<u32>| {
            if let Some(&at) = ids.get(id) {
                return at;
            }
            let at = parent.len() as u32;
            ids.insert(id.into(), at);
            id_names.push(id.into());
            parent.push(at);
            size.push(1);
            at
        };
        fn find(parent: &mut [u32], mut node: u32) -> u32 {
            while parent[node as usize] != node {
                let grand = parent[parent[node as usize] as usize];
                parent[node as usize] = grand;
                node = grand;
            }
            node
        }
        for path in csv_files(&data.join("links")) {
            let Ok(bytes) = std::fs::read(&path) else { continue };
            let mut fields = [None; 2];
            for line in data_lines(&bytes) {
                if !parse_line(line, &mut fields) {
                    continue;
                }
                let (Some(a), Some(b)) = (fields[0], fields[1]) else { continue }; // empty side: drop
                let a = intern(a, &mut ids, &mut parent, &mut size);
                let b = intern(b, &mut ids, &mut parent, &mut size);
                let (mut ra, mut rb) = (find(&mut parent, a), find(&mut parent, b));
                if ra == rb {
                    continue;
                }
                if size[ra as usize] < size[rb as usize] {
                    std::mem::swap(&mut ra, &mut rb);
                }
                parent[rb as usize] = ra;
                size[ra as usize] += size[rb as usize];
            }
        }
        // Name each person by the byte-wise smallest ID.
        let count = parent.len();
        let mut smallest: Vec<u32> = (0..count as u32).collect();
        for node in 0..count as u32 {
            let root = find(&mut parent, node) as usize;
            if id_names[node as usize] < id_names[smallest[root] as usize] {
                smallest[root] = node;
            }
        }
        let mut person_of_root: Vec<u32> = vec![u32::MAX; count];
        let mut names = Vec::new();
        let mut index = FxHashMap::default();
        for node in 0..count {
            let root = find(&mut parent, node as u32) as usize;
            if person_of_root[root] == u32::MAX {
                person_of_root[root] = names.len() as u32;
                names.push(id_names[smallest[root] as usize].clone());
            }
            index.insert(id_names[node].clone(), person_of_root[root]);
        }
        Self { index, names }
    }

    /// The person of a user_id; an ID in no link line is a person on its own.
    fn of(&mut self, user: &[u8]) -> u32 {
        if let Some(&person) = self.index.get(user) {
            return person;
        }
        let person = self.names.len() as u32;
        self.names.push(user.into());
        self.index.insert(user.into(), person);
        person
    }
}

// ---------------------------------------------------------------- events (rules.md "Events, in this order")

/// One parsed copy of an event, reduced to what the later steps need.
#[derive(Clone, Copy)]
struct Copy {
    ingest: i64,
    valid: bool,
    person: u32,
    ts: i64,
    price: u32,
    category: u32,
}

pub struct Event {
    pub person: u32,
    pub ts: i64,
    pub price: u32, // 0 unless a purchase (valid purchase prices are 1..=10,000,000)
    pub category: u32,
}

struct Context {
    window: (i64, i64),
    products: FxHashMap<Vec<u8>, u32>,
    unknown_category: u32,
    campaigns: FxHashMap<Vec<u8>, u32>,
}

/// Step 3 (validate) and step 4 (late) for one copy; `person` is resolved only for valid copies.
fn judge(context: &Context, people: &mut People, fields: &[Option<&[u8]>; 7], ingest: i64) -> Copy {
    let invalid = Copy { ingest, valid: false, person: 0, ts: 0, price: 0, category: 0 };
    let [_, user, ts, _, kind, product, price] = *fields;
    let (Some(user), Some(product), Some(kind)) = (user, product, kind) else { return invalid };
    let purchase = match kind {
        b"purchase" => true,
        b"view" | b"cart" => false,
        _ => return invalid,
    };
    let Some(ts) = ts.and_then(ts::parse) else { return invalid };
    if ts < context.window.0 || ts >= context.window.1 {
        return invalid;
    }
    let price = if purchase {
        let Some(price) = price.filter(|p| ascii_digits(p)) else { return invalid };
        let value = price.iter().try_fold(0u64, |acc, &b| {
            let next = acc * 10 + u64::from(b - b'0');
            (next <= PRICE_MAX).then_some(next)
        });
        match value {
            Some(value) if value >= 1 => value as u32,
            _ => return invalid,
        }
    } else {
        0
    };
    if ingest == NEVER || i128::from(ingest) - i128::from(ts) > LATE_MS {
        return invalid;
    }
    let category = *context.products.get(product).unwrap_or(&context.unknown_category);
    Copy { ingest, valid: true, person: people.of(user), ts, price, category }
}

fn read_events(data: &Path, context: &Context, people: &mut People) -> Vec<Event> {
    // Step 2: dedup on event_id, keeping the copy with the smallest ingest_ts
    // (ties: copies are identical, the first seen stays). Validation is per copy
    // but only the kept copy's verdict counts.
    let mut kept: FxHashMap<Box<[u8]>, Copy> = FxHashMap::default();
    for path in csv_files(&data.join("events")) {
        let Ok(bytes) = std::fs::read(&path) else { continue };
        let mut fields = [None; 7];
        for line in data_lines(&bytes) {
            if !parse_line(line, &mut fields) {
                continue; // step 1: malformed
            }
            let Some(event_id) = fields[0] else { continue }; // step 1: empty event_id
            let ingest = fields[3].and_then(ts::parse).unwrap_or(NEVER);
            match kept.get_mut(event_id) {
                Some(current) if ingest >= current.ingest => {}
                Some(current) => *current = judge(context, people, &fields, ingest),
                None => {
                    let copy = judge(context, people, &fields, ingest);
                    kept.insert(event_id.into(), copy);
                }
            }
        }
    }
    kept.into_values()
        .filter(|copy| copy.valid)
        .map(|copy| Event { person: copy.person, ts: copy.ts, price: copy.price, category: copy.category })
        .collect()
}

// ---------------------------------------------------------------- clicks (rules.md "Clicks")

pub struct Click {
    pub person: u32,
    pub ts: i64,
    pub click_id: Box<[u8]>,
    pub campaign: u32,
}

fn read_clicks(data: &Path, context: &Context, people: &mut People) -> Vec<Click> {
    let mut seen: FxHashSet<Box<[u8]>> = FxHashSet::default();
    let mut clicks = Vec::new();
    let (start, end) = (context.window.0.saturating_sub(CLICK_REACH_MS), context.window.1);
    for path in csv_files(&data.join("clicks")) {
        let Ok(bytes) = std::fs::read(&path) else { continue };
        let mut fields = [None; 4];
        for line in data_lines(&bytes) {
            if !parse_line(line, &mut fields) {
                continue;
            }
            let [Some(click_id), user, campaign, ts] = fields else { continue };
            // Step 2: copies have identical values, keep the first.
            if !seen.insert(click_id.into()) {
                continue;
            }
            // Step 3: validate.
            let (Some(user), Some(campaign)) = (user, campaign) else { continue };
            let Some(&campaign) = context.campaigns.get(campaign) else { continue };
            let Some(ts) = ts.and_then(ts::parse) else { continue };
            if ts < start || ts >= end {
                continue;
            }
            clicks.push(Click { person: people.of(user), ts, click_id: click_id.into(), campaign });
        }
    }
    clicks
}

// ---------------------------------------------------------------- sessions, attribution, aggregation

pub struct UserRow {
    pub person: u32,
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

struct DayAccumulator {
    person: u32,
    day: i32,
    sessions: u64,
    events: u64,
    revenue: u64,
    by_category: Vec<(u32, u64)>,
}

fn flush(acc: &mut Option<DayAccumulator>, rows: &mut Vec<UserRow>, category_names: &[Vec<u8>]) {
    if let Some(day) = acc.take() {
        // Highest summed purchase price; a tie goes to the byte-wise smallest name.
        let top_category = day
            .by_category
            .iter()
            .max_by(|a, b| a.1.cmp(&b.1).then_with(|| category_names[b.0 as usize].cmp(&category_names[a.0 as usize])))
            .map(|&(category, _)| category);
        rows.push(UserRow {
            person: day.person,
            day: day.day,
            sessions: day.sessions,
            events: day.events,
            revenue: day.revenue,
            top_category,
        });
    }
}

fn sessionize(
    mut events: Vec<Event>,
    mut clicks: Vec<Click>,
    campaign_count: usize,
    category_names: &[Vec<u8>],
) -> (Vec<UserRow>, FxHashMap<(u32, i32), CampaignCounts>) {
    events.sort_unstable_by_key(|event| (event.person, event.ts));
    clicks.sort_unstable_by(|a, b| (a.person, a.ts, &a.click_id).cmp(&(b.person, b.ts, &b.click_id)));

    let mut user_rows = Vec::new();
    let mut campaign_daily: FxHashMap<(u32, i32), CampaignCounts> = FxHashMap::default();
    let mut assist_stamp = vec![u64::MAX; campaign_count];
    let mut session_number: u64 = 0;
    let mut click_at = 0;
    let mut at = 0;
    while at < events.len() {
        let person = events[at].person;
        let mut end = at;
        while end < events.len() && events[end].person == person {
            end += 1;
        }
        let person_events = &events[at..end];
        while click_at < clicks.len() && clicks[click_at].person < person {
            click_at += 1;
        }
        let click_start = click_at;
        while click_at < clicks.len() && clicks[click_at].person == person {
            click_at += 1;
        }
        let person_clicks = &clicks[click_start..click_at];

        let mut day_acc: Option<DayAccumulator> = None;
        let mut first = 0;
        while first < person_events.len() {
            // Events step 5: a gap OVER 30 min starts a new session (equal ts: gap 0).
            let mut last = first + 1;
            while last < person_events.len() && person_events[last].ts - person_events[last - 1].ts <= SESSION_GAP_MS {
                last += 1;
            }
            let session = &person_events[first..last];
            first = last;
            session_number += 1;

            let start = session[0].ts;
            let day = start.div_euclid(DAY_MS) as i32; // step 6: UTC date of the start
            let revenue: u64 = session.iter().map(|event| u64::from(event.price)).sum();
            let purchased = session.iter().any(|event| event.price > 0);

            if day_acc.as_ref().is_some_and(|acc| acc.day != day) {
                flush(&mut day_acc, &mut user_rows, category_names);
            }
            let acc = day_acc.get_or_insert_with(|| DayAccumulator {
                person,
                day,
                sessions: 0,
                events: 0,
                revenue: 0,
                by_category: Vec::new(),
            });
            acc.sessions += 1;
            acc.events += session.len() as u64;
            acc.revenue += revenue;
            for event in session.iter().filter(|event| event.price > 0) {
                match acc.by_category.iter_mut().find(|(c, _)| *c == event.category) {
                    Some(entry) => entry.1 += u64::from(event.price),
                    None => acc.by_category.push((event.category, u64::from(event.price))),
                }
            }

            // Clicks step 4: qualifying clicks: start - 7 d <= ts <= start. The latest
            // wins; on a tie the smallest click_id (clicks are sorted by (ts, click_id)).
            let low = person_clicks.partition_point(|click| click.ts < start - CLICK_REACH_MS);
            let high = person_clicks.partition_point(|click| click.ts <= start);
            let credited = if high > low {
                let latest = person_clicks[high - 1].ts;
                let first_latest = low + person_clicks[low..high].partition_point(|click| click.ts < latest);
                // Clicks step 7: every campaign among the same clicks assists once.
                for click in &person_clicks[low..high] {
                    let stamp = &mut assist_stamp[click.campaign as usize];
                    if *stamp != session_number {
                        *stamp = session_number;
                        campaign_daily.entry((click.campaign, day)).or_default().assisted += 1;
                    }
                }
                person_clicks[first_latest].campaign
            } else {
                ORGANIC // step 5
            };
            let row = campaign_daily.entry((credited, day)).or_default();
            row.sessions += 1;
            row.purchasing += u64::from(purchased);
            row.revenue += revenue;
        }
        flush(&mut day_acc, &mut user_rows, category_names);
        at = end;
    }
    (user_rows, campaign_daily)
}

// ---------------------------------------------------------------- main

fn run() -> Result<()> {
    let mut timer = Timer::new();
    let args: Vec<String> = std::env::args().collect();
    let data = PathBuf::from(std::env::var("DATA_DIR").ok().or_else(|| args.get(1).cloned()).unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").ok().or_else(|| args.get(2).cloned()).unwrap_or("/output".into()));

    let window = load_window(&data)?;
    let product_rows = load_lookup(&data.join("products.csv"));
    let mut category_names: Vec<Vec<u8>> = Vec::new();
    let mut category_index: FxHashMap<Vec<u8>, u32> = FxHashMap::default();
    let mut products = FxHashMap::default();
    for (product, category) in product_rows {
        let next = category_names.len() as u32;
        let index = *category_index.entry(category.clone()).or_insert(next);
        if index == next {
            category_names.push(category);
        }
        products.insert(product, index);
    }
    // `unknown`: a name no category in the file uses (rules.md), so a fresh index.
    let unknown_category = category_names.len() as u32;
    category_names.push(b"unknown".to_vec());
    let campaign_rows = load_lookup(&data.join("campaigns.csv"));
    let campaigns: FxHashMap<Vec<u8>, u32> =
        campaign_rows.iter().enumerate().map(|(index, (id, _))| (id.clone(), index as u32)).collect();
    let context = Context { window, products, unknown_category, campaigns };
    timer.mark("lookups");

    let mut people = People::from_links(&data);
    timer.mark("links");
    let events = read_events(&data, &context, &mut people);
    timer.mark("events: parse+dedup+validate");
    let clicks = read_clicks(&data, &context, &mut people);
    timer.mark("clicks");
    let (user_rows, campaign_daily) = sessionize(events, clicks, campaign_rows.len(), &category_names);
    timer.mark("sessions+attribution");

    output::prepare(&out)?;
    output::write_user_daily(&out.join("user_daily/part-00000.parquet"), &user_rows, &people.names, &category_names)?;
    output::write_campaign_daily(&out.join("campaign_daily/part-00000.parquet"), &campaign_daily, &campaign_rows)?;
    timer.mark("parquet");
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("pipeline: {error}");
        std::process::exit(1);
    }
}
