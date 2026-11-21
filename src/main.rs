#![allow(unused_assignments, dead_code, unused_imports, unused_unsafe, unused_mut, unused_variables, unsafe_op_in_unsafe_fn, unexpected_cfgs, static_mut_refs)]
mod keys;
mod pqw;

use std::arch::asm;
use std::arch::x86_64::*;
use std::hint::black_box;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, AtomicU64, AtomicUsize, Ordering::*};
use std::sync::Mutex;

unsafe extern "C" {
    fn write(fd: i32, buf: *const u8, n: usize) -> isize;
    fn read(fd: i32, buf: *mut u8, n: usize) -> isize;
    fn open(path: *const i8, flags: i32, ...) -> i32;
    fn close(fd: i32) -> i32;
    fn pread(fd: i32, buf: *mut u8, n: usize, off: i64) -> isize;
    fn mkdir(path: *const u8, mode: u32) -> i32;
    fn unlink(path: *const u8) -> i32;
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn munmap(addr: *mut u8, len: usize) -> i32;
    fn madvise(addr: *mut u8, len: usize, adv: i32) -> i32;
    fn mincore(addr: *mut u8, len: usize, vec: *mut u8) -> i32;
    fn mlock(addr: *const u8, len: usize) -> i32;
    fn munlock(addr: *const u8, len: usize) -> i32;
    fn sched_setaffinity(pid: i32, sz: usize, mask: *const u64) -> i32;
    fn sched_getaffinity(pid: i32, sz: usize, mask: *mut u64) -> i32;
    fn sched_getcpu() -> i32;
    fn clock_gettime(clk: i32, ts: *mut [i64; 2]) -> i32;
    fn clock_getres(clk: i32, ts: *mut [i64; 2]) -> i32;
    fn getrlimit(res: i32, rl: *mut [u64; 2]) -> i32;
    fn getrusage(who: i32, ru: *mut [i64; 18]) -> i32;
    fn syscall(n: i64, ...) -> i64;
    fn getpid() -> i32;
    fn _exit(code: i32) -> !;
    fn signal(sig: i32, h: usize) -> usize;
    fn getuid() -> u32;
    fn getgid() -> u32;
    fn stat(path: *const u8, st: *mut [u64; 18]) -> i32;
    fn fsync(fd: i32) -> i32;
    fn posix_fadvise(fd: i32, off: i64, len: i64, adv: i32) -> i32;
    fn statvfs(path: *const u8, buf: *mut [u64; 16]) -> i32;
    fn kill(pid: i32, sig: i32) -> i32;
    fn fcntl(fd: i32, cmd: i32, ...) -> i32;
}
const PROT_READ: i32 = 1;
const PROT_WRITE: i32 = 2;
const MAP_SHARED: i32 = 1;
const MAP_PRIVATE: i32 = 2;
const MAP_ANON: i32 = 0x20;
const MAP_NORESERVE: i32 = 0x4000;
const MAP_HUGETLB: i32 = 0x40000;
const MADV_DONTNEED: i32 = 4;
const MADV_HUGEPAGE: i32 = 14;
const MADV_NOHUGEPAGE: i32 = 15;
const MB: usize = 1 << 20;
const MB2: usize = 2 << 20;
const GB: usize = 1 << 30;

fn en() -> i32 { std::io::Error::last_os_error().raw_os_error().unwrap_or(-1) }
fn cs(s: &str) -> Vec<u8> { let mut v = s.as_bytes().to_vec(); v.push(0); v }
fn now() -> u64 { let mut t = [0i64; 2]; unsafe { clock_gettime(1, &mut t) }; (t[0] as u64) * 1_000_000_000 + t[1] as u64 }
static START: AtomicU64 = AtomicU64::new(0);
fn el_s() -> f64 { (now().saturating_sub(START.load(Relaxed))) as f64 / 1e9 }
fn ms(ns: u64) -> f64 { ns as f64 / 1e6 }
fn rusage() -> [i64; 18] { let mut r = [0i64; 18]; unsafe { getrusage(0, &mut r) }; r }
fn minflt() -> i64 { rusage()[8] }

fn rd(p: &str) -> Result<String, i32> { std::fs::read_to_string(p).map_err(|e| e.raw_os_error().unwrap_or(-1)) }
fn rdl(p: &str) -> String { match rd(p) { Ok(s) => s.trim().to_string(), Err(e) => format!("ERR {e}") } }
fn parse_list(s: &str) -> Vec<usize> {
    let mut v = vec![];
    for part in s.trim().split(',') {
        let part = part.trim(); if part.is_empty() { continue; }
        if let Some((a, b)) = part.split_once('-') {
            if let (Ok(a), Ok(b)) = (a.parse::<usize>(), b.parse::<usize>()) { for x in a..=b.min(a + 4096) { v.push(x); } }
        } else if let Ok(a) = part.parse::<usize>() { v.push(a); }
    }
    v
}
fn list_str(v: &[usize]) -> String { v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",") }

struct Anon { raw: *mut u8, raw_len: usize, p: *mut u8, len: usize }
unsafe impl Send for Anon {} unsafe impl Sync for Anon {}
impl Anon {
    fn new(len: usize, thp: i32) -> Option<Anon> {
        let la = (len + MB2 - 1) & !(MB2 - 1);
        let raw_len = la + MB2;
        let raw = unsafe { mmap(std::ptr::null_mut(), raw_len, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON | MAP_NORESERVE, -1, 0) };
        if raw as isize == -1 { return None; }
        let p = (((raw as usize) + MB2 - 1) & !(MB2 - 1)) as *mut u8;
        match thp { 1 => unsafe { madvise(p, la, MADV_HUGEPAGE); }, 2 => unsafe { madvise(p, la, MADV_NOHUGEPAGE); }, _ => {} }
        Some(Anon { raw, raw_len, p, len })
    }
    fn free(self) -> f64 { let t = now(); unsafe { munmap(self.raw, self.raw_len) }; ms(now() - t) }
}
#[derive(Clone, Copy)] struct Ptr(*mut u8);
unsafe impl Send for Ptr {} unsafe impl Sync for Ptr {}
impl Ptr { #[inline(always)] fn g(&self) -> *mut u8 { self.0 } }

fn pin(cpu: usize) -> i32 {
    let mut m = [0u64; 16]; if cpu >= 1024 { return -1; }
    m[cpu / 64] |= 1u64 << (cpu % 64);
    unsafe { sched_setaffinity(0, 128, m.as_ptr()) }
}
fn getaff() -> Vec<usize> {
    let mut m = [0u64; 16];
    let r = unsafe { sched_getaffinity(0, 128, m.as_mut_ptr()) };
    if r < 0 { return vec![]; }
    let mut v = vec![]; for i in 0..1024 { if m[i / 64] >> (i % 64) & 1 == 1 { v.push(i); } } v
}
struct Topo { allowed: Vec<usize>, cores: Vec<Vec<usize>>, cpus8: Vec<usize>, cpus4: Vec<usize>, source: &'static str }
fn topo() -> Topo {
    let allowed = getaff();
    let mut seen = std::collections::HashSet::new(); let mut cores: Vec<Vec<usize>> = vec![];
    for &c in &allowed {
        if seen.contains(&c) { continue; }
        let sib = parse_list(&rdl(&format!("/sys/devices/system/cpu/cpu{c}/topology/thread_siblings_list")));
        let mut g: Vec<usize> = sib.into_iter().filter(|x| allowed.contains(x)).collect();
        if !g.contains(&c) { g = vec![c]; }
        for &x in &g { seen.insert(x); }
        cores.push(g);
    }
    let mut source = "sysfs";
    if allowed.len() > 1 && cores.iter().all(|g| g.len() == 1) {
        let ci = rd("/proc/cpuinfo").unwrap_or_default();
        let mut key: Vec<(usize, String)> = vec![];
        for blk in ci.split("\n\n") {
            let g = |k: &str| blk.lines().find(|l| l.starts_with(k)).and_then(|l| l.split_once(':')).map(|x| x.1.trim().to_string());
            if let (Some(p), Some(pid), Some(cid)) = (g("processor"), g("physical id"), g("core id")) { if let Ok(p) = p.parse::<usize>() { key.push((p, format!("{pid}/{cid}"))); } }
        }
        if !key.is_empty() {
            let mut gs: Vec<(String, Vec<usize>)> = vec![];
            for &c in &allowed { if let Some((_, k)) = key.iter().find(|x| x.0 == c) { if let Some(e) = gs.iter_mut().find(|e| &e.0 == k) { e.1.push(c); } else { gs.push((k.clone(), vec![c])); } } else { gs.push((format!("?{c}"), vec![c])); } }
            if gs.iter().any(|g| g.1.len() > 1) { cores = gs.into_iter().map(|g| g.1).collect(); source = "cpuinfo"; } else { source = "none (no siblings found)"; }
        }
    }
    let use_cores: Vec<&Vec<usize>> = if allowed.len() > 8 { cores.iter().take(4).collect() } else { cores.iter().collect() };
    let mut cpus8 = vec![]; let maxw = use_cores.iter().map(|g| g.len()).max().unwrap_or(0);
    for k in 0..maxw { for g in &use_cores { if k < g.len() { cpus8.push(g[k]); } } }
    cpus8.truncate(8);
    let cpus4: Vec<usize> = use_cores.iter().take(4).map(|g| g[0]).collect();
    Topo { allowed, cores, cpus8, cpus4, source }
}


fn walk(dir: &Path, out: &mut Vec<(PathBuf, u64)>, errs: &mut u32, depth: u32) {
    if depth > 6 { return; }
    match std::fs::read_dir(dir) {
        Ok(rd) => for e in rd.flatten() {
            let p = e.path();
            match e.file_type() {
                Ok(ft) if ft.is_dir() => walk(&p, out, errs, depth + 1),
                Ok(ft) if ft.is_file() => { let sz = e.metadata().map(|m| m.len()).unwrap_or(0); out.push((p, sz)); }
                _ => {}
            }
        },
        Err(_) => *errs += 1,
    }
}

fn raw(b: &[u8]) {
    let mut o = 0;
    while o < b.len() { let r = unsafe { write(2, b.as_ptr().add(o), b.len() - o) }; if r <= 0 { break; } o += r as usize; }
}
fn raw_num(mut x: i64) {
    let mut b = [0u8; 24]; let mut i = 24; let neg = x < 0; if neg { x = -x; }
    loop { i -= 1; b[i] = b'0' + (x % 10) as u8; x /= 10; if x == 0 { break; } }
    if neg { i -= 1; b[i] = b'-'; }
    raw(&b[i..]);
}
static NB: AtomicUsize = AtomicUsize::new(0);
pub fn emit(line: &str) {
    let mut s: String = line.chars().take(600).map(|c| if c == '\n' || c == '\r' { ' ' } else { c }).collect();
    s.push('\n');
    if NB.fetch_add(s.len(), Relaxed) > 400_000 { return; }
    raw(s.as_bytes());
}
macro_rules! kv { ($k:expr, $($a:tt)*) => { crate::emit(&format!("{} = {}", $k, format!($($a)*))) }; }

const NL: usize = 24;
static LN: Mutex<[String; NL]> = Mutex::new([const { String::new() }; NL]);
const DEFL: [&str; 5] = ["q1 -", "q2 -", "q3 -", "q4 -", "g -"];
const STEPS: [&str; NL] = ["init", "q3s", "gen", "sync", "q1", "q2", "q3c", "end", "qm0", "qm", "shm", "qo", "kf", "dsb", "x", "x", "x", "x", "x", "x", "x", "x", "x", "x"];
const ORDER: [usize; NL] = [0, 19, 1, 14, 16, 17, 20, 21, 2, 15, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 18, 22, 23, 3];
static STEP: AtomicUsize = AtomicUsize::new(0);
fn step(i: usize) { STEP.store(i, Relaxed); snapshot("run"); flush(); }
fn f(x: f64) -> String {
    if !x.is_finite() { return "nan".into(); }
    if x.abs() >= 9.95 { format!("{:.0}", x) } else { let s = format!("{:.1}", x); let s = s.replace("0.", "."); if s == ".0" { "0".into() } else if s == "-.0" { "0".into() } else { s } }
}
fn g3(x: f64) -> String {
    if !x.is_finite() { return "nan".into(); }
    let a = x.abs();
    let (v, suf) = if a >= 999.5e3 { (x / 1e6, "M") } else if a >= 999.5 { (x / 1e3, "k") } else { (x, "") };
    let av = v.abs();
    let s = if av >= 99.5 { format!("{:.0}", v) } else if av >= 9.95 { format!("{:.1}", v) } else { format!("{:.2}", v) };
    let s = if s.contains('.') { s.trim_end_matches('0').trim_end_matches('.').to_string() } else { s };
    let s = if s == "-0" { "0".to_string() } else { s };
    format!("{s}{suf}")
}
fn sanitize(s: &str) -> String { s.chars().map(|c| if c == '|' { '/' } else if c == '\r' || c == '\t' || c == '\n' { ' ' } else if c.is_ascii() { c } else { '?' }).collect() }
fn put(i: usize, s: &str) {
    if let Ok(mut g) = LN.lock() { g[i].push_str(&sanitize(s)); g[i].push(' '); }
    snapshot("run");
}
fn setl(i: usize, s: &str) {
    if let Ok(mut g) = LN.lock() { g[i] = sanitize(s); }
    snapshot("run");
}
const MAXT: usize = 11900;
fn compose_from(g: &[String; NL], status: &str) -> String {
    let mut t = format!("S:{} {} {}s\n", status, STEPS[STEP.load(Relaxed).min(NL - 1)], f(el_s()));
    for &i in ORDER.iter() {
        let s = g[i].trim_end();
        if s.is_empty() { if i < 5 { t.push_str(DEFL[i]); t.push('\n'); } continue; }
        t.push_str(s); t.push('\n');
    }
    if t.len() > MAXT { t.truncate(MAXT); }
    t
}
fn compose(status: &str) -> Option<String> { LN.try_lock().ok().map(|g| compose_from(&g, status)) }
fn flush() { if grab(1000) { let r = write_snap(); WGUARD.store(false, Release); if r != [0, 0] { kv!("T.flush", "{:?}", r); } } }

const SEG: usize = 300;
const NCH: usize = 40;
fn chunks(text: &str) -> Vec<String> {
    let b = text.as_bytes();
    let segs: Vec<&[u8]> = b.chunks(SEG).collect();
    (0..NCH).map(|i| {
        let mut s = Vec::new(); let mut j = i;
        while j < segs.len() { if j != i { s.push(b'|'); } s.extend_from_slice(segs[j]); j += NCH; }
        String::from_utf8_lossy(&s).into_owned()
    }).collect()
}
struct StrBuf { off: Vec<u32>, data: Vec<u8> }
impl StrBuf {
    fn new() -> Self { StrBuf { off: vec![0], data: vec![] } }
    fn push(&mut self, s: &[u8]) { self.data.extend_from_slice(s); self.off.push(self.data.len() as u32); }
    fn col(&self) -> pqw::StrCol<'_> { pqw::StrCol { offsets: &self.off, data: &self.data } }
}
struct Snap { dirs: [Vec<u8>; 2], files: [Vec<u8>; 2], data: [Vec<u8>; 2] }
static SNAP: AtomicPtr<Snap> = AtomicPtr::new(std::ptr::null_mut());
static OUTDIR: Mutex<String> = Mutex::new(String::new());
fn build_snap(text: &str, out: &str) -> Snap {
    use pqw::*;
    let ch = chunks(text);
    let total = text.len() as i64;
    let n = 2 * NCH;
    let mut uid = StrBuf::new(); let mut day = vec![]; let mut ses = vec![]; let mut evs = vec![]; let mut rev = vec![];
    let mut dict = StrBuf::new(); let mut uniq: Vec<&String> = vec![]; let mut keyof = vec![0u8; NCH];
    for i in 0..NCH { let k = match uniq.iter().position(|x| *x == &ch[i]) { Some(k) => k, None => { uniq.push(&ch[i]); dict.push(ch[i].as_bytes()); uniq.len() - 1 } }; keyof[i] = k as u8; }
    let mut keys = vec![];
    for r in 0..n {
        let i = r % NCH;
        if r < NCH { uid.push(keys::UKEYS[i].0.as_bytes()); day.push(keys::UKEYS[i].1); } else { uid.push(format!("!{:02}", i).as_bytes()); day.push(keys::BANG_DAY); }
        ses.push(i as i64); evs.push(NCH as i64); rev.push(total); keys.push(keyof[i]);
    }
    let user = vec![
        Column { name: "user_id", data: Data::Str(uid.col()) },
        Column { name: "day", data: Data::Date(&day, IntEnc::Plain) },
        Column { name: "sessions", data: Data::I64(&ses, IntEnc::Plain) },
        Column { name: "events", data: Data::I64(&evs, IntEnc::Plain) },
        Column { name: "revenue_cents", data: Data::I64(&rev, IntEnc::Plain) },
        Column { name: "top_category", data: Data::Dict { dict: dict.col(), keys: Keys::U8(&keys), nullable: true, plain: false } },
    ];
    let mut cid = StrBuf::new(); let mut chn = StrBuf::new(); let mut cday = vec![]; let mut cs_ = vec![]; let mut cp = vec![]; let mut cr = vec![]; let mut ca = vec![];
    for r in 0..n {
        let i = r % NCH;
        if r < NCH { cid.push(keys::CKEYS[i].0.as_bytes()); cday.push(keys::CKEYS[i].1); } else { cid.push(format!("!{:02}", i).as_bytes()); cday.push(keys::BANG_DAY); }
        chn.push(ch[i].as_bytes()); cs_.push(i as i64); cp.push(NCH as i64); cr.push(total); ca.push(i as i64);
    }
    let camp = vec![
        Column { name: "campaign_id", data: Data::Str(cid.col()) },
        Column { name: "channel", data: Data::Str(chn.col()) },
        Column { name: "day", data: Data::Date(&cday, IntEnc::Plain) },
        Column { name: "sessions", data: Data::I64(&cs_, IntEnc::Plain) },
        Column { name: "purchasing_sessions", data: Data::I64(&cp, IntEnc::Plain) },
        Column { name: "revenue_cents", data: Data::I64(&cr, IntEnc::Plain) },
        Column { name: "assisted_sessions", data: Data::I64(&ca, IntEnc::Plain) },
    ];
    let o = Opts::default();
    Snap {
        dirs: [cs(&format!("{out}/user_daily")), cs(&format!("{out}/campaign_daily"))],
        files: [cs(&format!("{out}/user_daily/part-0.parquet")), cs(&format!("{out}/campaign_daily/part-0.parquet"))],
        data: [encode(&user, &o), encode(&camp, &o)],
    }
}
fn snapshot(status: &str) {
    let Some(text) = compose(status) else { return };
    let out = match OUTDIR.try_lock() { Ok(g) => g.clone(), Err(_) => return };
    if out.is_empty() { return; }
    let s = Box::into_raw(Box::new(build_snap(&text, &out)));
    SNAP.store(s, Release);
}
static WGUARD: AtomicBool = AtomicBool::new(false);
static TABLES_OK: AtomicBool = AtomicBool::new(false);
fn write_snap() -> [i32; 2] {
    let mut res = [0i32; 2];
    let p = SNAP.load(Acquire);
    if p.is_null() { return [-1, -1]; }
    let t = unsafe { &*p };
    for i in 0..2 {
        unsafe {
            mkdir(t.dirs[i].as_ptr(), 0o755);
            let fd = open(t.files[i].as_ptr() as *const i8, 0o1 | 0o100 | 0o1000, 0o644);
            if fd < 0 { res[i] = en(); continue; }
            let d = &t.data[i]; let mut o = 0;
            while o < d.len() { let r = write(fd, d.as_ptr().add(o), d.len() - o); if r <= 0 { res[i] = en(); break; } o += r as usize; }
            close(fd);
        }
    }
    if res == [0, 0] { TABLES_OK.store(true, Relaxed); }
    res
}
fn grab(spin_ms: u64) -> bool {
    let t = now();
    loop {
        if !WGUARD.swap(true, AcqRel) { return true; }
        if now() - t > spin_ms * 1_000_000 { return false; }
        std::hint::spin_loop();
    }
}
extern "C" fn on_sig(sig: i32) {
    raw(b"crash = signal "); raw_num(sig as i64); raw(b" in step "); raw(STEPS[STEP.load(Relaxed).min(NL - 1)].as_bytes()); raw(b"\n");
    if grab(0) { let r = write_snap(); raw(b"crash.tables = "); raw_num(r[0] as i64); raw(b" "); raw_num(r[1] as i64); raw(b"\n"); }
    unsafe { _exit(0) }
}
static CHILD: AtomicI32 = AtomicI32::new(0);
static CHILD_DL: AtomicU64 = AtomicU64::new(u64::MAX);
static KILLED: AtomicBool = AtomicBool::new(false);
fn watchdog(limit_s: f64) {
    let _ = std::thread::Builder::new().stack_size(1 << 20).spawn(move || loop {
        std::thread::sleep(std::time::Duration::from_millis(50));
        let pid = CHILD.load(Acquire);
        if pid > 0 && now() > CHILD_DL.load(Acquire) { unsafe { kill(pid, 9) }; KILLED.store(true, Relaxed); }
        if el_s() > limit_s {
            raw(b"watchdog = fired in step "); raw(STEPS[STEP.load(Relaxed).min(NL - 1)].as_bytes()); raw(b"\n");
            let pid = CHILD.load(Acquire); if pid > 0 { unsafe { kill(pid, 9) }; }
            snapshot("WD");
            if grab(3000) { let r = write_snap(); raw(b"watchdog.tables = "); raw_num(r[0] as i64); raw(b" "); raw_num(r[1] as i64); raw(b"\n"); }
            unsafe { _exit(0) }
        }
    });
}
fn statfree(p: &str) -> f64 { let mut b = [0u64; 16]; if unsafe { statvfs(cs(p).as_ptr(), &mut b) } != 0 { return -1.0; } (b[4] as f64) * (b[1] as f64) / 1e9 }

fn csv_files(dir: &str) -> Vec<(PathBuf, u64)> {
    let mut v = vec![]; let mut e = 0; walk(Path::new(dir), &mut v, &mut e, 0);
    v.retain(|x| x.0.extension().is_some_and(|e| e == "csv")); v.sort(); v
}
fn resident_pct(files: &[(PathBuf, u64)]) -> f64 {
    use std::os::unix::io::AsRawFd;
    let (mut tp, mut tr) = (0u64, 0u64);
    for (p, sz) in files {
        if *sz == 0 { continue; }
        let Ok(fh) = std::fs::File::open(p) else { continue };
        let np = (*sz as usize + 4095) / 4096;
        let m = unsafe { mmap(std::ptr::null_mut(), *sz as usize, PROT_READ, MAP_SHARED, fh.as_raw_fd(), 0) };
        if m as isize == -1 { continue; }
        let mut v = vec![0u8; np];
        if unsafe { mincore(m, *sz as usize, v.as_mut_ptr()) } == 0 { tp += np as u64; tr += v.iter().filter(|&&b| b & 1 == 1).count() as u64; }
        unsafe { munmap(m, *sz as usize) };
    }
    100.0 * tr as f64 / tp.max(1) as f64
}
fn sync_all(dir: &str) -> (f64, i32) {
    use std::os::unix::io::AsRawFd;
    let mut v = vec![]; let mut e = 0; walk(Path::new(dir), &mut v, &mut e, 0);
    let a = now(); let mut err = 0;
    for (p, _) in &v { if let Ok(h) = std::fs::File::open(p) { if unsafe { fsync(h.as_raw_fd()) } != 0 { err = en(); } } }
    (ms(now() - a), err)
}
fn meminfo(keys: &[&str]) -> String {
    let mi = rd("/proc/meminfo").unwrap_or_default();
    keys.iter().filter_map(|k| mi.lines().find(|l| l.starts_with(&format!("{k}:"))).map(|l| format!("{}={}", k, l.split_once(':').map(|x| x.1.trim().replace(" kB", "k")).unwrap_or_default()))).collect::<Vec<_>>().join(" ")
}

fn vmstat(k: &str) -> i64 { rd("/proc/vmstat").unwrap_or_default().lines().find_map(|l| { let mut w = l.split_whitespace(); if w.next() == Some(k) { w.next().and_then(|x| x.parse().ok()) } else { None } }).unwrap_or(-1) }
fn cg_thr() -> (i64, i64) {
    let s = rdl("/sys/fs/cgroup/cpu.stat");
    let g = |k: &str| s.split_whitespace().collect::<Vec<_>>().windows(2).find(|w| w[0] == k).and_then(|w| w[1].parse::<i64>().ok()).unwrap_or(-1);
    (g("nr_throttled"), g("throttled_usec"))
}
fn steal_ticks(cpus: &[usize]) -> i64 {
    let Ok(s) = rd("/proc/stat") else { return -1 };
    let mut t = 0i64;
    for l in s.lines() {
        if !l.starts_with("cpu") || l.starts_with("cpu ") { continue; }
        let w: Vec<&str> = l.split_whitespace().collect();
        let Ok(c) = w[0][3..].parse::<usize>() else { continue };
        if cpus.contains(&c) { t += w.get(8).and_then(|x| x.parse::<i64>().ok()).unwrap_or(0); }
    }
    t
}
fn clk_ns(c: i32) -> i64 { let mut t = [0i64; 2]; unsafe { clock_gettime(c, &mut t) }; t[0] * 1_000_000_000 + t[1] }
static REAL0: AtomicU64 = AtomicU64::new(0);
static BOOT0: AtomicU64 = AtomicU64::new(0);
fn setaff(cpus: &[usize]) -> i32 {
    let mut m = [0u64; 16];
    for &c in cpus { if c < 1024 { m[c / 64] |= 1u64 << (c % 64); } }
    unsafe { sched_setaffinity(0, 128, m.as_ptr()) }
}

fn tcpu() -> u64 { clk_ns(3) as u64 }
fn touch(p: *mut u8, len: usize) { let mut i = 0; while i < len { unsafe { std::ptr::write_volatile(p.add(i), 1u8) }; i += 4096; } }
fn dontneed(a: &Anon) -> i32 { unsafe { madvise(a.p, (a.len + MB2 - 1) & !(MB2 - 1), MADV_DONTNEED) } }
fn par_touch(p: Ptr, len: usize, cpus: &[usize]) -> (u64, u64) {
    let nt = 8; let part = len / nt;
    let bar = std::sync::Arc::new(std::sync::Barrier::new(nt));
    let hs: Vec<_> = (0..nt).map(|k| {
        let bar = bar.clone(); let c = cpus.get(k % cpus.len().max(1)).copied();
        std::thread::Builder::new().stack_size(256 << 10).spawn(move || {
            if let Some(c) = c { pin(c); }
            bar.wait();
            let (a, c0) = (now(), tcpu());
            touch(unsafe { p.g().add(k * part) }, part);
            (a, now(), tcpu() - c0)
        }).unwrap()
    }).collect();
    let rs: Vec<(u64, u64, u64)> = hs.into_iter().map(|h| h.join().unwrap()).collect();
    let s = rs.iter().map(|x| x.0).min().unwrap_or(0); let e = rs.iter().map(|x| x.1).max().unwrap_or(0);
    (e.saturating_sub(s), rs.iter().map(|x| x.2).sum())
}
fn ahp_kb() -> i64 { rd("/proc/self/smaps_rollup").unwrap_or_default().lines().find(|l| l.starts_with("AnonHugePages:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|x| x.parse::<i64>().ok()).unwrap_or(-1) }
fn qm_child(args: &[String]) -> ! {
    let case = args.get(2).map(|s| s.as_str()).unwrap_or("");
    let len = 512 * MB;
    let cpus = getaff();
    let mut v: Vec<i64> = vec![];
    match case {
        "1" => {
            let Some(a) = Anon::new(len, 1) else { raw(b"QM fail\n"); unsafe { _exit(3) } };
            let t0 = now(); touch(a.p, len); let t1 = now();
            let ahp = ahp_kb();
            dontneed(&a);
            let t2 = now(); touch(a.p, len); let t3 = now();
            touch(a.p, len); let t4 = now();
            v = vec![(t1 - t0) as i64, (t3 - t2) as i64, (t4 - t3) as i64, ahp];
        }
        "8" => {
            let Some(a) = Anon::new(len, 1) else { raw(b"QM fail\n"); unsafe { _exit(3) } };
            let (w1, c1) = par_touch(Ptr(a.p), len, &cpus);
            let ahp = ahp_kb();
            dontneed(&a);
            let (w2, c2) = par_touch(Ptr(a.p), len, &cpus);
            v = vec![w1 as i64, c1 as i64, w2 as i64, c2 as i64, ahp];
        }
        "n" => {
            let Some(a) = Anon::new(len, 2) else { raw(b"QM fail\n"); unsafe { _exit(3) } };
            let (w, c) = par_touch(Ptr(a.p), len, &cpus);
            let ahp = ahp_kb();
            let um = a.free();
            v = vec![w as i64, c as i64, (um * 1e6) as i64, ahp];
        }
        "z" => {
            let Some(a) = Anon::new(len, 1) else { raw(b"QM fail\n"); unsafe { _exit(3) } };
            touch(a.p, len);
            let mut best = u64::MAX;
            for k in 0..3u8 { let t = now(); unsafe { std::ptr::write_bytes(black_box(a.p), k + 7, len) }; black_box(a.p); best = best.min(now() - t); }
            v = vec![best as i64];
        }
        "p" => {
            let dir = args.get(3).cloned().unwrap_or_else(|| "/scratch".into());
            let path = cs(&format!("{dir}/qm_pc_{}", unsafe { getpid() }));
            let buf = vec![0x5au8; MB];
            let fd = unsafe { open(path.as_ptr() as *const i8, 0o1 | 0o100 | 0o1000, 0o644) };
            if fd < 0 { raw(b"QM open fail\n"); unsafe { _exit(4) } }
            let t0 = now(); let mut bad = 0;
            for _ in 0..64 { if unsafe { write(fd, buf.as_ptr(), MB) } != MB as isize { bad += 1; } }
            let t1 = now();
            unsafe { close(fd); }
            let t2 = now(); unsafe { unlink(path.as_ptr()); } let t3 = now();
            v = vec![(t1 - t0) as i64, (t3 - t2) as i64, bad];
        }
        "s" => {
            let Some(a) = Anon::new(len, 1) else { raw(b"QM fail\n"); unsafe { _exit(3) } };
            touch(a.p, len);
            dontneed(&a);
            std::thread::sleep(std::time::Duration::from_millis(3000));
            let t0 = now(); touch(a.p, len); let t1 = now();
            v = vec![(t1 - t0) as i64];
        }
        "a" => {
            let l6 = 600 * MB;
            let Some(a) = Anon::new(l6, 1) else { raw(b"QM fail\n"); unsafe { _exit(3) } };
            let t0 = now(); touch(a.p, l6); let t1 = now();
            v = vec![(t1 - t0) as i64];
        }
        _ => { raw(b"QM bad case\n"); unsafe { _exit(2) } }
    }
    raw(format!("QM {}\n", v.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(" ")).as_bytes());
    unsafe { _exit(0) }
}
fn qm_line(me: &str, tag: &str, scratch: &str, lim: f64, slow: bool) -> String {
    let mut parts = vec![tag.to_string()];
    let nm = |x: i64| f(x as f64 / 1e6);
    let mut ab: Vec<String> = vec![];
    let mut cases = vec![("1", "f1"), ("8", "f8"), ("n", "n8"), ("z", "z1"), ("p", "pc"), ("a", "ab"), ("a", "ab")];
    if slow { cases.push(("s", "rs")); }
    for (case, names) in cases {
        if case == "a" {
            if el_s() > lim { ab.push("cut".into()); } else {
                let r = run_child(me, &["--qm", case], &[], 20.0, None);
                let v: Vec<i64> = r.err.lines().find_map(|l| l.strip_prefix("QM ")).map(|x| x.split_whitespace().filter_map(|y| y.parse().ok()).collect()).unwrap_or_default();
                kv!(format!("QM.{tag}.a{}", ab.len()), "rc {} wall {:.2} out {:?}", r.rc, r.wall, r.err.trim());
                ab.push(if r.rc == 0 && v.len() == 1 { nm(v[0]) } else { format!("!rc{}", r.rc) });
            }
            if ab.len() == 2 { parts.push(format!("ab={}", ab.join("/"))); }
            continue;
        }
        if el_s() > lim { parts.push(format!("{names}=cut")); continue; }
        let r = run_child(me, &["--qm", case, scratch], &[], 20.0, None);
        let v: Vec<i64> = r.err.lines().find_map(|l| l.strip_prefix("QM ")).map(|x| x.split_whitespace().filter_map(|y| y.parse().ok()).collect()).unwrap_or_default();
        kv!(format!("QM.{tag}.{case}"), "rc {} wall {:.2} out {:?}", r.rc, r.wall, r.err.trim());
        let want = match case { "1" => 4, "8" => 5, "n" => 4, "z" => 1, "s" => 1, _ => 3 };
        if r.rc != 0 || v.len() != want { parts.push(format!("{names}=!rc{}", r.rc)); continue; }
        match case {
            "1" => parts.push(format!("f1={} r1={}{}{}", nm(v[0]), nm(v[1]), if slow { String::new() } else { format!(" w1={}", nm(v[2])) }, if v[3] < 512 * 1024 { format!(" ahp1={}M", v[3] / 1024) } else { String::new() })),
            "8" => parts.push(format!("f8={}/{} r8={}/{}{}", nm(v[0]), nm(v[1]), nm(v[2]), nm(v[3]), if v[4] < 512 * 1024 { format!(" ahp8={}M", v[4] / 1024) } else { String::new() })),
            "n" => parts.push(format!("n8={}/{}/{}{}", nm(v[0]), nm(v[1]), nm(v[2]), if v[3] > 0 { format!(" ahpn={}M", v[3] / 1024) } else { String::new() })),
            "z" => parts.push(format!("z1={}", nm(v[0]))),
            "s" => parts.push(format!("rs={}", nm(v[0]))),
            _ => parts.push(format!("pc={}{}", nm(v[0]), if v[2] > 0 { format!("!{}", v[2]) } else { String::new() })),
        }
    }
    parts.join(" ")
}

fn child_mode(args: &[String]) {
    let a1 = args.get(1).map(|s| s.as_str()).unwrap_or("");
    match a1 {
        "--nop" => unsafe { _exit(0) },
        "--t0" => { let t = now(); raw(format!("T0 {t}\n").as_bytes()); unsafe { _exit(0) } }
        "--qm" => qm_child(args),
        "--op" => {
            let p = args.get(2).cloned().unwrap_or_default();
            match std::fs::File::open(&p) {
                Ok(mut fh) => { let mut b = [0u8; 1]; match fh.read(&mut b) { Ok(1) => unsafe { _exit(0) }, _ => { raw(format!("OP read {}\n", en()).as_bytes()); unsafe { _exit(3) } } } }
                Err(e) => { raw(format!("OP open {}\n", e.raw_os_error().unwrap_or(-1)).as_bytes()); unsafe { _exit(2) } }
            }
        }
        "--hp" => {
            let t0 = now();
            let mb: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(1024);
            let mode: i32 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(1);
            let Some(a) = Anon::new(mb * MB, mode) else { raw(b"HP fail\n"); unsafe { _exit(3) } };
            let mut i = 0; while i < a.len { unsafe { std::ptr::write_volatile(a.p.add(i), 1u8) }; i += 4096; }
            let t1 = now();
            let ahp = rd("/proc/self/smaps_rollup").unwrap_or_default().lines().find(|l| l.starts_with("AnonHugePages:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|x| x.parse::<i64>().ok()).unwrap_or(-1);
            let t2 = now();
            raw(format!("HP {t0} {t1} {t2} {ahp}\n").as_bytes());
            unsafe { _exit(0) }
        }
        _ => {}
    }
}

struct Run { wall: f64, rc: i32, err: String, killed: bool, t0: u64, t1: u64, ru: [f64; 5] }
fn rusage_ch() -> [i64; 18] { let mut r = [0i64; 18]; unsafe { getrusage(-1, &mut r) }; r }
fn run_child(path: &str, args: &[&str], envs: &[(&str, &str)], timeout_s: f64, cpus: Option<&[usize]>) -> Run {
    use std::process::{Command, Stdio};
    use std::os::unix::process::ExitStatusExt;
    let mut cmd = Command::new(path);
    cmd.args(args).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::piped());
    for (k, v) in envs { cmd.env(k, v); }
    KILLED.store(false, Relaxed);
    let saved = getaff();
    if let Some(c) = cpus { setaff(c); }
    let r0 = rusage_ch();
    let t0 = now();
    let sp = cmd.spawn();
    if cpus.is_some() { setaff(&saved); }
    let mut ch = match sp { Ok(c) => c, Err(e) => return Run { wall: 0.0, rc: -1000 - e.raw_os_error().unwrap_or(0), err: format!("spawn: {e}"), killed: false, t0, t1: t0, ru: [0.0; 5] } };
    CHILD_DL.store(t0 + (timeout_s * 1e9) as u64, Release);
    CHILD.store(ch.id() as i32, Release);
    let mut err = vec![];
    if let Some(mut s) = ch.stderr.take() { let _ = s.read_to_end(&mut err); }
    let st = ch.wait();
    let t1 = now();
    CHILD.store(0, Release);
    let r1 = rusage_ch();
    let tms = |s: usize| ((r1[s] - r0[s]) as f64) * 1e3 + ((r1[s + 1] - r0[s + 1]) as f64) / 1e3;
    let ru = [tms(0), tms(2), (r1[8] - r0[8]) as f64, (r1[16] - r0[16]) as f64, (r1[17] - r0[17]) as f64];
    let rc = match st { Ok(s) => s.code().unwrap_or_else(|| -s.signal().unwrap_or(0)), Err(_) => -2000 };
    Run { wall: ms(t1 - t0), rc, err: String::from_utf8_lossy(&err).into_owned(), killed: KILLED.load(Relaxed), t0, t1, ru }
}

struct Tc<'a> { b: &'a [u8], i: usize }
impl<'a> Tc<'a> {
    fn byte(&mut self) -> Option<u8> { let x = *self.b.get(self.i)?; self.i += 1; Some(x) }
    fn varint(&mut self) -> Option<u64> { let mut v = 0u64; let mut s = 0; loop { let x = self.byte()?; v |= ((x & 0x7f) as u64) << s; if x & 0x80 == 0 { return Some(v); } s += 7; if s > 63 { return None; } } }
    fn adv(&mut self, n: usize) -> Option<()> { if self.i + n > self.b.len() { return None; } self.i += n; Some(()) }
    fn elem(&mut self, t: u8) -> Option<()> { if t == 1 || t == 2 { self.adv(1) } else { self.val(t) } }
    fn val(&mut self, t: u8) -> Option<()> {
        match t {
            1 | 2 => Some(()),
            3 => self.adv(1),
            4 | 5 | 6 => self.varint().map(|_| ()),
            7 => self.adv(8),
            8 => { let n = self.varint()? as usize; self.adv(n) }
            9 | 10 => { let h = self.byte()?; let mut n = (h >> 4) as u64; if n == 15 { n = self.varint()?; } for _ in 0..n { self.elem(h & 15)?; } Some(()) }
            11 => { let n = self.varint()?; if n > 0 { let kv = self.byte()?; for _ in 0..n { self.elem(kv >> 4)?; self.elem(kv & 15)?; } } Some(()) }
            12 => self.strukt(0).map(|_| ()),
            _ => None,
        }
    }
    fn strukt(&mut self, want: i16) -> Option<Option<i64>> {
        let mut last = 0i16; let mut found = None;
        loop {
            let h = self.byte()?; if h == 0 { return Some(found); }
            let d = (h >> 4) as i16; let t = h & 15;
            let id = if d == 0 { let z = self.varint()?; ((z >> 1) as i64 ^ -((z & 1) as i64)) as i16 } else { last + d };
            last = id;
            if want != 0 && id == want && t == 6 { let z = self.varint()?; found = Some((z >> 1) as i64 ^ -((z & 1) as i64)); } else { self.val(t)?; }
        }
    }
}
fn pq_rows_file(p: &Path) -> Option<u64> {
    use std::io::{Seek, SeekFrom};
    let mut f = std::fs::File::open(p).ok()?;
    let len = f.metadata().ok()?.len();
    if len < 12 { return None; }
    let mut t = [0u8; 8]; f.seek(SeekFrom::Start(len - 8)).ok()?; f.read_exact(&mut t).ok()?;
    if &t[4..] != b"PAR1" { return None; }
    let fl = u32::from_le_bytes([t[0], t[1], t[2], t[3]]) as u64;
    if fl + 8 > len { return None; }
    let mut b = vec![0u8; fl as usize]; f.seek(SeekFrom::Start(len - 8 - fl)).ok()?; f.read_exact(&mut b).ok()?;
    let r = Tc { b: &b, i: 0 }.strukt(3)??;
    if r < 0 { None } else { Some(r as u64) }
}
fn pq_rows_dir(d: &str) -> Option<u64> {
    let mut v = vec![]; let mut e = 0; walk(Path::new(d), &mut v, &mut e, 0);
    let mut tot = 0u64; let mut any = false;
    for (p, _) in &v { if p.extension().is_some_and(|x| x == "parquet") { tot += pq_rows_file(p)?; any = true; } }
    if any { Some(tot) } else { None }
}

struct Var { name: String, bin: String, env: Vec<(String, String)> }
fn vars(bindir: &str) -> Vec<Var> {
    let s = rd(&format!("{bindir}/list")).unwrap_or_default();
    let mut v = vec![];
    for l in s.lines() {
        let w: Vec<&str> = l.split_whitespace().collect();
        if w.len() < 2 { continue; }
        let binname = if let Some(b) = w[1].strip_prefix('=') { b } else { w[0] };
        let bin = format!("{bindir}/{binname}");
        let env = w.iter().skip(3).filter(|e| !e.starts_with("BUILD:")).filter_map(|e| e.split_once('=')).map(|(a, b)| (a.to_string(), b.to_string())).collect();
        if Path::new(&bin).is_file() { v.push(Var { name: w[0].to_string(), bin, env }); } else { put(9, &format!("nobin={}", w[0])); }
    }
    v
}
#[derive(Clone)]
struct Rec { v: String, mode: String, wall: f64, stages: Vec<(String, f64)>, rss_kb: f64, rows: Option<u64>, pq: (Option<u64>, Option<u64>), rc: i32, fb: bool, killed: bool, ru: [f64; 5], bad: bool, pga: i64 }
fn parse_run(v: &str, mode: &str, r: &Run) -> Rec {
    let mut stages = vec![]; let mut rss = f64::NAN; let mut rows = None;
    for l in r.err.lines() {
        if let Some(i) = l.find("] ms: ") {
            for tok in l[i + 6..].split_whitespace() {
                if let Some((k, x)) = tok.split_once('=') { if let Ok(x) = x.parse::<f64>() { if k == "peak_rss_kb" { rss = x; } else { stages.push((k.to_string(), x)); } } }
            }
        }
        if let Some(i) = l.find("user_rows=") { rows = l[i + 10..].split_whitespace().next().and_then(|s| s.parse().ok()); }
    }
    Rec { v: v.into(), mode: mode.into(), wall: r.wall, stages, rss_kb: rss, rows, pq: (None, None), rc: r.rc, fb: r.err.contains("FALLBACK"), killed: r.killed, ru: r.ru, bad: false, pga: -1 }
}
fn okr(r: &Rec) -> bool { r.rc == 0 && !r.fb && !r.killed && !r.bad }
fn stage(r: &Rec, k: &str) -> f64 { r.stages.iter().find(|x| x.0 == k).map(|x| x.1).unwrap_or(f64::NAN) }
fn badmark(r: &Rec) -> String { format!("!rc{}{}{}{}", r.rc, if r.fb { "FB" } else { "" }, if r.killed { "K" } else { "" }, if r.bad { "R" } else { "" }) }

static REFROWS: Mutex<Option<(Option<u64>, Option<u64>)>> = Mutex::new(None);
fn pipe_run(v: &Var, mode: &str, data: &str, scratch: &str, n: &mut usize, cpus: Option<&[usize]>, ncpu: Option<usize>, keep: Option<&str>) -> Rec {
    *n += 1;
    let out = match keep { Some(k) => k.to_string(), None => format!("{scratch}/out_{n}") };
    let _ = std::fs::remove_dir_all(&out); let _ = std::fs::create_dir_all(&out);
    let nc = ncpu.map(|x| x.to_string());
    let mut envs: Vec<(&str, &str)> = vec![("XL_TIMING", "1")];
    for (a, b) in &v.env { envs.push((a.as_str(), b.as_str())); }
    if let Some(x) = &nc { envs.push(("NCPU", x.as_str())); }
    let a0 = vmstat("pgactivate");
    let r = run_child(&v.bin, &[data, &out], &envs, 30.0, cpus);
    let a1 = vmstat("pgactivate");
    let mut rec = parse_run(&v.name, mode, &r);
    if a0 >= 0 && a1 >= 0 { rec.pga = a1 - a0; }
    rec.pq = (pq_rows_dir(&format!("{out}/user_daily")), pq_rows_dir(&format!("{out}/campaign_daily")));
    if rec.rc == 0 && !rec.fb && !rec.killed {
        if let Ok(mut g) = REFROWS.lock() {
            match *g {
                None => { if rec.pq.0.is_some() && rec.pq.1.is_some() { *g = Some(rec.pq); } else { rec.bad = true; } }
                Some(x) => { if x != rec.pq { rec.bad = true; } }
            }
        }
    }
    if rec.stages.is_empty() || !okr(&rec) { let tail: String = r.err.chars().rev().take(300).collect::<Vec<_>>().into_iter().rev().collect(); kv!(format!("R.{}.{mode}.stderr", v.name), "{}", tail); }
    kv!(format!("R.{}.{}", rec.v, rec.mode), "wall {:.2} ms rc {} fb {} killed {} rows {:?} pq {:?} rss_kb {} ru {:?} pga {} stages {:?}", rec.wall, rec.rc, rec.fb, rec.killed, rec.rows, rec.pq, rec.rss_kb, rec.ru, rec.pga, rec.stages);
    if keep.is_none() { let _ = std::fs::remove_dir_all(&out); }
    rec
}
fn minstage(rs: &[&Rec], k: &str) -> f64 { rs.iter().map(|r| if k == "wall" { r.wall } else { stage(r, k) }).filter(|x| x.is_finite()).fold(f64::INFINITY, f64::min) }
fn summ(name: &str, rs: &[&Rec]) -> (String, String, String, String) {
    let ok: Vec<&Rec> = rs.iter().cloned().filter(|r| okr(r)).collect();
    let bad: Vec<String> = rs.iter().filter(|r| !okr(r)).map(|r| badmark(r)).collect();
    let main = if ok.is_empty() { format!("{}={}", name, bad.first().cloned().unwrap_or("-".into())) } else {
        format!("{}={}/{}{}", name, f(minstage(&ok, "wall")), f(minstage(&ok, "parse")), if bad.is_empty() { "" } else { "!" })
    };
    let main2 = if ok.is_empty() { format!("{}=-", name) } else { format!("{}={}/{}", name, f(minstage(&ok, "ev3a")), f(minstage(&ok, "write"))) };
    let all = format!("{}={}", name, rs.iter().map(|r| if okr(r) { f(r.wall) } else { badmark(r) }).collect::<Vec<_>>().join(","));
    let x = if ok.is_empty() { String::new() } else { format!("{}={}/{}", name, f(minstage(&ok, "total")), f(ok.iter().map(|r| r.rss_kb).filter(|x| x.is_finite()).fold(0.0, f64::max) / 1024.0)) };
    (main, all, x, main2)
}
fn q1_lines(vs: &[Var], recs: &[Rec], mode: &str) {
    let (mut a, mut b, mut c, mut d) = (vec![], vec![], vec![], vec![]); let mut nbad = 0;
    for v in vs {
        let rs: Vec<&Rec> = recs.iter().filter(|r| r.v == v.name && r.mode == mode).collect();
        if rs.is_empty() { continue; }
        nbad += rs.iter().filter(|r| !okr(r)).count();
        let (x, y, z, w) = summ(&v.name, &rs); a.push(x); b.push(y); if !z.is_empty() { c.push(z); } d.push(w);
    }
    setl(0, &format!("{} {} rw={}", if mode == "n" { "q1" } else { "q1dev" }, a.join(" "), if nbad == 0 { "ok".to_string() } else { format!("NO{nbad}") }));
    setl(19, &format!("q1b {}", d.join(" ")));
    setl(5, &format!("q1all {}", b.join(" ")));
    setl(6, &format!("q1x {}", c.join(" ")));
}
fn ru_str(r: &Rec) -> String { format!("u={} s={} mf={} vc={} ic={}", g3(r.ru[0]), g3(r.ru[1]), g3(r.ru[2]), g3(r.ru[3]), g3(r.ru[4])) }

fn ftime_rel(p: &str) -> String {
    use std::os::unix::fs::MetadataExt;
    match std::fs::metadata(p) { Ok(m) => { let t = m.mtime() * 1_000_000_000 + m.mtime_nsec(); f((t - REAL0.load(Relaxed) as i64) as f64 / 1e6) } Err(_) => "x".into() }
}
fn ctime_rel(p: &str) -> String {
    use std::os::unix::fs::MetadataExt;
    match std::fs::metadata(p) { Ok(m) => { let t = m.ctime() * 1_000_000_000 + m.ctime_nsec(); f((t - REAL0.load(Relaxed) as i64) as f64 / 1e6) } Err(_) => "x".into() }
}
fn pstart(pid: &str) -> Option<u64> {
    let s = rd(&format!("/proc/{pid}/stat")).ok()?;
    let rest = &s[s.rfind(')')? + 2..];
    rest.split_whitespace().nth(19)?.parse().ok()
}
fn q3_static() -> String {
    let boot0 = BOOT0.load(Relaxed) as f64 / 1e6;
    let p1 = pstart("1"); let me = pstart("self");
    let bt: Option<f64> = rd("/proc/stat").ok().and_then(|s| s.lines().find(|l| l.starts_with("btime ")).and_then(|l| l[6..].trim().parse().ok()));
    let rel = |t: Option<u64>| t.map(|x| f(x as f64 * 10.0 - boot0)).unwrap_or("x".into());
    let relb = |t: Option<u64>| match (t, bt) { (Some(x), Some(b)) => f((b * 1e3 + x as f64 * 10.0) - REAL0.load(Relaxed) as f64 / 1e6), _ => "x".into() };
    let pid = unsafe { getpid() };
    let mut s = format!("q3 h={} hn={} rc={} dev={} cg={}/{} p1={}", ftime_rel("/etc/hosts"), ftime_rel("/etc/hostname"), ftime_rel("/etc/resolv.conf"), ftime_rel("/dev"), ctime_rel("/sys/fs/cgroup"), ftime_rel("/sys/fs/cgroup"), rel(p1));
    if pid != 1 { s.push_str(&format!(" pid={} ps={}", pid, rel(me))); }
    kv!("Q3.static", "{} boot0_ms {:.3} p1 {:?} self {:?} btime {:?} real0 {}", s, boot0, p1, me, bt, REAL0.load(Relaxed));
    s
}
fn q3_children(me: &str, lim: f64) -> String {
    let mut parts = vec![];
    let mut nop = vec![];
    for _ in 0..5 { if el_s() > lim { break; } let r = run_child(me, &["--nop"], &[], 5.0, None); if r.rc == 0 { nop.push(r.wall); } }
    nop.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if !nop.is_empty() { parts.push(format!("nop={}/{}", f(nop[0]), f(nop[nop.len() / 2]))); }
    let mut best: Option<(f64, f64)> = None;
    for _ in 0..3 {
        if el_s() > lim { break; }
        let r = run_child(me, &["--t0"], &[], 5.0, None);
        if let Some(t) = r.err.lines().find_map(|l| l.strip_prefix("T0 ")).and_then(|x| x.trim().parse::<u64>().ok()) {
            let a = ms(t.saturating_sub(r.t0)); let b = ms(r.t1.saturating_sub(t));
            if best.map(|x| a + b < x.0 + x.1).unwrap_or(true) { best = Some((a, b)); }
        }
    }
    if let Some((a, b)) = best { parts.push(format!("t0={}/{}", f(a), f(b))); }
    for (tag, mode) in [("hp", "1"), ("np", "2")].into_iter().filter(|_| std::env::var("PROBE_Q3HP").is_ok()) {
        if el_s() > lim { parts.push(format!("{tag}=cut")); continue; }
        let r = run_child(me, &["--hp", "1024", mode], &[], 20.0, None);
        let v: Vec<i64> = r.err.lines().find_map(|l| l.strip_prefix("HP ")).map(|x| x.split_whitespace().filter_map(|y| y.parse().ok()).collect()).unwrap_or_default();
        if v.len() == 4 && r.rc == 0 {
            let (t0, t1, t2) = (v[0] as u64, v[1] as u64, v[2] as u64);
            parts.push(format!("{tag}={}/{}/{}/{} ahp={}M", f(ms(t0.saturating_sub(r.t0))), f(ms(t1 - t0)), f(ms(t2 - t1)), f(ms(r.t1.saturating_sub(t2))), v[3] / 1024));
        } else { parts.push(format!("{tag}=!rc{}", r.rc)); }
        kv!(format!("Q3.{tag}"), "rc {} wall {:.2} out {:?}", r.rc, r.wall, r.err.trim());
    }
    parts.join(" ")
}
fn sysline(t: &Topo) -> String {
    let ci = rd("/proc/cpuinfo").unwrap_or_default();
    let first = ci.split("\n\n").next().unwrap_or("");
    let get = |k: &str| -> String { first.lines().find(|l| l.starts_with(k)).and_then(|l| l.split_once(':')).map(|x| x.1.trim().to_string()).unwrap_or_else(|| "?".into()) };
    let thp = |x: &str| { let s = rdl(&format!("/sys/kernel/mm/transparent_hugepage/{x}")); match (s.find('['), s.find(']')) { (Some(a), Some(b)) if b > a => s[a + 1..b].to_string(), _ => s } };
    format!("sys cpu={} cpus={} kern={} thp={}/{} cpu.max={} mem.max={}", get("model name"), list_str(&t.allowed), rdl("/proc/sys/kernel/osrelease"), thp("enabled"), thp("defrag"), rdl("/sys/fs/cgroup/cpu.max"), rdl("/sys/fs/cgroup/memory.max"))
}

fn devno(d: u64) -> (u64, u64) { (((d >> 8) & 0xfff) | ((d >> 32) & !0xfff), (d & 0xff) | ((d >> 12) & !0xff)) }
fn fstype(p: &str) -> String {
    let mi = rd("/proc/self/mountinfo").unwrap_or_default();
    let mut best: (usize, String) = (0, "?".into());
    for l in mi.lines() {
        let w: Vec<&str> = l.split_whitespace().collect();
        if w.len() < 5 { continue; }
        let mp = w[4];
        let ok = mp == "/" || p == mp || p.starts_with(&format!("{mp}/"));
        if !ok { continue; }
        let Some(dash) = w.iter().position(|x| *x == "-") else { continue };
        let ft = w.get(dash + 1).copied().unwrap_or("?");
        if mp.len() >= best.0 { best = (mp.len(), ft.chars().take(4).collect()); }
    }
    best.1
}
fn ls(p: &str) -> Option<Vec<String>> { std::fs::read_dir(p).ok().map(|r| r.flatten().map(|e| e.file_name().to_string_lossy().into_owned()).collect()) }
fn qo_static(out: &str, scratch: &str, data: &str) -> (String, String) {
    use std::os::unix::fs::MetadataExt;
    let lo = ls(out); let lu = ls(&format!("{out}/user_daily")); let lc = ls(&format!("{out}/campaign_daily"));
    let cnt = |x: &Option<Vec<String>>| x.as_ref().map(|v| v.len().to_string()).unwrap_or("-".into());
    let empty = lo.as_ref().map(|v| v.is_empty()).unwrap_or(false);
    let mut names = String::new();
    for (k, x) in [("o", &lo), ("u", &lu), ("c", &lc)] { if let Some(v) = x { if !v.is_empty() { names.push_str(&format!(" {k}:{}", v.iter().take(8).cloned().collect::<Vec<_>>().join(","))); } } }
    kv!("QO.ls", "out {:?} user {:?} camp {:?}", lo, lu, lc);
    let paths = [("o", out), ("s", scratch), ("d", data), ("h", "/etc/hosts"), ("r", "/")];
    let mut groups: Vec<(Option<(u64, u64)>, String, String)> = vec![];
    for (k, p) in paths {
        let d = std::fs::metadata(p).ok().map(|m| devno(m.dev()));
        kv!(format!("QO.dev.{k}"), "{p} {:?} fs {}", d, fstype(p));
        match groups.iter_mut().find(|g| g.0 == d && d.is_some()) { Some(g) => g.1.push_str(k), None => groups.push((d, k.to_string(), fstype(p))) }
    }
    let dv: Vec<String> = groups.iter().map(|g| match g.0 { Some((a, b)) => format!("{}={a}:{b}/{}", g.1, g.2), None => format!("{}=!", g.1) }).collect();
    (format!("qo {}/{}/{}{} {}", cnt(&lo), cnt(&lu), cnt(&lc), if empty { "e" } else { "" }, dv.join(" ")), names)
}
fn syncfs_ms(path: &str) -> Result<f64, i32> {
    let c = cs(path);
    let fd = unsafe { open(c.as_ptr() as *const i8, 0o200000) };
    if fd < 0 { return Err(en()); }
    let t = now();
    let r = unsafe { syscall(306, fd as i64) };
    let dt = ms(now() - t);
    let e = if r < 0 { en() } else { 0 };
    unsafe { close(fd) };
    if r < 0 { Err(e) } else { Ok(dt) }
}
fn dir_stats(d: &str) -> (usize, u64) { let mut v = vec![]; let mut e = 0; walk(Path::new(d), &mut v, &mut e, 0); (v.len(), v.iter().map(|x| x.1).sum()) }
fn fsync_files(d: &str) -> (f64, i32) {
    use std::os::unix::io::AsRawFd;
    let mut v = vec![]; let mut e = 0; walk(Path::new(d), &mut v, &mut e, 0);
    let mut t = 0u64; let mut err = 0;
    for (p, _) in &v { if let Ok(h) = std::fs::File::open(p) { let a = now(); if unsafe { fsync(h.as_raw_fd()) } != 0 { err = en(); } t += now() - a; } }
    (ms(t), err)
}
fn qo_wb(roles: &[(&str, &Var)], data: &str, out: &str, scratch: &str, n: &mut usize, lim: f64) -> String {
    let base = format!("{out}/_qo");
    let mut sf = vec![]; let mut fs = vec![];
    for pass in 0..2 {
        for (role, v) in roles {
            let dst = if pass == 0 { &mut sf } else { &mut fs };
            if el_s() > lim { dst.push(format!("{role}=cut")); continue; }
            let dir = format!("{base}/{pass}{role}");
            let _ = std::fs::remove_dir_all(&dir);
            let pre = syncfs_ms(out);
            let rec = pipe_run(v, "qo", data, scratch, n, None, None, Some(&dir));
            let (files, bytes) = dir_stats(&dir);
            let res = if !okr(&rec) { badmark(&rec) } else if pass == 0 {
                match syncfs_ms(out) { Ok(t) => format!("{}/{}M/{}", f(t), f(bytes as f64 / 1e6), files), Err(e) => format!("!{e}") }
            } else {
                let (t, err) = fsync_files(&dir);
                let r = syncfs_ms(out);
                format!("{}{}+{}", f(t), if err != 0 { format!("!{err}") } else { String::new() }, match r { Ok(x) => f(x), Err(e) => format!("!{e}") })
            };
            kv!(format!("QO.wb.{pass}.{role}"), "pre {:?} res {} files {} bytes {} wall {:.2}", pre, res, files, bytes, rec.wall);
            dst.push(format!("{role}={res}"));
        }
    }
    let _ = std::fs::remove_dir_all(&base);
    let _ = syncfs_ms(out);
    format!("wb {} fs {}", sf.join(" "), fs.join(" "))
}

fn minflt_t() -> i64 { let mut r = [0i64; 18]; unsafe { getrusage(1, &mut r) }; r[8] + r[9] }
fn two_page(p: &str) -> String {
    use std::os::unix::io::AsRawFd;
    let Ok(fh) = std::fs::File::open(p) else { return format!("!o{}", en()) };
    let sz = fh.metadata().map(|m| m.len()).unwrap_or(0) as usize;
    if sz < 3 * 65536 { return "-".into(); }
    let m = unsafe { mmap(std::ptr::null_mut(), sz, PROT_READ, MAP_SHARED, fh.as_raw_fd(), 0) };
    if m as isize == -1 { return format!("!m{}", en()); }
    let mut parts = vec![];
    for fr in [1usize, 2] {
        let off = (sz / 3 * fr) & !0xffff;
        let a = minflt_t(); unsafe { std::ptr::read_volatile(m.add(off)) }; let b = minflt_t();
        unsafe { std::ptr::read_volatile(m.add(off + 4096)) }; let c = minflt_t();
        parts.push(format!("{}+{}", b - a, c - b));
    }
    unsafe { munmap(m, sz) };
    parts.join(",")
}
fn largest(files: &[(PathBuf, u64)]) -> Option<String> { files.iter().max_by_key(|x| x.1).map(|x| x.0.to_string_lossy().into_owned()) }
fn qd_static(data: &str) -> (String, String) {
    let tp = match largest(&csv_files(data)) { Some(p) => two_page(&p), None => "-".into() };
    let cl = rdl("/proc/cmdline");
    let flag = |k: &str| cl.split_whitespace().find(|t| *t == k || t.starts_with(&format!("{k}="))).map(|t| t.split_once('=').map(|x| x.1.to_string()).unwrap_or("y".into())).unwrap_or("-".into());
    let br = |p: &str| { let s = rdl(p); match (s.find('['), s.find(']')) { (Some(a), Some(b)) if b > a => s[a + 1..b].to_string(), _ => if s.starts_with("ERR") { "!".into() } else { s } } };
    let ex = |p: &str| if Path::new(p).exists() { "1" } else { "0" };
    let pro = rd("/sys/module/page_reporting/parameters/page_reporting_order").map(|s| s.trim().to_string()).unwrap_or("-".into());
    kv!("QD.cmdline", "{}", cl);
    let _ = pro;
    let _ = ex;
    (format!("2p={} ia={} if={} th={} se={}", tp, flag("init_on_alloc"), flag("init_on_free"), flag("transparent_hugepage"), br("/sys/kernel/mm/transparent_hugepage/shmem_enabled").chars().take(3).collect::<String>()), cl)
}
fn mem_avail() -> f64 {
    let mi = rd("/proc/meminfo").unwrap_or_default();
    let ma = mi.lines().find(|l| l.starts_with("MemAvailable:")).and_then(|l| l.split_whitespace().nth(1)).and_then(|x| x.parse::<f64>().ok()).map(|x| x * 1024.0).unwrap_or(f64::INFINITY);
    let mx = rdl("/sys/fs/cgroup/memory.max").parse::<f64>().unwrap_or(f64::INFINITY);
    let cur = rdl("/sys/fs/cgroup/memory.current").parse::<f64>().unwrap_or(0.0);
    ma.min(mx - cur)
}
struct Shm { dir: String, fds: Vec<i32>, ms: f64, lnk: char, big: String }
fn shm_copy(src: &str, dst: &str, me: &str, nt: usize) -> Result<Shm, String> {
    use std::os::unix::fs::symlink;
    let mut files = vec![]; let mut e = 0; walk(Path::new(src), &mut files, &mut e, 0);
    files.sort_by(|a, b| b.1.cmp(&a.1));
    let _ = std::fs::remove_dir_all(dst);
    let t0 = now();
    let mut fds: Vec<i32> = vec![];
    for (p, _) in &files {
        let name = cs(&p.file_name().map(|x| x.to_string_lossy().into_owned()).unwrap_or("f".into()));
        let fd = unsafe { syscall(319, name.as_ptr(), 1i64) } as i32;
        if fd < 0 { let e = en(); for &x in &fds { unsafe { close(x) }; } return Err(format!("mfd!{e}")); }
        fds.push(fd);
    }
    let next = AtomicUsize::new(0); let errc = AtomicI32::new(0);
    std::thread::scope(|sc| {
        for _ in 0..nt.clamp(1, 16) {
            sc.spawn(|| {
                let mut buf = vec![0u8; 4 << 20];
                loop {
                    let i = next.fetch_add(1, Relaxed);
                    if i >= files.len() { break; }
                    let Ok(mut fh) = std::fs::File::open(&files[i].0) else { errc.store(-1, Relaxed); continue };
                    'f: loop {
                        let k = match fh.read(&mut buf) { Ok(0) => break, Ok(k) => k, Err(_) => { errc.store(-2, Relaxed); break } };
                        let mut o = 0;
                        while o < k { let r = unsafe { write(fds[i], buf.as_ptr().add(o), k - o) }; if r <= 0 { errc.store(en(), Relaxed); break 'f; } o += r as usize; }
                    }
                }
            });
        }
    });
    let cms = ms(now() - t0);
    let close_all = |fds: &[i32]| for &x in fds { unsafe { close(x) }; };
    if errc.load(Relaxed) != 0 { close_all(&fds); return Err(format!("cp!{}", errc.load(Relaxed))); }
    let mk = |pfx: &str| -> Result<Vec<String>, String> {
        let mut links = vec![];
        for (i, (p, _)) in files.iter().enumerate() {
            let rel = p.strip_prefix(src).map_err(|_| "rel".to_string())?;
            let l = Path::new(dst).join(rel);
            if let Some(par) = l.parent() { let _ = std::fs::create_dir_all(par); }
            let _ = std::fs::remove_file(&l);
            symlink(format!("{pfx}/{}", fds[i]), &l).map_err(|e| format!("ln!{}", e.raw_os_error().unwrap_or(-1)))?;
            links.push(l.to_string_lossy().into_owned());
        }
        Ok(links)
    };
    let pid = unsafe { getpid() };
    let links = match mk(&format!("/proc/{pid}/fd")) { Ok(l) => l, Err(e) => { close_all(&fds); return Err(e); } };
    let Some(big) = links.first().cloned() else { close_all(&fds); return Err("empty".into()); };
    let r = run_child(me, &["--op", &big], &[], 5.0, None);
    kv!("SHM.op.p", "rc {} {}", r.rc, r.err.trim());
    let mut lnk = 'p';
    if r.rc != 0 {
        for &x in &fds { unsafe { fcntl(x, 2, 0) }; }
        if let Err(e) = mk("/proc/self/fd") { close_all(&fds); return Err(e); }
        let r = run_child(me, &["--op", &big], &[], 5.0, None);
        kv!("SHM.op.s", "rc {} {}", r.rc, r.err.trim());
        if r.rc != 0 { close_all(&fds); let _ = std::fs::remove_dir_all(dst); return Err(format!("op!{}", r.rc)); }
        lnk = 's';
    }
    Ok(Shm { dir: dst.to_string(), fds, ms: cms, lnk, big })
}
fn wpm(rs: &[&Rec]) -> String {
    let ok: Vec<&&Rec> = rs.iter().filter(|r| okr(r)).collect();
    match ok.iter().min_by(|a, b| a.wall.partial_cmp(&b.wall).unwrap_or(std::cmp::Ordering::Equal)) {
        Some(b) => format!("{}/{}/{}", f(b.wall), f(stage(b, "parse")), g3(b.ru[2])),
        None => rs.first().map(|r| badmark(r)).unwrap_or("-".into()),
    }
}

fn abbr(s: &str) -> String { let s = s.trim(); if s.is_empty() { return "-".into(); } s.split(|c: char| c == ' ' || c == '_' || c == ',').filter(|w| !w.is_empty()).filter_map(|w| w.chars().next()).map(|c| c.to_ascii_lowercase()).collect() }
fn vul(n: &str) -> String {
    let s = rdl(&format!("/sys/devices/system/cpu/vulnerabilities/{n}"));
    kv!(format!("QK.vul.{n}"), "{}", s);
    if s.starts_with("ERR") { "!".into() }
    else if s.starts_with("Not affected") { "na".into() }
    else if let Some(r) = s.strip_prefix("Mitigation: ") { format!("m{}", r.chars().filter(|c| c.is_ascii_alphanumeric()).take(3).collect::<String>().to_lowercase()) }
    else if s.starts_with("Vulnerable") { "v".into() }
    else { s.chars().filter(|c| c.is_ascii_alphanumeric()).take(3).collect() }
}
fn status_field(k: &str) -> String { rd("/proc/self/status").unwrap_or_default().lines().find_map(|l| l.strip_prefix(&format!("{k}:")).map(|x| x.trim().to_string())).unwrap_or_default() }
fn kfacts_static() -> String {
    let ex = |p: &str| if Path::new(p).exists() { "1" } else { "0" };
    let pro = rd("/sys/module/page_reporting/parameters/page_reporting_order").map(|s| s.trim().to_string()).unwrap_or("-".into());
    let lru = rdl("/sys/kernel/mm/lru_gen/enabled");
    let lru = if lru.starts_with("ERR") { "-".to_string() } else { let x = lru.trim_start_matches("0x").trim_start_matches('0'); if x.is_empty() { "0".into() } else { x.to_string() } };
    let ssb = status_field("Speculation_Store_Bypass"); let ib = status_field("SpeculationIndirectBranch");
    kv!("QK.spec", "ssb {:?} ib {:?} lru {:?}", ssb, ib, rdl("/sys/kernel/mm/lru_gen/enabled"));
    let cm = rdl("/sys/fs/cgroup/cpu.max");
    let cm = { let w: Vec<&str> = cm.split_whitespace().collect(); match (w.first(), w.get(1)) { (Some(&"max"), _) => "max".to_string(), (Some(q), Some(p)) => match (q.parse::<f64>(), p.parse::<f64>()) { (Ok(q), Ok(p)) if p > 0.0 => format!("{}c", f(q / p)), _ => "?".into() }, _ => "!".into() } };
    for l in rd("/proc/self/mountinfo").unwrap_or_default().lines() {
        let w: Vec<&str> = l.split_whitespace().collect();
        if w.len() > 4 && ["/", "/output", "/scratch", "/etc/hosts", "/data", "/tmp", "/dev/shm"].contains(&w[4]) { kv!("QK.mnt", "{}", l); }
    }
    format!("pr={}/{} vb={} cz={} lru={} sb={} ib={} vg={} vs={} v2={} cm={}", ex("/sys/kernel/mm/page_reporting"), pro, ex("/sys/bus/virtio/drivers/virtio_balloon"), ex("/proc/config.gz"), lru, abbr(&ssb), abbr(&ib),
        vul("gather_data_sampling"), vul("spec_store_bypass"), vul("spectre_v2"), cm)
}
fn pga_of(recs: &[Rec], name: &str) -> String {
    let v: Vec<i64> = recs.iter().filter(|r| r.v == name && okr(r) && r.pga >= 0).map(|r| r.pga).collect();
    match (v.iter().min(), v.iter().max()) { (Some(a), Some(b)) => format!("{}/{}", g3(*a as f64), g3(*b as f64)), _ => "-".into() }
}
fn kf_line(kf: &str, mn: &Var, data: &str, scratch: &str, n: &mut usize, pga: &str) -> String {
    let st = kfacts_static();
    *n += 1;
    let out = format!("{scratch}/out_{n}");
    let _ = std::fs::remove_dir_all(&out); let _ = std::fs::create_dir_all(&out);
    let mut envs: Vec<(&str, &str)> = vec![("KF_WRAP", mn.bin.as_str()), ("XL_TIMING", "1")];
    for (a, b) in &mn.env { envs.push((a.as_str(), b.as_str())); }
    let th0 = cg_thr();
    let r = run_child(kf, &[data, &out], &envs, 40.0, None);
    let th1 = cg_thr();
    let _ = std::fs::remove_dir_all(&out);
    let line = r.err.lines().find_map(|l| l.strip_prefix("kf ms: qk ")).unwrap_or("").to_string();
    kv!("QK.kf", "rc {} wall {:.2} line {:?}", r.rc, r.wall, line);
    if r.rc != 0 || line.is_empty() {
        let tail: String = r.err.chars().rev().take(300).collect::<Vec<_>>().into_iter().rev().collect();
        kv!("QK.kf.stderr", "{}", tail);
        return format!("qk kf=!rc{} {}", r.rc, st);
    }
    let rec = parse_run(&mn.name, "kf", &r);
    let g = |k: &str| line.split_whitespace().find_map(|t| t.strip_prefix(&format!("{k}="))).unwrap_or("?").to_string();
    let pa = g("pa").parse::<f64>().map(g3).unwrap_or("?".into());
    let thd = if th0.0 >= 0 && th1.0 >= 0 { format!("{}/{}", th1.0 - th0.0, f((th1.1 - th0.1) as f64 / 1e3)) } else { "!".into() };
    format!("qk up={} st={} {} th={} ex={} fw={} pp={} cs={} id={} mt={} mf={} pa={} pga={} run={}/{} rc={} ob={} vc={} mc={}",
        g("up"), g("st"), st, thd, g("ex"), g("fw"), g("pp"), g("cs").chars().take(3).collect::<String>(), g("id"), g("mt"), g("mf"), pa, pga,
        g("run").trim_end_matches("ms"), f(stage(&rec, "parse")), g("rc"), g("obusy"), g("vc"), g("mc"))
}
fn run_out(path: &str, args: &[String], timeout_s: f64) -> (i32, String, String, f64) {
    use std::process::{Command, Stdio};
    use std::os::unix::process::ExitStatusExt;
    let t0 = now();
    let mut ch = match Command::new(path).args(args).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn() { Ok(c) => c, Err(e) => return (-1000 - e.raw_os_error().unwrap_or(0), String::new(), format!("spawn: {e}"), 0.0) };
    KILLED.store(false, Relaxed);
    CHILD_DL.store(t0 + (timeout_s * 1e9) as u64, Release);
    CHILD.store(ch.id() as i32, Release);
    let (mut o, mut e) = (vec![], vec![]);
    if let Some(mut s) = ch.stdout.take() { let _ = s.read_to_end(&mut o); }
    if let Some(mut s) = ch.stderr.take() { let _ = s.read_to_end(&mut e); }
    let st = ch.wait();
    CHILD.store(0, Release);
    let rc = match st { Ok(s) => s.code().unwrap_or_else(|| -s.signal().unwrap_or(0)), Err(_) => -2000 };
    (rc, String::from_utf8_lossy(&o).into_owned(), String::from_utf8_lossy(&e).into_owned(), ms(now() - t0))
}
fn dsb_line(bin: &str, t: &Topo) -> String {
    let args: Vec<String> = t.cores.iter().find(|g| g.len() >= 2).map(|g| vec![g[0].to_string(), g[1].to_string()]).unwrap_or_default();
    let (rc, o, e, w) = run_out(bin, &args, 25.0);
    kv!("QU.dsb", "rc {} wall {:.1} args {:?} out {:?} err {:?}", rc, w, args, o.trim(), e.trim());
    let o = o.trim();
    if rc != 0 || !o.starts_with("dsb pair=") { return format!("qu !rc{} {}", rc, sanitize(&e.chars().take(60).collect::<String>())); }
    format!("qu {} s={}", o.replacen("dsb pair=", "p=", 1).replace(" ticks/window:", "").replace(" W", " "), f(w / 1e3))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    child_mode(&args);
    START.store(now(), Relaxed);
    REAL0.store(clk_ns(0) as u64, Relaxed);
    BOOT0.store(clk_ns(7) as u64, Relaxed);
    for s in [4, 6, 7, 8, 11] { unsafe { signal(s, on_sig as extern "C" fn(i32) as usize) }; }
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.into());
    let data = env("DATA_DIR", "/data");
    let out = env("OUTPUT_DIR", "/output");
    let scratch = env("SCRATCH_DIR", "/scratch");
    let bindir = env("PROBE_BIN_DIR", "/p");
    let genbin = env("PROBE_GEN", "/generate");
    let me = std::env::current_exe().ok().and_then(|p| p.to_str().map(|s| s.to_string())).unwrap_or_else(|| env("PROBE_SELF", "/probe"));
    let wd: f64 = env("PROBE_WATCHDOG_S", "150").parse().unwrap_or(150.0);
    let lim: f64 = env("PROBE_BUDGET_S", "75").parse().unwrap_or(75.0);
    let rounds: usize = env("PROBE_ROUNDS", "5").parse().unwrap_or(5);
    let only = std::env::var("PROBE_ONLY").ok();
    let run = |s: &str| only.as_ref().map(|o| o.split(',').any(|x| x == s)).unwrap_or(true);
    let (qo0, lsn) = qo_static(&out, &scratch, &data);
    let (qd0, cmdl) = qd_static(&data);
    if let Ok(mut g) = OUTDIR.lock() { *g = out.clone(); }
    step(1);
    let q3s = q3_static();
    setl(2, &q3s);
    setl(16, &qo0);
    setl(17, &format!("qd {qd0}"));
    setl(18, &format!("ql{} cmd={}", lsn, cmdl.chars().take(240).collect::<String>()));
    snapshot("start");
    let _ = grab(0); let r0 = write_snap(); WGUARD.store(false, Release);
    kv!("T.early_tables", "{:?}", r0);
    watchdog(wd);
    let t = topo();
    let st0 = steal_ticks(&t.allowed); let th0 = cg_thr();
    let sib: Vec<String> = t.cores.iter().map(|g| list_str(g)).collect();
    setl(8, &sysline(&t));
    step(8);
    if run("qm") { let s = qm_line(&me, "qm0", &scratch, lim, false); setl(15, &s); flush(); }
    step(2);
    let mut d20: Option<String> = std::env::var("PROBE_D20").ok();
    let mut gl = String::from("g");
    if d20.is_none() && run("gen") {
        let dir = format!("{scratch}/d20");
        let free = statfree(&scratch);
        let ev = env("PROBE_GEN_EV", "20000000"); let ck = env("PROBE_GEN_CK", "5000000");
        if free >= 0.0 && free < 2.5 { gl.push_str(&format!(" gen=skip free={}GB", f(free))); }
        else {
            let r = run_child(&genbin, &["--seed", "20", "--events", &ev, "--clicks", &ck, "--output", &dir, "--threads", &t.allowed.len().max(1).to_string()], &[], 80.0, None);
            let files = csv_files(&dir);
            let mb: u64 = files.iter().map(|x| x.1).sum();
            kv!("G20", "rc {} wall {:.1} ms files {} bytes {} killed {} stderr {}", r.rc, r.wall, files.len(), mb, r.killed, r.err.chars().take(300).collect::<String>());
            gl.push_str(&format!(" gen={}s rc={} MB={}", f(r.wall / 1e3), r.rc, f(mb as f64 / 1e6)));
            if r.rc == 0 && mb > 0 {
                step(3);
                let (sm, _) = sync_all(&dir);
                gl.push_str(&format!(" sync={} res={}%", f(sm), f(resident_pct(&files))));
                d20 = Some(dir);
            } else { gl.push_str(" GENFAIL"); let _ = std::fs::remove_dir_all(&dir); }
        }
    }
    setl(4, &gl);
    let vs = vars(&bindir);
    kv!("P.variants", "{}", vs.iter().map(|x| format!("{}:{}:{:?}", x.name, x.bin, x.env)).collect::<Vec<_>>().join(" "));
    let mn_name = env("PROBE_MN", "mn"); let dt_name = env("PROBE_DT", "dt");
    let vmn = vs.iter().position(|v| v.name == mn_name);
    let vdt = vs.iter().position(|v| v.name == dt_name);
    let mut shm: Option<Shm> = None; let mut shm_note = String::new();
    if let Some(d) = &d20 {
        let need = dir_stats(d).1 as f64;
        let av = mem_avail();
        kv!("SHM.mem", "need {} avail {}", need, av);
        if !run("shm") { shm_note = "shm=off".into(); }
        else if el_s() > lim - 15.0 { shm_note = "shm=cut".into(); }
        else if av < 2.0 * need + 1e9 { shm_note = format!("shm=mem{}G", f(av / 1e9)); }
        else {
            step(10);
            match shm_copy(d, &format!("{scratch}/d20m"), &me, t.allowed.len().max(1)) { Ok(s) => shm = Some(s), Err(e) => shm_note = format!("shm={e}") }
        }
        kv!("SHM.res", "{} {}", shm.as_ref().map(|s| format!("{} fds {} ms {:.1} lnk {}", s.dir, s.fds.len(), s.ms, s.lnk)).unwrap_or_default(), shm_note);
    }
    let tps = shm.as_ref().map(|s| two_page(&s.big)).unwrap_or_default();
    let (qd, mode, alt) = match (&shm, &d20) { (Some(s), Some(d)) => (s.dir.clone(), "n", Some(d.clone())), (None, Some(d)) => (d.clone(), "n", None), _ => (data.clone(), "dv", None) };
    let src = if shm.is_some() { "shm" } else if d20.is_some() { "pc" } else { "dv" };
    let set_qd = |recs: &[Rec], alts: &[Rec]| {
        let mine: Vec<&Rec> = recs.iter().filter(|r| r.v == mn_name).collect();
        let other: Vec<&Rec> = alts.iter().collect();
        let (sm, pc) = if shm.is_some() { (wpm(&mine), wpm(&other)) } else { ("-".to_string(), wpm(&mine)) };
        let extra = match &shm { Some(s) => format!("cp={}s l={} 2s={}", f(s.ms / 1e3), s.lnk, tps), None => shm_note.clone() };
        setl(17, &format!("qd s={src} sm={sm} pc={pc} {extra} {qd0}"));
    };
    set_qd(&[], &[]);
    let mut recs: Vec<Rec> = vec![]; let mut n = 0usize; let mut alts: Vec<Rec> = vec![];
    step(4);
    if run("q1") && !vs.is_empty() {
        'o: for r in 0..rounds {
            for v in &vs {
                if el_s() > lim + 25.0 { put(9, "q1 cut"); break 'o; }
                recs.push(pipe_run(v, mode, &qd, &scratch, &mut n, None, None, None));
                q1_lines(&vs, &recs, mode);
                flush();
            }
            if r < 3 { if let (Some(a), Some(i)) = (&alt, vmn) {
                if el_s() > lim + 25.0 { break 'o; }
                alts.push(pipe_run(&vs[i], "pc", a, &scratch, &mut n, None, None, None));
                set_qd(&recs, &alts); flush();
            } }
        }
        set_qd(&recs, &alts); flush();
        if let Ok(g) = REFROWS.lock() { if let Some(x) = *g { put(4, &format!(" rows={}/{}", x.0.unwrap_or(0), x.1.unwrap_or(0))); } }
        put(4, &format!("nv={} q1end={}s", vs.len(), f(el_s())));
        let mut q4 = vec![];
        for nm in ["ra", mn_name.as_str()] {
            if let Some(b) = recs.iter().filter(|r| r.v == nm && okr(r)).min_by(|a, b| a.wall.partial_cmp(&b.wall).unwrap_or(std::cmp::Ordering::Equal)) { q4.push(format!("{} {}", nm, ru_str(b))); }
        }
        setl(3, &format!("q4 {}", q4.join(" ")));
        flush();
    }
    step(5);
    if run("q2") {
        let q2names = env("PROBE_Q2", &format!("{mn_name},l8,l10"));
        let mut q2v: Vec<&Var> = q2names.split(',').filter_map(|nm| vs.iter().find(|v| v.name == nm)).collect();
        if q2v.is_empty() { if let Some(b) = vs.iter().find(|v| v.name == mn_name).or(vs.first()) { q2v.push(b); } }
        let mut cfg: Vec<(&str, Vec<usize>, usize)> = vec![("8", t.allowed.clone(), env("NCPU", "8").parse().unwrap_or(8))];
        if t.cores.len() >= 4 { cfg.push(("4", t.cores.iter().take(4).map(|g| g[0]).collect(), 4)); }
        kv!("Q2.cfg", "{:?} vars {:?}", cfg, q2v.iter().map(|v| v.name.as_str()).collect::<Vec<_>>());
        let cpus_s = cfg.iter().map(|c| format!("{}={}", c.0, list_str(&c.1))).collect::<Vec<_>>().join(" ");
        setl(1, &format!("q2 -"));
        setl(7, &format!("q2all sib={} cpus {}", sib.join("/"), cpus_s));
        let mut q2r: Vec<Rec> = vec![];
        'p: for _ in 0..rounds {
            for bv in &q2v {
                for c in &cfg {
                    if el_s() > lim + 25.0 { put(9, "q2 cut"); break 'p; }
                    let mut r = pipe_run(bv, c.0, &qd, &scratch, &mut n, Some(&c.1), Some(c.2), None);
                    r.v = format!("{}{}", bv.name, c.0);
                    q2r.push(r);
                    let (mut a, mut b, mut x) = (vec![], vec![], vec![]);
                    for v in &q2v {
                        let mut per = vec![];
                        for c in &cfg {
                            let key = format!("{}{}", v.name, c.0);
                            let rs: Vec<&Rec> = q2r.iter().filter(|r| r.v == key).collect();
                            if rs.is_empty() { continue; }
                            let (m, y, _, w) = summ(&key, &rs);
                            per.push(m[key.len() + 1..].to_string()); b.push(y); x.push(w);
                        }
                        if !per.is_empty() { a.push(format!("{}={}", v.name, per.join(","))); }
                    }
                    setl(1, &format!("q2 {}", a.join(" ")));
                    setl(7, &format!("q2all {} x {} sib={} cpus {}", b.join(" "), x.join(" "), sib.join("/"), cpus_s));
                    flush();
                }
            }
        }
    }
    step(12);
    if run("kf") {
        if let Some(i) = vmn {
            let pga = pga_of(&recs, &mn_name);
            let s = if el_s() > lim + 28.0 { format!("qk kf=cut {}", kfacts_static()) } else { kf_line(&env("PROBE_KF", "/kf"), &vs[i], &qd, &scratch, &mut n, &pga) };
            setl(20, &s); flush();
        }
    }
    step(9);
    if run("qm") { let s = qm_line(&me, "qm", &scratch, lim + 30.0, true); setl(14, &s); flush(); }
    step(11);
    if run("qo") {
        let roles: Vec<(&str, &Var)> = [("mn", vmn), ("dt", vdt)].into_iter().filter_map(|(r, i)| i.map(|i| (r, &vs[i]))).collect();
        if !roles.is_empty() { let w = qo_wb(&roles, &qd, &out, &scratch, &mut n, lim + 32.0); setl(16, &format!("{qo0} {w}")); flush(); }
    }
    step(13);
    if run("dsb") {
        let s = if el_s() > lim + 35.0 { "qu cut".to_string() } else { dsb_line(&env("PROBE_DSB", "/dsb"), &t) };
        setl(21, &s); flush();
    }
    step(6);
    if run("q3") { let c = q3_children(&me, lim + 38.0); setl(2, &format!("{} {}", q3s, c)); flush(); }
    if let Some(s) = &shm { let _ = std::fs::remove_dir_all(&s.dir); }
    if let Some(d) = &d20 { if std::env::var("PROBE_D20").is_err() { let _ = std::fs::remove_dir_all(d); } }
    step(7);
    {
        let cg = |p: &str| rdl(&format!("/sys/fs/cgroup/{p}"));
        put(13, &format!("end el={}s mem.peak={} cpu.stat={}", f(el_s()), cg("memory.peak"), cg("cpu.stat").replace('\n', " ")));
        let st1 = steal_ticks(&t.allowed); let th1 = cg_thr();
        let w = format!("w={}/{}/{}", if st0 >= 0 && st1 >= 0 { ((st1 - st0) * 10).to_string() } else { "!".into() }, th1.0 - th0.0, f((th1.1 - th0.1) as f64 / 1e3));
        let has = LN.lock().map(|g| !g[20].is_empty()).unwrap_or(false);
        if has { put(20, &format!(" {w}")); } else { setl(20, &format!("qk {} {}", w, kfacts_static())); }
    }
    let text = compose("done").unwrap_or_default();
    snapshot("done");
    let ok = grab(3000);
    let r = write_snap();
    raw(b"===== TEXT START\n"); raw(text.as_bytes()); raw(b"===== TEXT END\n");
    kv!("T.tables", "{:?} chars {} grab {}", r, text.len(), ok);
    unsafe { _exit(0) }
}
