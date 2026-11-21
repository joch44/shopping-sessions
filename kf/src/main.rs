use std::arch::x86_64::{__cpuid, __cpuid_count, _rdtsc};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Instant;

extern "C" {
    fn syscall(n: i64, ...) -> i64;
}
fn rd(p: &str) -> String { std::fs::read_to_string(p).unwrap_or_default().trim().to_string() }
fn cpus() -> Vec<usize> {
    let mut m = [0u64; 16];
    let r = unsafe { syscall(204, 0i64, 128usize, m.as_mut_ptr()) };
    let mut v = vec![];
    if r > 0 { for i in 0..1024 { if m[i / 64] >> (i % 64) & 1 == 1 { v.push(i); } } }
    v
}
fn pin(c: usize) { let mut m = [0u64; 16]; m[c / 64] |= 1 << (c % 64); unsafe { syscall(203, 0i64, 128usize, m.as_ptr()); } }
fn med(v: &mut Vec<f64>) -> f64 { v.sort_by(|a, b| a.partial_cmp(b).unwrap()); v[v.len() / 2] }
fn sig(a: u32, b: u32, c: u32) -> String {
    let mut s = vec![];
    for x in [a, b, c] { s.extend_from_slice(&x.to_le_bytes()); }
    String::from_utf8_lossy(&s).trim_matches(char::from(0)).to_string()
}
fn irqs(our: &[usize]) -> Vec<(String, u64, u64)> {
    let t = rd("/proc/interrupts");
    let mut lines = t.lines();
    let hdr: Vec<usize> = lines.next().unwrap_or("").split_whitespace().filter_map(|w| w.strip_prefix("CPU")?.parse().ok()).collect();
    let mut out = vec![];
    for l in lines {
        let mut w = l.split_whitespace();
        let name = w.next().unwrap_or("").trim_end_matches(':').to_string();
        if !["LOC", "RES", "CAL", "TLB", "PIN", "NPI", "PIW", "HYP", "HVS", "IWI", "THR", "MCP"].contains(&name.as_str()) { continue; }
        let (mut a, mut b) = (0u64, 0u64);
        for (i, x) in w.take(hdr.len()).enumerate() {
            let x: u64 = x.parse().unwrap_or(0);
            if our.contains(&hdr[i]) { a += x } else { b += x }
        }
        out.push((name, a, b));
    }
    out
}
fn stat(our: &[usize]) -> [u64; 4] {
    let mut r = [0u64; 4];
    for l in rd("/proc/stat").lines() {
        if !l.starts_with("cpu") || l.starts_with("cpu ") { continue; }
        let w: Vec<&str> = l.split_whitespace().collect();
        let c: usize = w[0][3..].parse().unwrap_or(9999);
        let f: Vec<u64> = w[1..].iter().map(|x| x.parse().unwrap_or(0)).collect();
        let busy = f[0] + f[1] + f[2] + f[5] + f[6];
        if our.contains(&c) { r[0] += f[7]; r[1] += busy } else { r[2] += busy; r[3] += f[3] + f[4] }
    }
    r
}
static FX: AtomicU32 = AtomicU32::new(0);
static T1: AtomicU64 = AtomicU64::new(0);
static PP: AtomicU64 = AtomicU64::new(0);

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let cs = cpus();
    let mut q = String::from("qk");
    let mut e = |k: &str, v: String| { eprintln!("KF {} = {}", k, v); };
    let l1 = unsafe { __cpuid(1) };
    let hv = l1.ecx >> 31 & 1;
    let h0 = unsafe { __cpuid(0x4000_0000) };
    let hs = sig(h0.ebx, h0.ecx, h0.edx);
    let h1 = unsafe { __cpuid(0x4000_0001) };
    let tsck = if h0.eax >= 0x4000_0010 { unsafe { __cpuid(0x4000_0010) }.eax } else { 0 };
    let l7 = unsafe { __cpuid_count(7, 0) };
    let e7 = unsafe { __cpuid(0x8000_0007) };
    e("cpuid", format!("hv={} sig={} max={:#x} kvm_feat_eax={:#x} hints_edx={:#x} tsc_khz={} leaf7 ebx={:#x} ecx={:#x} edx={:#x} invtsc={}", hv, hs, h0.eax, h1.eax, h1.edx, tsck, l7.ebx, l7.ecx, l7.edx, e7.edx >> 8 & 1));
    let mut v = vec![];
    for _ in 0..7 {
        let t0 = Instant::now();
        for _ in 0..200 { unsafe { __cpuid(0) }; }
        v.push(t0.elapsed().as_nanos() as f64 / 200.0);
    }
    let cpuid_ns = med(&mut v);
    let mut v = vec![];
    for _ in 0..7 {
        let t0 = Instant::now();
        let mut x = 0u128;
        for _ in 0..2000 { x += Instant::now().elapsed().as_nanos(); }
        std::hint::black_box(x);
        v.push(t0.elapsed().as_nanos() as f64 / 4000.0);
    }
    let cg_ns = med(&mut v);
    let mut v = vec![];
    for _ in 0..7 {
        let t0 = Instant::now();
        for _ in 0..2000 { unsafe { syscall(110) }; }
        v.push(t0.elapsed().as_nanos() as f64 / 2000.0);
    }
    let sc_ns = med(&mut v);
    let (c0, t0) = (unsafe { _rdtsc() }, Instant::now());
    std::thread::sleep(std::time::Duration::from_millis(20));
    let ghz = (unsafe { _rdtsc() } - c0) as f64 / t0.elapsed().as_nanos() as f64;
    let (ca, cb) = (cs[0], *cs.get(1).unwrap_or(&cs[0]));
    let base = Instant::now();
    let h = std::thread::spawn(move || {
        pin(cb);
        for i in 1..=41u32 {
            while FX.load(Ordering::Acquire) != i * 2 - 1 {
                unsafe { syscall(202, FX.as_ptr(), 128i64, FX.load(Ordering::Acquire), 0usize, 0usize, 0usize) };
            }
            T1.store(base.elapsed().as_nanos() as u64, Ordering::Release);
            FX.store(i * 2, Ordering::Release);
        }
    });
    let mut full = [0u64; 16];
    unsafe { syscall(204, 0i64, 128usize, full.as_mut_ptr()) };
    pin(ca);
    let mut wl = vec![];
    for i in 1..=41u32 {
        std::thread::sleep(std::time::Duration::from_micros(if i % 2 == 0 { 2000 } else { 300 }));
        let t = base.elapsed().as_nanos() as u64;
        FX.store(i * 2 - 1, Ordering::Release);
        unsafe { syscall(202, FX.as_ptr(), 129i64, 1i64) };
        while FX.load(Ordering::Acquire) != i * 2 { std::hint::spin_loop(); }
        if i > 1 { wl.push((T1.load(Ordering::Acquire) - t) as f64 / 1000.0); }
    }
    h.join().ok();
    let wmax = wl.iter().cloned().fold(0.0, f64::max);
    let wmed = med(&mut wl);
    let h = std::thread::spawn(move || {
        pin(cb);
        for i in 0..20000u64 {
            while PP.load(Ordering::Acquire) != 2 * i + 1 { std::hint::spin_loop(); }
            PP.store(2 * i + 2, Ordering::Release);
        }
    });
    let t0 = Instant::now();
    for i in 0..20000u64 {
        PP.store(2 * i + 1, Ordering::Release);
        while PP.load(Ordering::Acquire) != 2 * i + 2 { std::hint::spin_loop(); }
    }
    let pp_ns = t0.elapsed().as_nanos() as f64 / 20000.0;
    h.join().ok();
    unsafe { syscall(203, 0i64, 128usize, full.as_ptr()) };
    let up = rd("/proc/uptime").split_whitespace().next().unwrap_or("?").to_string();
    let csrc = rd("/sys/devices/system/clocksource/clocksource0/current_clocksource");
    let idle = rd("/sys/devices/system/cpu/cpuidle/current_driver");
    let gov = rd("/sys/devices/system/cpu/cpuidle/current_governor_ro");
    let gov = if gov.is_empty() { rd("/sys/devices/system/cpu/cpuidle/current_governor") } else { gov };
    let mi = rd("/proc/meminfo");
    let mget = |k: &str| mi.lines().find(|l| l.starts_with(k)).and_then(|l| l.split_whitespace().nth(1)).and_then(|x| x.parse::<u64>().ok()).unwrap_or(0) / 1024;
    let vm = rd("/proc/vmstat");
    let vget = |k: &str| vm.lines().find(|l| l.split_whitespace().next() == Some(k)).and_then(|l| l.split_whitespace().nth(1)).unwrap_or("?").to_string();
    let ncpu_vm = rd("/proc/cpuinfo").lines().filter(|l| l.starts_with("processor")).count();
    let ci = rd("/proc/cpuinfo");
    let cget = |k: &str| ci.lines().find(|l| l.starts_with(k)).and_then(|l| l.split(':').nth(1)).unwrap_or("?").trim().to_string();
    let vul = |n: &str| rd(&format!("/sys/devices/system/cpu/vulnerabilities/{}", n));
    let st = rd("/proc/self/status");
    let ssb = st.lines().find(|l| l.starts_with("Speculation_Store_Bypass")).unwrap_or("").to_string();
    let ib = st.lines().find(|l| l.starts_with("SpeculationIndirectBranch")).unwrap_or("").to_string();
    let nodes = rd("/sys/devices/system/node/online");
    let hz_guess = 0;
    e("files", format!("uptime={} clocksource={} avail={} idle={} gov={} haltpoll={} node={} vmcpus={} model={} microcode={}",
        up, csrc, rd("/sys/devices/system/clocksource/clocksource0/available_clocksource"), idle, gov, rd("/sys/module/haltpoll/parameters/guest_halt_poll_ns"), nodes, ncpu_vm, cget("model name"), cget("microcode")));
    e("mem", format!("MemTotal={}M MemFree={}M Cached={}M Shmem={}M AnonHuge={}M pgalloc_normal={} pgfault={} thp_fault_alloc={} thp_fault_fallback={} compact_stall={} balloon_inflate={} nr_free_pages={}",
        mget("MemTotal"), mget("MemFree"), mget("Cached"), mget("Shmem:"), mget("AnonHugePages"), vget("pgalloc_normal"), vget("pgfault"), vget("thp_fault_alloc"), vget("thp_fault_fallback"), vget("compact_stall"), vget("balloon_inflate"), vget("nr_free_pages")));
    e("cmdline", rd("/proc/cmdline"));
    e("thp", format!("enabled={} defrag={} shmem={} khp_defrag={}", rd("/sys/kernel/mm/transparent_hugepage/enabled"), rd("/sys/kernel/mm/transparent_hugepage/defrag"), rd("/sys/kernel/mm/transparent_hugepage/shmem_enabled"), rd("/sys/kernel/mm/transparent_hugepage/khugepaged/defrag")));
    for n in ["gather_data_sampling", "spec_store_bypass", "spectre_v2", "mmio_stale_data", "tsx_async_abort", "mds", "retbleed", "indirect_target_selection", "its", "reg_file_data_sampling", "vmscape", "tsa"] {
        let s = vul(n);
        if !s.is_empty() { e(&format!("vul.{}", n), s); }
    }
    e("spec", format!("{} | {}", ssb, ib));
    e("cpus", format!("{:?} interrupts_bytes={} stat_bytes={}", cs, rd("/proc/interrupts").len(), rd("/proc/stat").len()));
    let _ = hz_guess;
    let short = |s: &str| s.split_whitespace().next().unwrap_or("-").to_string();
    q += &format!(" up={:.0} sig={} pv={:x}/{:x} tk={} cs={} id={} ex={:.0} cg={:.0} sc={:.0} tsc={:.2} fw={:.0}/{:.0} pp={:.0} mt={}G mf={}G pa={} n={} vc={} mc={} gds={} ssb={}",
        up.parse::<f64>().unwrap_or(-1.0), if hs.is_empty() { "-".into() } else { hs.chars().take(4).collect::<String>() }, h1.eax, h1.edx, tsck, short(&csrc), short(&idle),
        cpuid_ns, cg_ns, sc_ns, ghz, wmed, wmax, pp_ns, mget("MemTotal") / 1024, mget("MemFree") / 1024, vget("pgalloc_normal"), nodes, ncpu_vm, cget("microcode"),
        short(&vul("gather_data_sampling")), ssb.split(':').nth(1).map(|s| s.trim().replace(' ', "_")).unwrap_or("-".into()));
    let wrap = std::env::var("KF_WRAP").ok();
    let pos = args.iter().position(|a| a == "--");
    if pos.is_some() || wrap.is_some() {
        let (prog, rest): (String, Vec<String>) = match (&wrap, pos) { (Some(w), _) => (w.clone(), args[1..].to_vec()), (None, Some(p)) => (args[p + 1].clone(), args[p + 2..].to_vec()) , _ => unreachable!() };
        let i0 = irqs(&cs);
        let s0 = stat(&cs);
        let t0 = Instant::now();
        let st = std::process::Command::new(&prog).args(&rest).status();
        let wall = t0.elapsed().as_secs_f64() * 1000.0;
        let i1 = irqs(&cs);
        let s1 = stat(&cs);
        let mut d = String::new();
        for (a, b) in i0.iter().zip(i1.iter()) {
            if b.1 > a.1 || b.2 > a.2 { d += &format!(" {}={}/{}", a.0, b.1 - a.1, b.2 - a.2); }
        }
        let tk = 10.0;
        q += &format!(" | run={:.0}ms rc={} st={:.0} busy={:.0} obusy={:.0} oidle={:.0}{}", wall, st.map(|s| s.code().unwrap_or(-1)).unwrap_or(-2),
            (s1[0] - s0[0]) as f64 * tk, (s1[1] - s0[1]) as f64 * tk, (s1[2] - s0[2]) as f64 * tk, (s1[3] - s0[3]) as f64 * tk, d);
    }
    println!("{}", q);
    eprintln!("kf ms: {}", q);
}
