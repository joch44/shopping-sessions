//! Speed floors. `floor DATA_DIR THREADS MODE` where MODE is
//!   read  : pread every byte of events/, clicks/, links/ into per-thread buffers (no work)
//!   count : same, and count '\n' in each buffer (the cheapest possible pass)
//! Work is split by bytes, not by file, in 8 MiB chunks pulled from a shared counter.
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

const CHUNK: u64 = 8 << 20;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (dir, threads, mode) = (&args[1], args[2].parse::<usize>().unwrap(), args[3].as_str());
    let start = Instant::now();
    let mut files = Vec::new();
    for folder in ["events", "clicks", "links"] {
        let Ok(entries) = std::fs::read_dir(format!("{dir}/{folder}")) else { continue };
        for entry in entries {
            let path = entry.unwrap().path();
            let len = std::fs::metadata(&path).unwrap().len();
            files.push((File::open(&path).unwrap(), len));
        }
    }
    let mut jobs = Vec::new();
    for (index, (_, len)) in files.iter().enumerate() {
        let mut at = 0;
        while at < *len {
            jobs.push((index, at, CHUNK.min(len - at)));
            at += CHUNK;
        }
    }
    jobs.sort_by_key(|&(_, _, len)| std::cmp::Reverse(len));
    let total: u64 = files.iter().map(|(_, len)| len).sum();
    let next = AtomicUsize::new(0);
    let newlines = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                let mut buffer = vec![0u8; CHUNK as usize];
                let mut local = 0usize;
                loop {
                    let at = next.fetch_add(1, Ordering::Relaxed);
                    let Some(&(file, offset, len)) = jobs.get(at) else { break };
                    let slice = &mut buffer[..len as usize];
                    files[file].0.read_exact_at(slice, offset).unwrap();
                    if mode == "count" {
                        local += slice.iter().filter(|&&b| b == b'\n').count();
                    }
                }
                newlines.fetch_add(local, Ordering::Relaxed);
            });
        }
    });
    let seconds = start.elapsed().as_secs_f64();
    println!("{mode} threads={threads} bytes={total} newlines={} seconds={seconds:.3} GB/s={:.2}",
             newlines.load(Ordering::Relaxed), total as f64 / seconds / 1e9);
}
