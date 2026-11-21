#!/usr/bin/env python3
"""Convert a time measured on nolimit-sim (the scorer-mirror VM) to an expected
leaderboard time, using the logged calibration pairs in tools/calibration.csv.

    to_board.py SIM_SECONDS [--events N]
        SIM_SECONDS measured on nolimit-sim with nolimit-run on a dataset of N
        events (default 20M, the practice size). Prints the expected practice
        board time and the expected final (60M events) time.

factor = median(board / sim) over logged submissions in the same speed regime as
SIM_SECONDS: pairs whose sim time is within 5x of it (normalised to 20M events).
A 2-minute CPU-bound run and a 2-second run differ in how much fixed container
and memory overhead they carry, so their ratios don't transfer (reference: 1.00,
Rust v9: 0.878). With no pair in range, all pairs are used. The organizers say the
same image can score up to ~20% apart across scoring machines, so the printed
range is +-20%. Time is scaled linearly with events (the organizers: "time grows
at least in step with the data"), so final estimates are lower bounds.
"""
import csv
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PRACTICE_EVENTS = 20_000_000
FINAL_EVENTS = 60_000_000
FINAL_CAP_S = 180


REGIME = 5.0  # pairs count when their 20M-normalised sim time is within this factor


def factor(sim20=None):
    """Median board/sim ratio over pairs near sim20 (sim seconds at 20M events).

    Returns (factor, pairs used, pairs logged)."""
    with open(os.path.join(HERE, "calibration.csv")) as handle:
        rows = [row for row in csv.reader(line for line in handle if not line.startswith("#"))]
    pairs = []
    for r in rows:
        if r and r[4] and r[5]:
            sim, board = float(r[4]), float(r[5])
            events = int(r[3]) if r[3] else PRACTICE_EVENTS
            pairs.append((sim * PRACTICE_EVENTS / events, board / sim))
    near = [ratio for s, ratio in pairs if sim20 is None or 1 / REGIME <= s / sim20 <= REGIME]
    used = near or [ratio for _, ratio in pairs]
    return statistics.median(used), len(used), len(pairs)


def main(argv):
    sim = float(argv[0])
    events = int(argv[argv.index("--events") + 1]) if "--events" in argv else PRACTICE_EVENTS
    f, n, total = factor(sim * PRACTICE_EVENTS / events)
    practice = sim * f * PRACTICE_EVENTS / events
    final = sim * f * FINAL_EVENTS / events
    print(f"factor {f:.3f} (from {n} of {total} calibration pair{'s' * (total != 1)}, same speed regime)")
    print(f"practice board (~20M events): {practice:8.2f} s   (range {practice * 0.8:.2f} - {practice * 1.2:.2f})")
    print(f"final (~60M events):          {final:8.2f} s   (range {final * 0.8:.2f} - {final * 1.2:.2f})"
          + ("   !! over the 3-minute final cap" if final > FINAL_CAP_S else ""))


if __name__ == "__main__":
    main(sys.argv[1:])
