#!/usr/bin/env python3
"""Convert a time measured on nolimit-sim (the scorer-mirror VM) to an expected
leaderboard time, using the logged calibration pairs in tools/calibration.csv.

    to_board.py SIM_SECONDS [--events N]
        SIM_SECONDS measured on nolimit-sim with nolimit-run on a dataset of N
        events (default 20M, the practice size). Prints the expected practice
        board time and the expected final (60M events) time.

factor = median(board / sim) over logged submissions. The organizers say the
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


def factor():
    with open(os.path.join(HERE, "calibration.csv")) as handle:
        rows = [row for row in csv.reader(line for line in handle if not line.startswith("#"))]
    ratios = [float(r[5]) / float(r[4]) for r in rows if r and r[4] and r[5]]
    return statistics.median(ratios), len(ratios)


def main(argv):
    sim = float(argv[0])
    events = int(argv[argv.index("--events") + 1]) if "--events" in argv else PRACTICE_EVENTS
    f, n = factor()
    practice = sim * f * PRACTICE_EVENTS / events
    final = sim * f * FINAL_EVENTS / events
    print(f"factor {f:.3f} (from {n} calibration pair{'s' * (n != 1)})")
    print(f"practice board (~20M events): {practice:8.2f} s   (range {practice * 0.8:.2f} - {practice * 1.2:.2f})")
    print(f"final (~60M events):          {final:8.2f} s   (range {final * 0.8:.2f} - {final * 1.2:.2f})"
          + ("   !! over the 3-minute final cap" if final > FINAL_CAP_S else ""))


if __name__ == "__main__":
    main(sys.argv[1:])
