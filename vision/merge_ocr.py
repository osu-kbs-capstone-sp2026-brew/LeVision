#!/usr/bin/env python3
"""
merge_ocr.py
============
Phase 4 — Merge OCR clock timeline with ground-truth game state.

Reads:
  vision/debug_outputs/clock_timeline.json   (video_sec → quarter + clock)
  vision/data/nba/ground_truth_timeline.json (game_sec  → full state)

For each video_sec:
  1. Parse the OCR quarter + clock into a game_elapsed_sec index
  2. Snap to the nearest valid game_sec in the ground-truth timeline
     (handles OCR drift / missing seconds gracefully)
  3. Attach the state snapshot

Output:
  vision/data/nba/video_state_map.json   {video_sec: state_snapshot}
"""
from __future__ import annotations

import json
from pathlib import Path

VISION_DIR = Path(__file__).parent
DATA_DIR   = VISION_DIR / "data" / "nba"
DEBUG_DIR  = VISION_DIR / "debug_outputs"

QUARTER_MAP = {"1ST": 1, "2ND": 2, "3RD": 3, "4TH": 4, "OT": 5}
PERIOD_SECS = 720   # 12 min per NBA quarter


def clock_remaining(clock_str: str) -> int:
    """'MM:SS' → integer seconds remaining in the period."""
    try:
        m, s = clock_str.split(":")
        return int(m) * 60 + int(s)
    except Exception:
        return 720  # default to start-of-period if unparseable


def ocr_to_game_sec(quarter: str | None, clock: str | None) -> int | None:
    """Convert OCR (quarter, clock) → game_elapsed_seconds.

    Returns None if either field is None (pre-game / OCR miss).
    """
    if not quarter or not clock:
        return None
    period = QUARTER_MAP.get(str(quarter).upper())
    if period is None:
        return None
    remaining = clock_remaining(clock)
    return (period - 1) * PERIOD_SECS + (PERIOD_SECS - remaining)


def snap(target_sec: int, available: list[int]) -> int:
    """Return the closest game_sec in *available* to *target_sec*."""
    return min(available, key=lambda s: abs(s - target_sec))


def merge(
    ocr_timeline: list[dict],
    gt_timeline: dict[str, dict],
) -> dict[str, dict]:
    """Return {video_sec: state_snapshot} for every entry in ocr_timeline."""
    available_secs = sorted(int(k) for k in gt_timeline.keys())

    result: dict[str, dict] = {}

    null_count = 0
    drift_buckets: list[int] = []

    for entry in ocr_timeline:
        vsec  = entry["video_sec"]
        qtr   = entry.get("quarter")
        clock = entry.get("clock")

        target = ocr_to_game_sec(qtr, clock)

        if target is None:
            # Pre-game or OCR miss — use game_sec 0 (tip-off state)
            gsec = 0
            null_count += 1
        else:
            gsec = snap(target, available_secs)
            drift = abs(gsec - target)
            drift_buckets.append(drift)

        # Attach the ground-truth state but override clock/period with the
        # OCR-observed values so the displayed game_clock is frame-accurate.
        snapshot = dict(gt_timeline[str(gsec)])
        snapshot["ocr_clock"]   = clock  or "12:00"
        snapshot["ocr_quarter"] = qtr    or "1ST"
        result[str(vsec)] = snapshot

    print(f"  Mapped {len(result)} video_secs")
    print(f"  Pre-game / null OCR: {null_count} seconds")
    if drift_buckets:
        avg = sum(drift_buckets) / len(drift_buckets)
        mx  = max(drift_buckets)
        print(f"  Clock drift  avg={avg:.1f}s  max={mx}s")

    return result


if __name__ == "__main__":
    ocr_path = DEBUG_DIR / "clock_timeline.json"
    gt_path  = DATA_DIR  / "ground_truth_timeline.json"

    if not ocr_path.exists():
        raise FileNotFoundError(f"OCR timeline not found: {ocr_path}")
    if not gt_path.exists():
        raise FileNotFoundError(f"Ground-truth timeline not found: {gt_path}  (run state_machine.py first)")

    print("Loading OCR timeline…")
    ocr_timeline = json.loads(ocr_path.read_text())
    print(f"  {len(ocr_timeline)} OCR entries")

    print("Loading ground-truth timeline…")
    gt_timeline = json.loads(gt_path.read_text())
    print(f"  {len(gt_timeline)} game-second entries")

    print("Merging…")
    video_state_map = merge(ocr_timeline, gt_timeline)

    out_path = DATA_DIR / "video_state_map.json"
    out_path.write_text(json.dumps(video_state_map, indent=2))
    print(f"\n✓ Saved video_state_map ({len(video_state_map)} entries) → {out_path}")
