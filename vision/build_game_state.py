#!/usr/bin/env python3
"""
build_game_state.py
===================
Phase 5 — Orchestrate the full pipeline and emit processed_game_state.json.

Execution order:
  1. fetch_pbp.py        (if data not already cached)
  2. state_machine.py    (build ground-truth timeline)
  3. merge_ocr.py        (align video seconds to game state)
  4. Format + save       vision/processed_game_state.json

Required structure per video_sec:
  {
    "game_clock": "MM:SS",
    "period": int,
    "home_team": {
      "on_court": [player_id, ...],
      "player_stats": {
        "player_id": {"pts": 0, "reb": 0, "ast": 0, "stl": 0, "blk": 0}
      }
    },
    "visitor_team": { ... },
    "recent_events": ["...", "...", "..."]
  }
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

VISION_DIR = Path(__file__).parent
DATA_DIR   = VISION_DIR / "data" / "nba"
DEBUG_DIR  = VISION_DIR / "debug_outputs"
OUT_PATH   = VISION_DIR / "processed_game_state.json"

QUARTER_MAP = {"1ST": 1, "2ND": 2, "3RD": 3, "4TH": 4, "OT": 5}
PYTHON = sys.executable


# ── pipeline orchestration ────────────────────────────────────────────────────

def run_step(script: str) -> None:
    path = VISION_DIR / script
    print(f"\n{'─'*56}")
    print(f"  Running {script}…")
    print(f"{'─'*56}")
    result = subprocess.run([PYTHON, str(path)], cwd=str(VISION_DIR))
    if result.returncode != 0:
        raise RuntimeError(f"{script} exited {result.returncode}")


def ensure_data() -> None:
    if not (DATA_DIR / "game_meta.json").exists() or not (DATA_DIR / "pbp_raw.json").exists():
        run_step("fetch_pbp.py")
    else:
        print("✓ PBP data cached — skipping fetch_pbp.py")

    if not (DATA_DIR / "ground_truth_timeline.json").exists():
        run_step("state_machine.py")
    else:
        print("✓ Ground-truth timeline cached — skipping state_machine.py")

    run_step("merge_ocr.py")


# ── formatting ────────────────────────────────────────────────────────────────

def format_output(
    video_state_map: dict[str, dict],
    home_team_id: int,
    away_team_id: int,
    home_pids: set[int],
    visitor_pids: set[int],
) -> dict[str, dict]:
    """Convert internal state snapshots to the required output schema."""
    out: dict[str, dict] = {}

    for vsec_str, state in video_state_map.items():
        on_court  = state.get("on_court", {})
        all_stats = state.get("stats", {})

        # on_court keys are strings in the JSON (JSON serialises int keys to str)
        home_court    = on_court.get(str(home_team_id), [])
        visitor_court = on_court.get(str(away_team_id), [])

        # Use OCR-observed clock/period for the displayed game_clock
        ocr_clock   = state.get("ocr_clock")   or state.get("clock", "12:00")
        ocr_quarter = state.get("ocr_quarter")  or "1ST"
        period = QUARTER_MAP.get(str(ocr_quarter).upper(), state.get("period", 1))

        home_stats:    dict[str, dict] = {}
        visitor_stats: dict[str, dict] = {}

        home_court_set    = set(int(x) for x in home_court)
        visitor_court_set = set(int(x) for x in visitor_court)

        for pid_str, pstats in all_stats.items():
            pid = int(pid_str)
            record = {
                "pts": pstats.get("pts", 0),
                "reb": pstats.get("reb", 0),
                "ast": pstats.get("ast", 0),
                "stl": pstats.get("stl", 0),
                "blk": pstats.get("blk", 0),
            }
            # Assign to team: prefer on-court membership, fall back to roster
            if pid in home_court_set or pid in home_pids:
                home_stats[str(pid)] = record
            elif pid in visitor_court_set or pid in visitor_pids:
                visitor_stats[str(pid)] = record

        out[vsec_str] = {
            "game_clock":  ocr_clock,
            "period":      period,
            "home_team": {
                "on_court":     [int(x) for x in home_court],
                "player_stats": home_stats,
            },
            "visitor_team": {
                "on_court":     [int(x) for x in visitor_court],
                "player_stats": visitor_stats,
            },
            # Ground-truth: only events that happened at this exact second.
            "events": state.get("events", []),
            # Derived: rolling context of last 5 events seen up to this second.
            # Populated by GameStateMachine.build_recent_events() post-ingestion.
            "recent_events": state.get("recent_events", []),
        }

    return out


# ── validation ────────────────────────────────────────────────────────────────

def validate_output(
    out: dict,
    gt_timeline: dict,          # full ground-truth timeline for end-of-game stats
    home_team_id: int,
    away_team_id: int,
) -> None:
    print("\n── Validation ────────────────────────────────────────")

    # 1. Lineup integrity across every video_sec
    lineup_issues = 0
    for vsec_str, entry in out.items():
        for side in ("home_team", "visitor_team"):
            court = entry[side]["on_court"]
            if len(court) != 5:
                print(f"  [WARN] vsec {vsec_str} {side}: {len(court)} players (expected 5)")
                lineup_issues += 1
            if len(set(court)) != len(court):
                print(f"  [WARN] vsec {vsec_str} {side}: duplicate players")
                lineup_issues += 1

    if lineup_issues == 0:
        print("  ✓ Lineup check PASS — 5 unique players per team every second")
    else:
        print(f"  ✗ {lineup_issues} lineup issues")

    # 2. Clock progression — must be non-decreasing within a period
    prev_period, prev_rem = 0, 720
    clock_issues = 0
    for vsec_str in sorted(out.keys(), key=int):
        entry = out[vsec_str]
        p = entry["period"]
        try:
            m, s = entry["game_clock"].split(":")
            rem = int(m) * 60 + int(s)
        except Exception:
            rem = 720
        if p == prev_period and rem > prev_rem + 2:   # allow tiny OCR jitter
            clock_issues += 1
        prev_period, prev_rem = p, rem

    if clock_issues == 0:
        print("  ✓ Clock progression PASS — no backward jumps")
    else:
        print(f"  ✗ {clock_issues} clock regression(s) detected")

    # 3. Validate end-of-game stats against official box score
    # (done against ground_truth_timeline, not the short video excerpt)
    final_sec = str(max(int(k) for k in gt_timeline.keys()))
    final_stats = gt_timeline[final_sec]["stats"]

    TARGETS = [
        (2544,    "LeBron James",  "pts", 31),
        (2544,    "LeBron James",  "ast", 10),
        (201939,  "Stephen Curry", "pts", 38),
        (201939,  "Stephen Curry", "stl",  0),
        (1630559, "Austin Reaves", "pts", 26),
        (1630559, "Austin Reaves", "reb", 10),
    ]
    stat_issues = 0
    for pid, name, stat, expected in TARGETS:
        actual = final_stats.get(str(pid), {}).get(stat, 0)
        ok = actual == expected
        marker = "✓" if ok else "✗"
        if not ok:
            stat_issues += 1
        print(f"  {name:20s} {stat.upper():3s} {actual:3d}/{expected:3d} {marker}")

    if stat_issues == 0:
        print("  ✓ All end-of-game stats match official box score")

    # 4. No missing video seconds
    secs = sorted(int(k) for k in out.keys())
    expected_range = list(range(min(secs), max(secs) + 1))
    missing = set(expected_range) - set(secs)
    if missing:
        print(f"  ✗ Missing video_secs: {sorted(missing)}")
    else:
        print(f"  ✓ Continuous timeline — {len(secs)} seconds, no gaps")


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ensure_data()

    # Load metadata
    meta         = json.loads((DATA_DIR / "game_meta.json").read_text())
    home_team_id = meta["home_team_id"]   # GSW
    away_team_id = meta["away_team_id"]   # LAL

    # Build team-membership sets from box score
    boxscore     = json.loads((DATA_DIR / "player_boxscore.json").read_text())
    home_pids    = {r["personId"] for r in boxscore if r["teamId"] == home_team_id}
    visitor_pids = {r["personId"] for r in boxscore if r["teamId"] == away_team_id}

    # Load merged video-state map
    video_state_map = json.loads((DATA_DIR / "video_state_map.json").read_text())
    print(f"\nLoaded video_state_map: {len(video_state_map)} video_secs")

    # Load ground-truth for end-of-game validation
    gt_timeline = json.loads((DATA_DIR / "ground_truth_timeline.json").read_text())

    # Format
    print("Formatting output…")
    out = format_output(video_state_map, home_team_id, away_team_id, home_pids, visitor_pids)

    # Validate
    validate_output(out, gt_timeline, home_team_id, away_team_id)

    # Save
    OUT_PATH.write_text(json.dumps(out, indent=2))
    print(f"\n✓ processed_game_state.json saved ({len(out)} entries) → {OUT_PATH}")

    # Print a sample entry
    mid_vsec = str(sorted(int(k) for k in out.keys())[len(out)//2])
    e = out[mid_vsec]
    print(f"\n── Sample (video_sec={mid_vsec}) ───────────────────────")
    print(f"  period={e['period']}  game_clock={e['game_clock']}")
    print(f"  home  on_court  = {e['home_team']['on_court']}")
    print(f"  visit on_court  = {e['visitor_team']['on_court']}")
    print(f"  events          = {e['events']}")
    print(f"  recent_events   = {e['recent_events']}")
    lbj_s = e["visitor_team"]["player_stats"].get("2544", {})
    print(f"  LeBron so far   = {lbj_s}")


if __name__ == "__main__":
    main()
