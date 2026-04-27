#!/usr/bin/env python3
"""
possession_utils.py
===================
Pure post-processing functions for the possession detection pipeline.

No Modal, no CV, no network calls — purely deterministic data
transformations that can be imported and unit-tested in isolation.

Used by player_clips.py (Modal orchestrator) and vision/tests/.
"""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path


# ── name normalisation (mirrors state_machine.py) ─────────────────────────────

def _norm(name: str) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    nfkd = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    return " ".join(stripped.lower().split())


# ── registry builders ─────────────────────────────────────────────────────────

def build_jersey_to_pid(boxscore_path: str | Path) -> dict[str, int]:
    """Read player_boxscore.json → {jersey_num_string: personId}."""
    data = json.loads(Path(boxscore_path).read_text())
    registry: dict[str, int] = {}
    for row in data:
        pid = row.get("personId")
        jersey = row.get("jerseyNum")
        if pid and jersey is not None:
            registry[str(jersey).strip()] = int(pid)
    return registry


def build_name_registry(boxscore_path: str | Path) -> dict[str, int]:
    """Read player_boxscore.json → {normalised_name_variant: personId}.

    Registers full name, family name, and "F. FamilyName" abbreviation
    so that PBP event strings like "L. James" resolve correctly.
    """
    data = json.loads(Path(boxscore_path).read_text())
    idx: dict[str, int] = {}
    for row in data:
        pid = row.get("personId")
        if not pid:
            continue
        pid = int(pid)
        first = str(row.get("firstName", "")).strip()
        family = str(row.get("familyName", "")).strip()
        full = f"{first} {family}".strip()
        candidates = [full, family]
        if first:
            candidates.append(f"{first[0]}. {family}")
        for name in candidates:
            if name:
                idx[_norm(name)] = pid
    return idx


def lookup_name(name: str, registry: dict[str, int]) -> int | None:
    """Resolve a display name (with possible abbreviation) to personId."""
    key = _norm(name)
    if key in registry:
        return registry[key]
    # Fallback: match on last token only ("James" → 2544)
    parts = key.split()
    return registry.get(parts[-1]) if parts else None


# ── per-second aggregation ────────────────────────────────────────────────────

def aggregate_to_seconds(raw: list[dict]) -> dict[int, str | None]:
    """Majority-vote frame-level jersey detections into one value per video second.

    At 1 FPS each video_sec has exactly one frame, so the vote is trivial.
    At higher frame rates, the winner must appear in > 50 % of detected frames.
    """
    by_sec: dict[int, list[str]] = {}
    all_vsecs: set[int] = set()

    for frame in raw:
        vsec = int(frame["video_sec"])
        all_vsecs.add(vsec)
        jersey = frame.get("jersey_num")
        if jersey is not None:
            j = str(jersey).strip()
            if j:
                by_sec.setdefault(vsec, []).append(j)

    result: dict[int, str | None] = {}
    for vsec in sorted(all_vsecs):
        jerseys = by_sec.get(vsec, [])
        if not jerseys:
            result[vsec] = None
            continue

        counts: dict[str, int] = {}
        for j in jerseys:
            counts[j] = counts.get(j, 0) + 1

        winner = max(counts, key=lambda k: counts[k])
        result[vsec] = winner if counts[winner] / len(jerseys) > 0.5 else None

    return result


# ── temporal smoothing ────────────────────────────────────────────────────────

def apply_dwell_filter(
    by_sec: dict[int, str | None],
    min_dwell: int = 2,
) -> dict[int, str | None]:
    """Keep jersey assignments only when the same value holds for
    *min_dwell* **consecutive** video seconds.

    Single-second flickers from passes, occlusions, or brief mis-detections
    are suppressed.  Non-consecutive seconds (gaps in video_sec keys) break
    a run.
    """
    secs = sorted(by_sec.keys())
    result: dict[int, str | None] = {s: None for s in secs}

    i = 0
    while i < len(secs):
        jersey = by_sec[secs[i]]
        if jersey is None:
            i += 1
            continue

        # Extend run: same jersey AND no gap in video_sec sequence
        j = i
        while (
            j + 1 < len(secs)
            and by_sec[secs[j + 1]] == jersey
            and secs[j + 1] == secs[j] + 1
        ):
            j += 1

        run_length = j - i + 1
        if run_length >= min_dwell:
            for k in range(i, j + 1):
                result[secs[k]] = jersey

        i = j + 1

    return result


def apply_forward_fill(
    filtered: dict[int, str | None],
    max_fill: int = 4,
) -> dict[int, str | None]:
    """Hold the last valid jersey assignment for up to *max_fill* video seconds.

    Bridges: ball in flight during a pass, catching delay, brief occlusion.
    Reverts to None once the gap exceeds *max_fill* seconds (loose ball,
    rebound scramble, dead ball).
    """
    secs = sorted(filtered.keys())
    result: dict[int, str | None] = {}

    last_jersey: str | None = None
    last_valid_sec: int = -9999

    for sec in secs:
        jersey = filtered[sec]
        if jersey is not None:
            last_jersey = jersey
            last_valid_sec = sec
            result[sec] = jersey
        elif last_jersey is not None and (sec - last_valid_sec) <= max_fill:
            result[sec] = last_jersey
        else:
            last_jersey = None   # gap too large — reset
            result[sec] = None

    return result


# ── identity mapping ──────────────────────────────────────────────────────────

def jersey_to_personid(
    filled: dict[int, str | None],
    registry: dict[str, int],
) -> dict[int, int | None]:
    """Map jersey number strings to NBA personIds.

    Unrecognised strings (OCR errors like "B", "1B") become None.
    """
    return {
        vsec: registry.get(jersey) if jersey is not None else None
        for vsec, jersey in filled.items()
    }


# ── on-court validation ───────────────────────────────────────────────────────

def validate_oncourt(
    possession: dict[int, int | None],
    gs_path: str | Path,
) -> dict[int, int | None]:
    """Reject any personId that is not on the court at that video second.

    Safety net: the CV model cannot legally assign possession to a bench
    player.  Any such assignment is likely a jersey OCR mis-read.
    """
    gs = json.loads(Path(gs_path).read_text())
    result: dict[int, int | None] = {}

    for vsec, pid in possession.items():
        if pid is None:
            result[vsec] = None
            continue

        entry = gs.get(str(vsec), {})
        home_court = [int(x) for x in entry.get("home_team", {}).get("on_court", [])]
        visitor_court = [int(x) for x in entry.get("visitor_team", {}).get("on_court", [])]
        all_oncourt = set(home_court + visitor_court)

        result[vsec] = pid if pid in all_oncourt else None

    return result


# ── PBP overrides ─────────────────────────────────────────────────────────────

def apply_pbp_overrides(
    possession: dict[int, int | None],
    gs_path: str | Path,
    boxscore_path: str | Path,
) -> dict[int, int | None]:
    """Override CV possession at PBP event seconds.

    Reads *events* from processed_game_state.json (already video_sec-aligned
    by merge_ocr.py — never reads pbp_raw.json directly).

    Priority: PBP ground truth beats CV for any second that has a parseable
    event.  If the CV result agrees, the override is a no-op.
    """
    name_registry = build_name_registry(boxscore_path)
    gs = json.loads(Path(gs_path).read_text())
    result = dict(possession)

    for vsec_str, entry in gs.items():
        vsec = int(vsec_str)
        events = entry.get("events", [])
        if not events:
            continue

        for event in events:
            pid = _parse_possessor(event, name_registry)
            if pid is not None:
                result[vsec] = pid
                break   # one authoritative override per second

    return result


def _parse_possessor(event: str, registry: dict[str, int]) -> int | None:
    """Extract the personId of the ball-carrier from a PBP event string.

    Returns None if the event does not imply a clear possessor (e.g. a block,
    which fires on the *defender*).

    Supported patterns (mirroring state_machine.py output):
      Made shot  : "Name N' Shot Type (N PTS) ..."
      Missed shot: "MISS Name N' Shot Type ..."
      Rebound    : "Name REBOUND ..."
      Steal      : "Name STEAL ..."
      Block      : skipped (fires on defender, not possessor)
    """
    # Blocks — skip entirely; possessor is identified by the concurrent
    # missed-shot event in the same second (also stored in events[]).
    if re.search(r"\bBLOCK\b", event, re.I):
        return None

    # Steal → stealer gains possession
    m = re.match(r"^(.+?)\s+STEAL\b", event, re.I)
    if m:
        return lookup_name(m.group(1).strip(), registry)

    # Rebound → rebounder gains possession
    m = re.match(r"^(.+?)\s+REBOUND\b", event, re.I)
    if m:
        return lookup_name(m.group(1).strip(), registry)

    # Missed shot → shooter had possession
    m = re.match(r"^MISS\s+(.+?)\s+\d+", event, re.I)
    if m:
        return lookup_name(m.group(1).strip(), registry)

    # Made shot → scorer had possession ("Name N' Shot …")
    # The foot-mark (') or curly apostrophe separates name from distance.
    m = re.match(r"^(.+?)\s+\d+[''ʼ]\s", event)
    if m:
        return lookup_name(m.group(1).strip(), registry)

    return None


# ── segment index (precomputation helper, mirrors frontend logic) ─────────────

def build_segment_index(
    possession: dict[int, str | None],
) -> dict[str, list[dict]]:
    """Build a possession segment index from a video-second possession map.

    Args:
        possession: {video_sec: personId_string | None}

    Returns:
        {personId_string: [{"start": int, "end": int}, ...]}
        Each list is in ascending order and contains contiguous possession runs.

    Used for backend precomputation (future: possession_segments.json).
    The frontend TypeScript hook mirrors this logic exactly.
    """
    sorted_secs = sorted(possession.keys())
    index: dict[str, list[dict]] = {}

    run_pid: str | None = None
    run_start: int = 0
    prev_sec: int = 0

    def close_run(end: int) -> None:
        nonlocal run_pid
        if run_pid is not None:
            index.setdefault(run_pid, []).append({"start": run_start, "end": end})
            run_pid = None

    for i, sec in enumerate(sorted_secs):
        pid = possession[sec]
        end_of_prev = prev_sec if i > 0 else sec
        # A gap in video_sec (non-consecutive seconds) always breaks a run,
        # even when the same player appears on both sides of the gap.
        has_gap = (i > 0) and (sec - prev_sec > 1)
        if pid is None:
            close_run(end_of_prev)
        elif pid != run_pid or has_gap:
            close_run(end_of_prev)
            run_pid = pid
            run_start = sec
        # same pid, no gap → run continues
        prev_sec = sec

    if run_pid is not None and sorted_secs:
        close_run(sorted_secs[-1])

    return index


def merge_segments(
    segment_lists: list[list[dict]],
) -> list[dict]:
    """Merge and sort multiple possession segment lists, combining overlaps.

    Two segments overlap when A.end >= B.start (i.e. they share at least one
    second).  In single-possession data this never occurs (two players cannot
    both hold the ball simultaneously), but the function is correct for any
    input and is used to compute multi-player union segment lists.

    Args:
        segment_lists: One list of {"start", "end"} dicts per selected player.

    Returns:
        A single sorted, non-overlapping list of merged segments.
    """
    all_segs: list[dict] = []
    for segs in segment_lists:
        all_segs.extend(segs)

    if not all_segs:
        return []

    all_segs.sort(key=lambda s: s["start"])

    merged = [dict(all_segs[0])]
    for seg in all_segs[1:]:
        last = merged[-1]
        if seg["start"] <= last["end"]:               # overlap or adjacent
            last["end"] = max(last["end"], seg["end"])
        else:
            merged.append(dict(seg))

    return merged


# ── final merge ───────────────────────────────────────────────────────────────

def merge_with_game_state(
    possession: dict[int, int | None],
    gs_path: str | Path,
    out_path: str | Path,
) -> None:
    """Inject *player_possession* into every entry of processed_game_state.json.

    processed_game_state.json is NEVER modified.  The output is a strict
    superset written to *out_path*.
    """
    gs = json.loads(Path(gs_path).read_text())
    output: dict[str, dict] = {}

    for vsec_str, state in gs.items():
        pid = possession.get(int(vsec_str))
        output[vsec_str] = {**state, "player_possession": pid}

    Path(out_path).write_text(json.dumps(output, indent=2))
    print(f"✓ Wrote {out_path}  ({len(output)} entries)")


# ── diagnostic helper ─────────────────────────────────────────────────────────

def possession_coverage_report(possession: dict[int, int | None]) -> None:
    total = len(possession)
    covered = sum(1 for v in possession.values() if v is not None)
    pct = covered / total * 100 if total else 0
    print(f"  Coverage: {covered}/{total} seconds ({pct:.1f}%)")
    if pct < 30:
        print("  [WARN] Coverage < 30 % — possession detection may be unreliable")
    elif pct < 50:
        print("  [NOTE] Coverage < 50 % — consider running at higher FPS")
