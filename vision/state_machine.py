#!/usr/bin/env python3
"""
state_machine.py
================
Phase 2 — Core state engine.

Builds a second-by-second ground-truth game-state timeline for the
Lakers vs Warriors Christmas Day 2024 game by replaying NBA play-by-play
events in chronological order.

State tracked per second:
  • Period + game clock
  • On-court 5-player lineups per team
  • Cumulative player stats (PTS, REB, AST, STL, BLK)
  • Timestamped event ledger: each PBP event is stored exactly once,
    keyed to the game_sec it occurred — not carried forward across seconds.

Output: vision/data/nba/ground_truth_timeline.json
  Keys are integer game-elapsed-seconds (0 = tip-off).
  Each entry has an "events" field: a list of strings for events that
  happened at that exact second, or [] if nothing occurred.
"""
from __future__ import annotations

import copy
import json
import re
import unicodedata
from pathlib import Path
from typing import Any

DATA_DIR = Path(__file__).parent / "data" / "nba"


# ── clock helpers ─────────────────────────────────────────────────────────────

def parse_iso_clock(clock_str: str) -> int:
    """Convert 'PT11M41.00S' → integer seconds remaining in the period."""
    m = re.match(r"PT(\d+)M([\d.]+)S", str(clock_str))
    if m:
        return int(m.group(1)) * 60 + int(float(m.group(2)))
    return 0


def remaining_to_clock_str(remaining: int) -> str:
    """Seconds remaining → 'MM:SS'."""
    m, s = divmod(max(remaining, 0), 60)
    return f"{m:02d}:{s:02d}"


def game_elapsed(period: int, remaining: int) -> int:
    """Monotonically increasing game-second index.

    period 1, 12:00 remaining → 0   (tip-off)
    period 1,  0:00 remaining → 720 (end of Q1)
    period 2, 12:00 remaining → 720 (start of Q2)
    period 4,  0:00 remaining → 2880 (end of regulation)
    """
    return (period - 1) * 720 + (720 - remaining)


# ── name normalization ────────────────────────────────────────────────────────

def norm(name: str) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    nfkd = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    return " ".join(stripped.lower().split())


# ── state machine ─────────────────────────────────────────────────────────────

class GameStateMachine:
    """Replay NBA PBP events and produce a second-by-second state timeline."""

    MEANINGFUL_ACTIONS = {"Made Shot", "Missed Shot", "Rebound", "Turnover", ""}

    def __init__(
        self,
        pbp: list[dict],
        boxscore: list[dict],
        home_team_id: int,
        away_team_id: int,
    ) -> None:
        self.home_id = home_team_id
        self.away_id = away_team_id

        # on_court: team_id → list of personIds (exactly 5 each)
        self.on_court: dict[int, list[int]] = {home_team_id: [], away_team_id: []}

        # cumulative stats: personId → {pts, reb, ast, stl, blk}
        self.stats: dict[int, dict[str, int]] = {}

        # timestamped event ledger: game_sec → [event descriptions]
        # each event lives at exactly one second and is never carried forward
        self.events_by_second: dict[int, list[str]] = {}

        # derived rolling context window: game_sec → last-N events seen up to
        # that second.  Populated by build_recent_events() AFTER all PBP rows
        # have been processed.  Never written to during event ingestion.
        self.recent_events_by_second: dict[int, list[str]] = {}

        # tracks which second is currently being processed so handlers can
        # call _add_event without needing to pass the second explicitly
        self.current_game_sec: int = 0

        # current period + clock for snapshot labelling
        self.period = 1
        self.clock = "12:00"

        # name → personId registry for sub / assist lookup
        self._name_idx: dict[str, int] = {}

        self._build_registry(pbp, boxscore)
        self._init_starters(boxscore)
        self._init_stats(boxscore)

        # The final timeline: game_sec → state dict
        self.timeline: dict[int, dict] = {}
        self._build_timeline(pbp)

    # ── registry ──────────────────────────────────────────────────────────────

    def _register(self, name: str, pid: int) -> None:
        if not name or not pid:
            return
        self._name_idx[norm(name)] = pid
        # Also register just the last word (last name)
        parts = name.strip().split()
        if parts:
            self._name_idx[norm(parts[-1])] = pid
        # And the abbreviated "F. LastName" form if applicable
        if len(parts) >= 2:
            abbrev = f"{parts[0][0]}. {' '.join(parts[1:])}"
            self._name_idx[norm(abbrev)] = pid

    def _build_registry(self, pbp: list[dict], boxscore: list[dict]) -> None:
        # From PBP (has both playerName and playerNameI)
        for row in pbp:
            pid = row.get("personId")
            if not pid or pid == 0:
                continue
            self._register(row.get("playerName", ""), pid)
            self._register(row.get("playerNameI", ""), pid)
        # From box score (firstName + familyName)
        for row in boxscore:
            pid = row.get("personId")
            if not pid:
                continue
            full = f"{row.get('firstName', '')} {row.get('familyName', '')}".strip()
            self._register(full, pid)
            self._register(row.get("familyName", ""), pid)

    def _lookup(self, name: str) -> int | None:
        """Return personId for a display name, tolerating accent/case differences."""
        key = norm(name)
        pid = self._name_idx.get(key)
        if pid:
            return pid
        # Try last token only (handles "L. James" → "james")
        parts = key.split()
        if parts:
            return self._name_idx.get(parts[-1])
        return None

    # ── initialization ────────────────────────────────────────────────────────

    def _init_starters(self, boxscore: list[dict]) -> None:
        """Populate on_court from box-score position field ('G', 'F', 'C')."""
        for row in boxscore:
            pos = str(row.get("position", "")).strip()
            if pos not in ("G", "F", "C"):
                continue
            tid = row["teamId"]
            pid = row["personId"]
            if tid in self.on_court and pid not in self.on_court[tid]:
                self.on_court[tid].append(pid)

        for tid, lineup in self.on_court.items():
            print(f"  Starters team {tid}: {lineup} ({len(lineup)} players)")
            if len(lineup) != 5:
                print(f"  [WARN] Expected 5, got {len(lineup)}")

    def _init_stats(self, boxscore: list[dict]) -> None:
        for row in boxscore:
            pid = row.get("personId")
            if pid:
                self.stats[pid] = {"pts": 0, "reb": 0, "ast": 0, "stl": 0, "blk": 0}

    # ── recent-events builder ─────────────────────────────────────────────────

    def build_recent_events(self, window: int = 5) -> None:
        """Derive a rolling context window from the ground-truth event ledger.

        For every second from 0 → max event second, maintains a deque of the
        last *window* events seen up to and including that second.  The result
        is stored in self.recent_events_by_second and is NEVER written to
        during PBP ingestion — it is a pure post-processing derivation.

        Semantics:
          events_by_second[sec]        — atomic ground truth (what happened NOW)
          recent_events_by_second[sec] — rolling context    (last N seen so far)
        """
        all_event_secs = sorted(self.events_by_second.keys())
        if not all_event_secs:
            return

        max_sec = max(all_event_secs)
        rolling: list[str] = []

        for sec in range(max_sec + 1):
            # Use sec-1 so recent_events[t] reflects history up to t-1 only.
            # events[t] and recent_events[t] therefore never overlap on the
            # same timestep — events is ground truth NOW, recent_events is
            # everything that happened BEFORE now.
            if sec - 1 in self.events_by_second:
                rolling.extend(self.events_by_second[sec - 1])
            # Trim to the last N events (no carry-forward of the full history)
            rolling = rolling[-window:]
            self.recent_events_by_second[sec] = list(rolling)

    # ── timeline builder ──────────────────────────────────────────────────────

    def _build_timeline(self, pbp: list[dict]) -> None:
        """Group events by game_sec and fill the timeline second-by-second."""

        # Group events by game-elapsed-seconds
        bucket: dict[int, list[dict]] = {}
        for row in pbp:
            remaining = parse_iso_clock(row.get("clock", "PT00M00.00S"))
            period = int(row.get("period", 1))
            gsec = game_elapsed(period, remaining)
            bucket.setdefault(gsec, []).append(row)

        max_sec = max(bucket.keys()) if bucket else 2880

        prev_sec = 0
        current_state = self._snapshot()  # initial state: starters, zero stats, no events

        for gsec in sorted(bucket.keys()):
            # Gap seconds — no events happened here, emit empty events list
            for s in range(prev_sec, gsec):
                self.timeline[s] = {**current_state, "events": []}

            # Tell all event handlers which second they're writing into
            self.current_game_sec = gsec

            # Apply all PBP rows that fall on this second
            for row in bucket[gsec]:
                self.period = int(row.get("period", self.period))
                remaining = parse_iso_clock(row.get("clock", "PT12M00.00S"))
                self.clock = remaining_to_clock_str(remaining)
                self._apply(row)

            # Snapshot state AFTER events (stats / lineups updated)
            current_state = self._snapshot()
            # Attach only the events that belong to this exact second
            self.timeline[gsec] = {
                **current_state,
                "events": self.events_by_second.get(gsec, []),
            }
            prev_sec = gsec + 1

        # Fill to end of game — no further events
        for s in range(prev_sec, max_sec + 1):
            self.timeline[s] = {**current_state, "events": []}

        print(f"  Timeline spans game_sec 0 – {max_sec}  ({len(self.timeline)} entries)")

        # ── post-processing: derive rolling context window ────────────────────
        # build_recent_events runs AFTER all PBP rows are ingested so the
        # events_by_second ledger is complete before the window is computed.
        self.build_recent_events()

        # Inject recent_events into every timeline entry as a second, distinct
        # field.  It never touches the "events" field (ground truth).
        for sec, entry in self.timeline.items():
            entry["recent_events"] = self.recent_events_by_second.get(sec, [])

    def _apply(self, row: dict) -> None:
        action = row.get("actionType", "")

        if action == "Substitution":
            self._do_sub(row)
        elif action == "Made Shot":
            self._do_made_shot(row)
        elif action == "Free Throw":
            # shotResult is always empty in PlayByPlayV3; missed FTs have "MISS" in description
            if "MISS" not in row.get("description", "").upper():
                self._do_free_throw(row)
        elif action == "Rebound":
            self._do_rebound(row)
        elif action == "Turnover":
            self._do_turnover(row)
        elif action == "":
            self._do_unlabelled(row)
        elif action == "Missed Shot":
            self._add_event(self.current_game_sec, row.get("description", "")[:80])

    # ── event handlers ────────────────────────────────────────────────────────

    def _do_sub(self, row: dict) -> None:
        """Swap out → in for one player per substitution event.

        PBP format:
            personId = outgoing player's ID
            description = "SUB: {incoming_display_name} FOR {outgoing_display_name}"
        """
        out_id = row.get("personId")
        team_id = row.get("teamId")
        desc = row.get("description", "")

        m = re.match(r"SUB:\s+(.+?)\s+FOR\s+", desc, re.IGNORECASE)
        if not m:
            return
        in_name = m.group(1).strip()
        in_id = self._lookup(in_name)

        lineup = self.on_court.get(team_id, [])

        if out_id and out_id in lineup:
            lineup.remove(out_id)
        elif out_id:
            pass  # already off court (duplicate event guard)

        if in_id and in_id not in lineup:
            lineup.append(in_id)
        elif not in_id:
            print(f"  [WARN] sub: couldn't resolve incoming player '{in_name}' in '{desc}'")

    def _do_made_shot(self, row: dict) -> None:
        pid = row.get("personId")
        if not pid or pid == 0:
            return
        desc = row.get("description", "")

        # Point value
        pts = 3 if "3PT" in desc else 2
        self._add(pid, "pts", pts)

        # Assist — "(LastName N AST)" in description
        m = re.search(r"\((\w[\w .']+?)\s+\d+\s+AST\)", desc)
        if m:
            asst_id = self._lookup(m.group(1).strip())
            if asst_id:
                self._add(asst_id, "ast", 1)

        self._add_event(self.current_game_sec, desc[:80])

    def _do_free_throw(self, row: dict) -> None:
        pid = row.get("personId")
        if pid and pid != 0:
            self._add(pid, "pts", 1)

    def _do_rebound(self, row: dict) -> None:
        pid = row.get("personId")
        desc = row.get("description", "")
        # Skip team rebounds (personId == 0 or description says "Team")
        if not pid or pid == 0 or "Team" in desc:
            return
        self._add(pid, "reb", 1)
        self._add_event(self.current_game_sec, desc[:80])

    def _do_turnover(self, row: dict) -> None:
        self._add_event(self.current_game_sec, row.get("description", "")[:80])

    def _do_unlabelled(self, row: dict) -> None:
        """Empty actionType rows are steals and blocks."""
        pid = row.get("personId")
        desc = row.get("description", "")
        if not pid or pid == 0:
            return
        if "BLOCK" in desc:
            self._add(pid, "blk", 1)
            self._add_event(self.current_game_sec, desc[:80])
        elif "STEAL" in desc:
            self._add(pid, "stl", 1)
            self._add_event(self.current_game_sec, desc[:80])

    # ── state helpers ─────────────────────────────────────────────────────────

    def _add(self, pid: int, stat: str, value: int) -> None:
        if pid not in self.stats:
            self.stats[pid] = {"pts": 0, "reb": 0, "ast": 0, "stl": 0, "blk": 0}
        self.stats[pid][stat] += value

    def _add_event(self, sec: int, desc: str) -> None:
        """Record *desc* in the event ledger at *sec*.  No carry-forward."""
        if desc:
            self.events_by_second.setdefault(sec, []).append(desc)

    def _snapshot(self) -> dict:
        """Capture mutable state (lineups + stats).  Events are attached
        separately in _build_timeline so they never bleed across seconds."""
        return {
            "period": self.period,
            "clock": self.clock,
            "on_court": copy.deepcopy(self.on_court),
            "stats": copy.deepcopy(self.stats),
        }


# ── validation ────────────────────────────────────────────────────────────────

VALIDATION_TARGETS = {
    2544:    {"name": "LeBron James",   "pts": 31, "ast": 10},
    201939:  {"name": "Stephen Curry",  "pts": 38, "stl": 0},
    1630559: {"name": "Austin Reaves",  "pts": 26, "reb": 10},
}


def validate(sm: GameStateMachine) -> None:
    # Use the final state (last entry in timeline)
    final = sm.timeline[max(sm.timeline.keys())]
    print("\n── Validation ────────────────────────────────────────")
    all_ok = True
    for pid, targets in VALIDATION_TARGETS.items():
        got = final["stats"].get(pid, {})
        line = f"  {targets['name']:20s}"
        for stat, expected in targets.items():
            if stat == "name":
                continue
            actual = got.get(stat, 0)
            ok = actual == expected
            if not ok:
                all_ok = False
            marker = "✓" if ok else "✗"
            line += f"  {stat.upper()}={actual}/{expected}{marker}"
        print(line)

    # Lineup sanity
    for tid, lineup in final["on_court"].items():
        uniq = list(set(lineup))
        ok_count = len(uniq) == 5
        marker = "✓" if ok_count else "✗"
        print(f"  Team {tid} on-court: {len(uniq)} players {marker}")

    if all_ok:
        print("  ✓ All validation targets met")
    else:
        print("  [WARN] Some targets missed — check stat logic")


# ── main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    meta     = json.loads((DATA_DIR / "game_meta.json").read_text())
    pbp      = json.loads((DATA_DIR / "pbp_raw.json").read_text())
    boxscore = json.loads((DATA_DIR / "player_boxscore.json").read_text())

    print("Building GameStateMachine…")
    sm = GameStateMachine(
        pbp=pbp,
        boxscore=boxscore,
        home_team_id=meta["home_team_id"],
        away_team_id=meta["away_team_id"],
    )

    validate(sm)

    # Serialize — keys must be strings for JSON
    out_path = DATA_DIR / "ground_truth_timeline.json"
    out_path.write_text(json.dumps({str(k): v for k, v in sm.timeline.items()}, indent=2))
    print(f"\n✓ Saved timeline ({len(sm.timeline)} entries) → {out_path}")
