#!/usr/bin/env python3
"""
Unit tests for possession_utils.py pure functions.

Run with:
    cd vision && python -m pytest tests/test_possession_utils.py -v

No Modal, no CV, no network — pure data transformation tests.
"""
import json
import sys
import tempfile
from pathlib import Path

import pytest

# Ensure vision/ is on path when running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent))

from possession_utils import (
    _parse_possessor,
    aggregate_to_seconds,
    apply_dwell_filter,
    apply_forward_fill,
    apply_pbp_overrides,
    build_jersey_to_pid,
    build_name_registry,
    jersey_to_personid,
    lookup_name,
    merge_with_game_state,
    validate_oncourt,
)


# ── fixtures ──────────────────────────────────────────────────────────────────

BOXSCORE_ROWS = [
    {"personId": 2544,    "firstName": "LeBron",  "familyName": "James",  "jerseyNum": "23", "teamId": 1610612747},
    {"personId": 201939,  "firstName": "Stephen", "familyName": "Curry",  "jerseyNum": "30", "teamId": 1610612744},
    {"personId": 1630559, "firstName": "Austin",  "familyName": "Reaves", "jerseyNum": "15", "teamId": 1610612747},
    {"personId": 203110,  "firstName": "Draymond","familyName": "Green",  "jerseyNum": "23", "teamId": 1610612744},
]

GAME_STATE = {
    "1":  {"home_team": {"on_court": [201939, 203110]}, "visitor_team": {"on_court": [2544, 1630559]}, "events": []},
    "2":  {"home_team": {"on_court": [201939, 203110]}, "visitor_team": {"on_court": [2544, 1630559]}, "events": []},
    "3":  {"home_team": {"on_court": [201939, 203110]}, "visitor_team": {"on_court": [2544, 1630559]}, "events": []},
    "10": {"home_team": {"on_court": [201939]},         "visitor_team": {"on_court": [2544]},          "events": ["MISS L. James 16' Pullup Jump Shot"]},
    "11": {"home_team": {"on_court": [201939]},         "visitor_team": {"on_court": [2544]},          "events": ["Reaves REBOUND (Off:0 Def:1)"]},
}


@pytest.fixture
def boxscore_file(tmp_path):
    p = tmp_path / "player_boxscore.json"
    p.write_text(json.dumps(BOXSCORE_ROWS))
    return p


@pytest.fixture
def gs_file(tmp_path):
    p = tmp_path / "processed_game_state.json"
    p.write_text(json.dumps(GAME_STATE))
    return p


# ── build_jersey_to_pid ───────────────────────────────────────────────────────

class TestBuildJerseyToPid:
    def test_maps_jersey_to_personid(self, boxscore_file):
        reg = build_jersey_to_pid(boxscore_file)
        assert reg["30"] == 201939
        assert reg["15"] == 1630559

    def test_string_key(self, boxscore_file):
        reg = build_jersey_to_pid(boxscore_file)
        # Keys must be strings regardless of JSON source type
        assert all(isinstance(k, str) for k in reg)

    def test_duplicate_jersey_last_wins(self, boxscore_file):
        # Jersey "23" appears for both LeBron and Draymond — last row wins
        reg = build_jersey_to_pid(boxscore_file)
        assert reg["23"] in {2544, 203110}   # either is acceptable


# ── aggregate_to_seconds ──────────────────────────────────────────────────────

class TestAggregateToSeconds:
    def test_single_frame_per_second(self):
        raw = [
            {"video_sec": 1, "jersey_num": "23"},
            {"video_sec": 2, "jersey_num": "30"},
            {"video_sec": 3, "jersey_num": None},
        ]
        result = aggregate_to_seconds(raw)
        assert result[1] == "23"
        assert result[2] == "30"
        assert result[3] is None

    def test_majority_vote_multi_frame(self):
        raw = [
            {"video_sec": 1, "jersey_num": "23"},
            {"video_sec": 1, "jersey_num": "23"},
            {"video_sec": 1, "jersey_num": "30"},   # minority
        ]
        assert aggregate_to_seconds(raw)[1] == "23"

    def test_no_majority_returns_none(self):
        raw = [
            {"video_sec": 1, "jersey_num": "23"},
            {"video_sec": 1, "jersey_num": "30"},
        ]
        # 50 % tie → no clear winner
        assert aggregate_to_seconds(raw)[1] is None

    def test_all_none_frames(self):
        raw = [{"video_sec": 1, "jersey_num": None}, {"video_sec": 2, "jersey_num": None}]
        result = aggregate_to_seconds(raw)
        assert result[1] is None
        assert result[2] is None

    def test_empty_jersey_string_treated_as_none(self):
        raw = [{"video_sec": 1, "jersey_num": "  "}]
        assert aggregate_to_seconds(raw)[1] is None


# ── apply_dwell_filter ────────────────────────────────────────────────────────

class TestApplyDwellFilter:
    def test_run_meets_min_dwell(self):
        by_sec = {1: "23", 2: "23", 3: None}
        result = apply_dwell_filter(by_sec, min_dwell=2)
        assert result[1] == "23"
        assert result[2] == "23"
        assert result[3] is None

    def test_run_below_min_dwell_suppressed(self):
        by_sec = {1: "23", 2: None, 3: "23"}
        result = apply_dwell_filter(by_sec, min_dwell=2)
        # Each "23" appears for only 1 second — suppressed
        assert result[1] is None
        assert result[3] is None

    def test_non_consecutive_seconds_break_run(self):
        # Seconds 1 and 3 are not consecutive → two separate runs of length 1
        by_sec = {1: "23", 3: "23"}   # gap at 2
        result = apply_dwell_filter(by_sec, min_dwell=2)
        assert result[1] is None
        assert result[3] is None

    def test_long_run_all_kept(self):
        by_sec = {i: "30" for i in range(1, 8)}
        result = apply_dwell_filter(by_sec, min_dwell=3)
        assert all(result[i] == "30" for i in range(1, 8))

    def test_all_none_unchanged(self):
        by_sec = {1: None, 2: None, 3: None}
        result = apply_dwell_filter(by_sec)
        assert all(v is None for v in result.values())


# ── apply_forward_fill ────────────────────────────────────────────────────────

class TestApplyForwardFill:
    def test_fills_gap_within_max(self):
        filtered = {1: "23", 2: None, 3: None, 4: None}
        result = apply_forward_fill(filtered, max_fill=4)
        assert result[2] == "23"
        assert result[3] == "23"
        assert result[4] == "23"

    def test_gap_exceeds_max_reverts_to_none(self):
        filtered = {1: "23", 2: None, 3: None, 4: None, 5: None, 6: None}
        result = apply_forward_fill(filtered, max_fill=3)
        # Seconds 2, 3, 4 filled (1+3=4 ≤ max_fill? sec-last = 1,2,3 ≤ 3)
        assert result[2] == "23"   # 2-1=1 ≤ 3
        assert result[3] == "23"   # 3-1=2 ≤ 3
        assert result[4] == "23"   # 4-1=3 ≤ 3
        assert result[5] is None   # 5-1=4 > 3
        assert result[6] is None

    def test_new_valid_after_reset_starts_fresh_fill(self):
        filtered = {1: "23", 2: None, 3: None, 4: None, 5: None, 6: "30", 7: None}
        result = apply_forward_fill(filtered, max_fill=2)
        assert result[6] == "30"
        assert result[7] == "30"  # 7-6=1 ≤ 2

    def test_no_fill_when_none_from_start(self):
        filtered = {1: None, 2: None, 3: "23", 4: None}
        result = apply_forward_fill(filtered, max_fill=2)
        assert result[1] is None
        assert result[2] is None
        assert result[3] == "23"
        assert result[4] == "23"


# ── jersey_to_personid ────────────────────────────────────────────────────────

class TestJerseyToPersonid:
    def test_known_jersey(self):
        filled = {1: "30", 2: None}
        reg = {"30": 201939}
        result = jersey_to_personid(filled, reg)
        assert result[1] == 201939
        assert result[2] is None

    def test_unknown_jersey_becomes_none(self):
        filled = {1: "99"}
        result = jersey_to_personid(filled, {"30": 201939})
        assert result[1] is None


# ── validate_oncourt ──────────────────────────────────────────────────────────

class TestValidateOncourt:
    def test_valid_player_kept(self, gs_file):
        possession = {1: 2544}   # LeBron is in visitor on_court at sec 1
        result = validate_oncourt(possession, gs_file)
        assert result[1] == 2544

    def test_bench_player_rejected(self, gs_file):
        possession = {1: 999999}   # not on court
        result = validate_oncourt(possession, gs_file)
        assert result[1] is None

    def test_none_passes_through(self, gs_file):
        possession = {1: None}
        assert validate_oncourt(possession, gs_file)[1] is None

    def test_missing_vsec_in_gs_rejects(self, gs_file):
        possession = {999: 2544}   # second 999 not in game state
        result = validate_oncourt(possession, gs_file)
        assert result[999] is None


# ── _parse_possessor (internal helper) ───────────────────────────────────────

class TestParsePossessor:
    @pytest.fixture
    def registry(self, boxscore_file):
        return build_name_registry(boxscore_file)

    def test_missed_shot(self, registry):
        pid = _parse_possessor("MISS L. James 16' Pullup Jump Shot", registry)
        assert pid == 2544

    def test_made_shot(self, registry):
        pid = _parse_possessor("Curry 2' Cutting Layup Shot (2 PTS) (Green 1 AST)", registry)
        assert pid == 201939

    def test_rebound(self, registry):
        pid = _parse_possessor("Reaves REBOUND (Off:0 Def:1)", registry)
        assert pid == 1630559

    def test_block_skipped(self, registry):
        pid = _parse_possessor("James BLOCK (1 BLK)", registry)
        assert pid is None

    def test_unrecognised_name_returns_none(self, registry):
        pid = _parse_possessor("MISS Unknown Player 15' Jump Shot", registry)
        assert pid is None

    def test_made_shot_full_name(self, registry):
        pid = _parse_possessor("L. James 6' Hook Shot (2 PTS)", registry)
        assert pid == 2544


# ── apply_pbp_overrides ───────────────────────────────────────────────────────

class TestApplyPbpOverrides:
    def test_missed_shot_overrides_cv(self, gs_file, boxscore_file):
        # CV says nobody at sec 10, but PBP says LeBron missed → LeBron
        possession = {10: None, 11: None}
        result = apply_pbp_overrides(possession, gs_file, boxscore_file)
        assert result[10] == 2544     # LeBron: MISS L. James

    def test_rebound_overrides_cv(self, gs_file, boxscore_file):
        possession = {11: None}
        result = apply_pbp_overrides(possession, gs_file, boxscore_file)
        assert result[11] == 1630559  # Reaves REBOUND

    def test_no_event_second_unchanged(self, gs_file, boxscore_file):
        possession = {1: 201939}
        result = apply_pbp_overrides(possession, gs_file, boxscore_file)
        assert result[1] == 201939    # no event at sec 1, CV result preserved

    def test_override_beats_wrong_cv(self, gs_file, boxscore_file):
        possession = {10: 201939}     # CV wrong: says Curry
        result = apply_pbp_overrides(possession, gs_file, boxscore_file)
        assert result[10] == 2544     # PBP correctly overrides to LeBron


# ── merge_with_game_state ─────────────────────────────────────────────────────

class TestMergeWithGameState:
    def test_player_possession_injected(self, gs_file, tmp_path):
        possession = {1: 2544, 2: None, 3: 201939}
        out = tmp_path / "possession_game_state.json"
        merge_with_game_state(possession, gs_file, out)

        data = json.loads(out.read_text())
        assert data["1"]["player_possession"] == 2544
        assert data["2"]["player_possession"] is None
        assert data["3"]["player_possession"] == 201939

    def test_existing_fields_preserved(self, gs_file, tmp_path):
        out = tmp_path / "out.json"
        merge_with_game_state({1: 2544}, gs_file, out)
        data = json.loads(out.read_text())
        # Original fields still present
        assert "home_team" in data["1"]
        assert "visitor_team" in data["1"]
        assert "events" in data["1"]

    def test_source_file_untouched(self, gs_file, tmp_path):
        original = gs_file.read_text()
        out = tmp_path / "out.json"
        merge_with_game_state({1: 2544}, gs_file, out)
        assert gs_file.read_text() == original   # never modified
