#!/usr/bin/env python3
"""
Unit tests for build_segment_index and merge_segments in possession_utils.py.

These are the Tests A–E specified in the implementation plan (§6.2
"Backend: Segment Generation Correctness").

No Modal, no CV, no network — pure deterministic dict transformations.

Run with:
    cd vision && python -m pytest tests/test_segment_builder.py -v
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from possession_utils import build_segment_index, merge_segments


# ── Test A — Basic run-length encoding ────────────────────────────────────────

class TestBuildSegmentIndexBasic:
    def test_basic_run_length(self):
        """Plan Test A: normal sequence with two distinct players."""
        inp = {1: None, 2: "2544", 3: "2544", 4: "2544", 5: None, 6: "201939"}
        result = build_segment_index(inp)
        assert result == {
            "2544":   [{"start": 2, "end": 4}],
            "201939": [{"start": 6, "end": 6}],
        }

    def test_no_possession_returns_empty(self):
        """Plan Test B: all-None input → empty index."""
        inp = {1: None, 2: None, 3: None}
        assert build_segment_index(inp) == {}

    def test_single_second_segments(self):
        """Plan Test C: isolated single-second possessions produce two segments."""
        inp = {1: "2544", 2: None, 3: "2544"}
        result = build_segment_index(inp)
        assert result == {
            "2544": [{"start": 1, "end": 1}, {"start": 3, "end": 3}]
        }

    def test_empty_input(self):
        assert build_segment_index({}) == {}

    def test_single_second_single_player(self):
        inp = {5: "2544"}
        result = build_segment_index(inp)
        assert result == {"2544": [{"start": 5, "end": 5}]}

    def test_player_switch_mid_run(self):
        """Run A then immediately run B with no gap."""
        inp = {1: "2544", 2: "2544", 3: "201939", 4: "201939"}
        result = build_segment_index(inp)
        assert result == {
            "2544":   [{"start": 1, "end": 2}],
            "201939": [{"start": 3, "end": 4}],
        }

    def test_trailing_run_closed(self):
        """Segment that runs to the last second is closed properly."""
        inp = {1: None, 2: "2544", 3: "2544"}
        result = build_segment_index(inp)
        assert result == {"2544": [{"start": 2, "end": 3}]}

    def test_segment_start_equals_end_when_gap(self):
        """Non-consecutive seconds break a run."""
        inp = {1: "2544", 3: "2544"}   # gap at 2
        result = build_segment_index(inp)
        # Run at 1 ends at 1; run at 3 ends at 3 (separate segments)
        assert result == {"2544": [{"start": 1, "end": 1}, {"start": 3, "end": 3}]}

    def test_segments_per_player_sorted_ascending(self):
        inp = {1: "A", 2: None, 3: "B", 4: None, 5: "A"}
        result = build_segment_index(inp)
        assert result["A"] == [{"start": 1, "end": 1}, {"start": 5, "end": 5}]
        assert result["B"] == [{"start": 3, "end": 3}]

    def test_all_same_player(self):
        inp = {i: "2544" for i in range(1, 6)}
        result = build_segment_index(inp)
        assert result == {"2544": [{"start": 1, "end": 5}]}


# ── Test D & E — merge_segments (multi-player union) ─────────────────────────

class TestMergeSegments:
    def test_overlap_merged(self):
        """Plan Test D: overlapping segments from two players merge into one."""
        segs_a = [{"start": 10, "end": 15}]
        segs_b = [{"start": 13, "end": 20}]
        merged = merge_segments([segs_a, segs_b])
        assert merged == [{"start": 10, "end": 20}]

    def test_no_overlap_preserved(self):
        """Plan Test E: non-overlapping segments from two players stay separate."""
        segs_a = [{"start": 10, "end": 15}]
        segs_b = [{"start": 20, "end": 25}]
        merged = merge_segments([segs_a, segs_b])
        assert merged == [{"start": 10, "end": 15}, {"start": 20, "end": 25}]

    def test_empty_lists(self):
        assert merge_segments([]) == []
        assert merge_segments([[], []]) == []

    def test_single_list_passthrough(self):
        segs = [{"start": 1, "end": 5}, {"start": 10, "end": 15}]
        assert merge_segments([segs]) == segs

    def test_adjacent_segments_merged(self):
        """Segments that share a boundary second (end == start) should merge."""
        segs_a = [{"start": 1, "end": 5}]
        segs_b = [{"start": 5, "end": 10}]
        merged = merge_segments([segs_a, segs_b])
        assert merged == [{"start": 1, "end": 10}]

    def test_output_sorted_ascending(self):
        segs_a = [{"start": 20, "end": 25}]
        segs_b = [{"start": 1, "end": 5}]
        merged = merge_segments([segs_a, segs_b])
        assert merged[0]["start"] < merged[1]["start"]

    def test_three_players_no_overlap(self):
        merged = merge_segments([
            [{"start": 1,  "end": 3}],
            [{"start": 10, "end": 12}],
            [{"start": 20, "end": 22}],
        ])
        assert merged == [
            {"start": 1,  "end": 3},
            {"start": 10, "end": 12},
            {"start": 20, "end": 22},
        ]

    def test_three_players_chain_overlap(self):
        """A overlaps B, B overlaps C → all three merge into one."""
        merged = merge_segments([
            [{"start": 1,  "end": 5}],
            [{"start": 4,  "end": 8}],
            [{"start": 7,  "end": 10}],
        ])
        assert merged == [{"start": 1, "end": 10}]

    def test_original_lists_not_mutated(self):
        segs_a = [{"start": 1, "end": 5}]
        segs_b = [{"start": 3, "end": 8}]
        _ = merge_segments([segs_a, segs_b])
        assert segs_a == [{"start": 1, "end": 5}]
        assert segs_b == [{"start": 3, "end": 8}]
