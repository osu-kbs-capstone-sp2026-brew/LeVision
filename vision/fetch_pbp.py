#!/usr/bin/env python3
"""
fetch_pbp.py
============
Phase 1 — API Selection & Data Fetching.

Finds the Lakers vs Warriors Christmas Day 2024 game via nba_api, then
fetches PlayByPlayV3 and BoxScoreTraditionalV3.  Raw data is saved as JSON
under vision/data/nba/ so downstream scripts can run offline.

Usage:
    python fetch_pbp.py
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pandas as pd

# ── directories ──────────────────────────────────────────────────────────────
DATA_DIR = Path(__file__).parent / "data" / "nba"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── known team IDs ────────────────────────────────────────────────────────────
LAL_ID = 1610612747   # Los Angeles Lakers
GSW_ID = 1610612744   # Golden State Warriors

GAME_DATE = "12/25/2024"


# ── helpers ──────────────────────────────────────────────────────────────────

def _save_df(df: pd.DataFrame, name: str) -> Path:
    p = DATA_DIR / name
    df.to_json(p, orient="records", indent=2)
    print(f"  ✓ saved {len(df)} rows → {p}")
    return p


def _save_json(obj: dict, name: str) -> Path:
    p = DATA_DIR / name
    p.write_text(json.dumps(obj, indent=2, default=str))
    print(f"  ✓ saved → {p}")
    return p


# ── step 1 : find game_id ─────────────────────────────────────────────────────

def find_game_id() -> str:
    from nba_api.stats.endpoints import leaguegamefinder

    print(f"\n[1] Searching for LAL vs GSW on {GAME_DATE}…")
    gf = leaguegamefinder.LeagueGameFinder(
        date_from_nullable=GAME_DATE,
        date_to_nullable=GAME_DATE,
        league_id_nullable="00",
        timeout=60,
    )
    time.sleep(0.8)

    df = gf.get_data_frames()[0]
    print(df[["GAME_ID", "TEAM_ABBREVIATION", "MATCHUP", "WL", "PTS"]].to_string())

    lal_row = df[df["TEAM_ID"] == LAL_ID]
    if lal_row.empty:
        raise RuntimeError("No Lakers game found on 12/25/2024 — check date or network")

    # Keep as string — the NBA API needs the full zero-padded 10-digit ID
    game_id = str(lal_row.iloc[0]["GAME_ID"]).strip()
    print(f"\n  ✓ game_id = {game_id}")
    return game_id


# ── step 2 : play-by-play ─────────────────────────────────────────────────────

def fetch_pbp(game_id: str) -> pd.DataFrame:
    from nba_api.stats.endpoints import playbyplayv3

    print(f"\n[2] Fetching PlayByPlayV3 for {game_id}…")
    time.sleep(0.8)

    raw = playbyplayv3.PlayByPlayV3(game_id=game_id, timeout=90)
    df = raw.get_data_frames()[0]

    print(f"  {len(df)} events")
    print(f"  columns: {list(df.columns)}")
    print("\n  First 15 events:")
    display_cols = [c for c in ["period", "clock", "actionType", "subType",
                                 "personId", "playerName", "description",
                                 "shotResult", "scoreHome", "scoreAway"]
                    if c in df.columns]
    print(df[display_cols].head(15).to_string())

    print("\n  actionType breakdown:")
    print(df["actionType"].value_counts().to_string())

    if "subType" in df.columns:
        subs = df[df["actionType"] == "substitution"]
        print(f"\n  Substitution subTypes: {subs['subType'].unique()}")

    _save_df(df, "pbp_raw.json")
    return df


# ── step 3 : box score ────────────────────────────────────────────────────────

def fetch_boxscore(game_id: str) -> pd.DataFrame:
    from nba_api.stats.endpoints import boxscoretraditionalv3

    print(f"\n[3] Fetching BoxScoreTraditionalV3 for {game_id}…")
    time.sleep(0.8)

    raw = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id, timeout=90)
    dfs = raw.get_data_frames()

    print(f"  {len(dfs)} dataframes returned")
    for i, d in enumerate(dfs):
        print(f"  df[{i}] shape={d.shape}  cols={list(d.columns)[:10]}")

    # Identify player-level df (has personId column)
    player_df: pd.DataFrame | None = None
    for d in dfs:
        if "personId" in d.columns or "PLAYER_ID" in d.columns:
            player_df = d
            break
    if player_df is None:
        player_df = dfs[0]

    print(f"\n  Player df columns: {list(player_df.columns)}")
    print(f"\n  All players:")
    id_col   = "personId"   if "personId"   in player_df.columns else "PLAYER_ID"
    name_col = "playerName" if "playerName" in player_df.columns else "PLAYER_NAME"
    team_col = "teamId"     if "teamId"     in player_df.columns else "TEAM_ID"
    starter_col = next(
        (c for c in ["starter", "START_POSITION", "startPosition"] if c in player_df.columns),
        None,
    )

    show = [c for c in [id_col, name_col, team_col, starter_col,
                         "points", "PTS", "reboundsTotal", "REB",
                         "assists", "AST", "steals", "STL", "blocks", "BLK"]
            if c and c in player_df.columns]
    print(player_df[show].to_string())

    if starter_col:
        starters = player_df[player_df[starter_col].astype(str).isin(["1", "G", "F", "C"])]
        print(f"\n  Starters ({len(starters)}):")
        print(starters[[id_col, name_col, team_col, starter_col]].to_string())

    _save_df(player_df, "player_boxscore.json")
    return player_df


# ── main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    meta_path = DATA_DIR / "game_meta.json"

    # Re-use cached game_id if available
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        game_id = meta["game_id"]
        print(f"Using cached game_id: {game_id}")
    else:
        game_id = find_game_id()
        meta = {
            "game_id": game_id,
            "home_team_id": GSW_ID,
            "away_team_id": LAL_ID,
        }
        _save_json(meta, "game_meta.json")

    fetch_pbp(game_id)
    fetch_boxscore(game_id)

    print("\n✓ fetch_pbp.py complete — data saved to", DATA_DIR)
