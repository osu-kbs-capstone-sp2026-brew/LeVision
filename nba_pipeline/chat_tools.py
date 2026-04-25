from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Optional

from .data_service import (
    get_game_play_by_play as ds_get_game_play_by_play,
    get_player_game_stat_by_date as ds_get_player_game_stat_by_date,
    get_player_last_n_games_stat as ds_get_player_last_n_games_stat,
    get_team_recent_games as ds_get_team_recent_games,
)
from .query_executor import execute_structured_queries
from .query_planner import plan_query, repair_structured_queries
from .query_schema import validate_structured_queries
from .settings import Settings

LOGGER = logging.getLogger("nba_pipeline.chat_tools")


def get_player_last_n_games_stat(
    player_name: str,
    stat_name: str,
    n: int,
    settings: Optional[Settings] = None,
) -> dict[str, Any]:
    return ds_get_player_last_n_games_stat(
        player_query=player_name,
        stat_name=stat_name,
        n=n,
        settings=settings,
    )


def get_player_game_stat_by_date(
    player_name: str,
    stat_name: str,
    target_date: str,
    settings: Optional[Settings] = None,
) -> dict[str, Any]:
    return ds_get_player_game_stat_by_date(
        player_query=player_name,
        stat_name=stat_name,
        target_date=target_date,
        settings=settings,
    )


def get_team_recent_games(
    team_name: str,
    n: int,
    settings: Optional[Settings] = None,
) -> dict[str, Any]:
    return ds_get_team_recent_games(team_query=team_name, n=n, settings=settings)


def get_game_play_by_play(
    team_name: Optional[str] = None,
    event_id: Optional[str] = None,
    target_date: Optional[str] = None,
    settings: Optional[Settings] = None,
) -> dict[str, Any]:
    return ds_get_game_play_by_play(
        team_name=team_name,
        event_id=event_id,
        target_date=target_date,
        settings=settings,
    )


def _detect_final_two_only(query: str) -> bool:
    lower = query.lower()
    return "final 2 minute" in lower or "last 2 minute" in lower


def _clock_seconds(play: dict[str, Any]) -> Optional[int]:
    clock = play.get("clock")
    display = None
    if isinstance(clock, dict):
        display = clock.get("displayValue") or clock.get("value")
    elif clock is not None:
        display = clock
    if display is None:
        display = play.get("clockDisplayValue")
    if display is None:
        return None

    text = str(display).strip()
    match = re.fullmatch(r"(\d{1,2}):(\d{2})", text)
    if not match:
        return None
    minutes = int(match.group(1))
    seconds = int(match.group(2))
    return minutes * 60 + seconds


def _filter_final_two_minutes(plays: list[dict[str, Any]]) -> list[dict[str, Any]]:
    max_period = 0
    for play in plays:
        period = play.get("period")
        number = None
        if isinstance(period, dict):
            number = period.get("number")
        elif isinstance(period, int):
            number = period
        if isinstance(number, int):
            max_period = max(max_period, number)

    filtered: list[dict[str, Any]] = []
    for play in plays:
        period = play.get("period")
        number = None
        if isinstance(period, dict):
            number = period.get("number")
        elif isinstance(period, int):
            number = period

        if max_period and isinstance(number, int) and number != max_period:
            continue

        seconds_left = _clock_seconds(play)
        if seconds_left is not None and seconds_left <= 120:
            filtered.append(play)

    return filtered


def _render_date(date_text: Any) -> str:
    text = str(date_text or "")[:10]
    if not text:
        return "unknown date"
    try:
        return datetime.strptime(text, "%Y-%m-%d").strftime("%b %d, %Y")
    except ValueError:
        return text


def _render_number(value: Any) -> str:
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _ordinal_label(rank: int) -> str:
    ordinal_words = {
        1: "highest",
        2: "second",
        3: "third",
        4: "fourth",
        5: "fifth",
        6: "sixth",
        7: "seventh",
        8: "eighth",
        9: "ninth",
        10: "tenth",
    }
    if rank in ordinal_words:
        return ordinal_words[rank]
    suffix = "th"
    if rank % 100 not in {11, 12, 13}:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(rank % 10, "th")
    return f"{rank}{suffix}"


def _game_stat_leader_sentence(
    *,
    leader_name: str,
    leader_value: Any,
    stat_label: str,
    operation: str,
    rank: int,
) -> str:
    direction = "lowest" if operation == "min_single_game" else "highest"
    if stat_label == "points":
        scorer_label = direction if rank <= 1 else f"{_ordinal_label(rank)}-{direction}"
        return (
            f"The {scorer_label} scorer in that game was {leader_name} "
            f"with {_render_number(leader_value)} points."
        )

    ranked_label = direction if rank <= 1 else f"{_ordinal_label(rank)}-{direction}"
    return (
        f"The {ranked_label} {stat_label} total in that game was {leader_name} "
        f"with {_render_number(leader_value)}."
    )


def _team_display_name(team: Any) -> str:
    if not isinstance(team, dict):
        return "Unknown team"
    display = str(team.get("display_name") or "").strip()
    if display:
        return display
    abbreviation = str(team.get("abbreviation") or "").strip()
    if abbreviation:
        return abbreviation
    location = str(team.get("location") or "").strip()
    name = str(team.get("name") or "").strip()
    combined = " ".join(part for part in [location, name] if part).strip()
    return combined or "Unknown team"


def _team_compact_name(team: Any) -> str:
    if not isinstance(team, dict):
        return "UNK"
    abbreviation = str(team.get("abbreviation") or "").strip()
    if abbreviation:
        return abbreviation
    return _team_display_name(team)


def _format_game_summary(game: dict[str, Any]) -> str:
    home_team = game.get("home_team") if isinstance(game.get("home_team"), dict) else {}
    away_team = game.get("away_team") if isinstance(game.get("away_team"), dict) else {}
    date_text = _render_date(game.get("date"))
    away_points = game.get("away_points")
    home_points = game.get("home_points")
    score = (
        f"{_team_compact_name(away_team)} {_render_number(away_points)}, "
        f"{_team_compact_name(home_team)} {_render_number(home_points)}"
        if away_points is not None and home_points is not None
        else f"{_team_compact_name(away_team)} vs {_team_compact_name(home_team)}"
    )
    return f"{date_text}: {score}"


def _format_stat_game_row(game: dict[str, Any]) -> str:
    date_text = _render_date(game.get("date"))
    opponent = str(game.get("opponent") or "UNK")
    value = game.get("stat_value", game.get("value"))
    return f"- {date_text} vs {opponent}: {_render_number(value)}"


def _format_game_entity_stat_result(payload: dict[str, Any]) -> str:
    result = payload.get("result") or {}
    if not isinstance(result, dict):
        return "I could not find a game-level stat result."

    leader = result.get("leader")
    stat = payload.get("stat") or {}
    stat_label = str(stat.get("label") or stat.get("field") or "stat")
    if not isinstance(leader, dict):
        return str(payload.get("message") or f"I could not find player {stat_label} values for that game.")

    leader_name = str(leader.get("name") or "Unknown player")
    leader_value = leader.get("stat_value")
    leader_rank = int(leader.get("rank") or payload.get("rank") or 1)
    operation = str(payload.get("operation") or "")
    game = result.get("game") if isinstance(result.get("game"), dict) else payload.get("game")
    sentence = _game_stat_leader_sentence(
        leader_name=leader_name,
        leader_value=leader_value,
        stat_label=stat_label,
        operation=operation,
        rank=leader_rank,
    )
    if isinstance(game, dict):
        return f"{sentence} ({_format_game_summary(game)})"
    return sentence


def _format_stat_result(payload: dict[str, Any]) -> str:
    query = payload.get("query") if isinstance(payload.get("query"), dict) else {}
    if query.get("entity_type") == "game":
        return _format_game_entity_stat_result(payload)

    player = payload.get("player") or {}
    team = payload.get("team") or {}
    stat = payload.get("stat") or {}
    player_name = str(player.get("name") or "").strip()
    team_name = (
        str(team.get("abbreviation") or "").strip()
        or str(team.get("name") or "").strip()
        or (
            f"{str(team.get('location') or '').strip()} {str(team.get('name') or '').strip()}".strip()
            if isinstance(team, dict)
            else ""
        )
    )
    subject_name = player_name or team_name or "Entity"
    stat_label = str(stat.get("label") or stat.get("field") or "stat")

    operation = str(payload.get("operation") or "")
    if operation == "game_log":
        games = payload.get("games") or []
        if not games:
            return f"I could not find {subject_name}'s {stat_label} for that scope."

        lines = [f"{subject_name} {stat_label} ({len(games)} games):"]
        for game in games:
            if isinstance(game, dict):
                lines.append(_format_stat_game_row(game))
        return "\n".join(lines)

    if operation == "latest_game":
        game = payload.get("game")
        if not isinstance(game, dict):
            return f"I could not find a latest game for {subject_name}."
        date_text = _render_date(game.get("date"))
        opponent = str(game.get("opponent") or "UNK")
        value = game.get("stat_value", game.get("value"))
        return f"In {subject_name}'s last game ({date_text} vs {opponent}), {stat_label} was {_render_number(value)}."

    if operation in {"sum", "average", "max_single_game", "min_single_game"}:
        aggregate = payload.get("aggregate") or {}
        if not isinstance(aggregate, dict) or aggregate.get("status") != "ok":
            return f"I could not compute {operation} for {subject_name} {stat_label}."

        value = aggregate.get("value")
        if operation == "sum":
            sample_size = aggregate.get("sample_size")
            return f"{subject_name} total {stat_label}: {_render_number(value)} across {sample_size} games."

        if operation == "average":
            sample_size = aggregate.get("sample_size")
            try:
                numeric_value = float(value)
                rendered = f"{numeric_value:.2f}"
            except (TypeError, ValueError):
                rendered = str(value)
            return f"{subject_name} average {stat_label}: {rendered} across {sample_size} games."

        game = aggregate.get("game")
        if not isinstance(game, dict):
            return f"I could not find a {operation} game for {subject_name} {stat_label}."

        direction = "highest" if operation == "max_single_game" else "lowest"
        return (
            f"{subject_name} {direction} {stat_label} game: {_render_number(value)}\n"
            f"{_format_stat_game_row(game)}"
        )

    return "I could not format this stat response."


def _format_single_game_lookup_result(game: dict[str, Any]) -> str:
    winner = game.get("winner") if isinstance(game.get("winner"), dict) else None
    loser = game.get("loser") if isinstance(game.get("loser"), dict) else None
    date_text = _render_date(game.get("date"))
    home_points = game.get("home_points")
    away_points = game.get("away_points")
    if winner and loser and home_points is not None and away_points is not None:
        winning_score = away_points if winner.get("id") == (game.get("away_team") or {}).get("id") else home_points
        losing_score = home_points if winner.get("id") == (game.get("away_team") or {}).get("id") else away_points
        return (
            f"{_team_display_name(winner)} won on {date_text}, "
            f"beating {_team_display_name(loser)} {_render_number(winning_score)}-{_render_number(losing_score)}."
        )
    return f"I found the game on {date_text}. {_format_game_summary(game)}"


def _format_game_lookup_result(payload: dict[str, Any]) -> str:
    operation = str(payload.get("operation") or "")
    result = payload.get("result")

    if operation == "single_game_lookup" and isinstance(result, dict):
        return _format_single_game_lookup_result(result)

    if not isinstance(result, dict):
        return "I could not find matching games."

    team = result.get("team") or {}
    team_label = team.get("abbreviation") or team.get("name") or "Team"
    games = result.get("games") or []
    if not games:
        return f"I could not find recent games for {team_label}."

    lines = [f"Recent games for {team_label}:"]
    for game in games:
        if not isinstance(game, dict):
            continue
        event_id = game.get("game_id") or game.get("event_id")
        date_text = str(game.get("date") or "")[:10]
        opponent = game.get("opponent") or "UNK"
        status = game.get("status") or "unknown"
        home_points = game.get("home_points")
        away_points = game.get("away_points")
        score = (
            f"{_render_number(away_points)}-{_render_number(home_points)}"
            if away_points is not None and home_points is not None
            else "N/A"
        )
        lines.append(
            f"- {date_text} vs {opponent} ({status}), score {score}, event {event_id}"
        )

    return "\n".join(lines)


def _format_play_by_play_result(payload: dict[str, Any], final_two_only: bool = False) -> str:
    result = payload.get("result") or {}
    event_id = result.get("event_id")
    plays = result.get("plays") or []
    if not isinstance(plays, list):
        plays = []

    if final_two_only:
        plays = _filter_final_two_minutes([row for row in plays if isinstance(row, dict)])
    else:
        plays = [row for row in plays if isinstance(row, dict)]

    if not plays:
        scope = "final 2 minutes" if final_two_only else "play-by-play"
        return f"I could not find {scope} data for event {event_id}."

    lines = [
        (
            f"Final 2 minutes for event {event_id} (showing up to 25 plays):"
            if final_two_only
            else f"Play-by-play for event {event_id} (showing up to 25 plays):"
        )
    ]

    for play in plays[:25]:
        text = play.get("text") or play.get("shortText") or "(no text)"
        clock = None
        period = None

        if isinstance(play.get("clock"), dict):
            clock = play["clock"].get("displayValue")
        if isinstance(play.get("period"), dict):
            period = play["period"].get("number")

        prefix = ""
        if period is not None:
            prefix += f"Q{period} "
        if clock:
            prefix += f"{clock} "
        lines.append(f"- {prefix.strip()} {text}".strip())

    return "\n".join(lines)


def _compose_linked_game_answer(results: list[dict[str, Any]]) -> Optional[str]:
    if len(results) != 2:
        return None

    first, second = results
    if str(first.get("result_type") or "") != "game_lookup":
        return None
    if str(first.get("operation") or "") != "single_game_lookup":
        return None
    if str(second.get("result_type") or "") != "stat_query":
        return None

    second_query = second.get("query") if isinstance(second.get("query"), dict) else {}
    if second_query.get("entity_type") != "game":
        return None

    first_result = first.get("result")
    second_result = second.get("result")
    if not isinstance(first_result, dict) or not isinstance(second_result, dict):
        return None

    leader = second_result.get("leader")
    if not isinstance(leader, dict):
        return None

    stat = second.get("stat") or {}
    stat_label = str(stat.get("label") or stat.get("field") or "stat")
    leader_name = str(leader.get("name") or "Unknown player")
    leader_value = leader.get("stat_value")
    leader_rank = int(leader.get("rank") or second.get("rank") or 1)
    operation = str(second.get("operation") or "")
    return (
        f"{_format_single_game_lookup_result(first_result)} "
        f"{_game_stat_leader_sentence(leader_name=leader_name, leader_value=leader_value, stat_label=stat_label, operation=operation, rank=leader_rank)}"
    )


def _format_multi_execution_result(payload: dict[str, Any], final_two_only: bool = False) -> str:
    results = payload.get("results") or []
    if not isinstance(results, list) or not results:
        return str(payload.get("message") or "NBA query execution failed.")

    linked = _compose_linked_game_answer([row for row in results if isinstance(row, dict)])
    if linked:
        return linked

    rendered: list[str] = []
    for result in results:
        if not isinstance(result, dict):
            continue
        rendered_text = _format_execution_result(result, final_two_only=final_two_only)
        if rendered_text:
            rendered.append(rendered_text)
    return "\n".join(rendered) if rendered else "NBA query execution failed."


def _format_execution_result(payload: dict[str, Any], final_two_only: bool = False) -> str:
    if str(payload.get("result_type") or "") == "multi_query":
        return _format_multi_execution_result(payload, final_two_only=final_two_only)

    status = str(payload.get("status") or "")
    if status == "clarification":
        message = str(payload.get("message") or "I need clarification.")
        candidates = payload.get("candidates") or []
        if isinstance(candidates, list) and candidates:
            candidate_list = ", ".join(str(item) for item in candidates[:6])
            return f"{message} Candidates: {candidate_list}."
        return message

    if status == "error":
        return str(payload.get("message") or "NBA query execution failed.")

    if status == "no_data":
        if payload.get("result_type") == "stat_query":
            return _format_stat_result(payload)
        return str(payload.get("message") or "No data matched the query.")

    result_type = str(payload.get("result_type") or "")
    if result_type == "stat_query":
        return _format_stat_result(payload)
    if result_type == "play_by_play_query":
        return _format_play_by_play_result(payload, final_two_only=final_two_only)
    if result_type == "game_lookup":
        return _format_game_lookup_result(payload)

    return "I could not format the NBA tool result."


def _normalize_conversation_history(
    conversation_history: Optional[list[dict[str, Any]]],
    current_query: str,
) -> list[dict[str, str]]:
    if not isinstance(conversation_history, list):
        return []

    normalized: list[dict[str, str]] = []
    for index, message in enumerate(conversation_history):
        if not isinstance(message, dict):
            continue
        role = str(message.get("role") or "").strip().lower()
        content = str(message.get("content") or "").strip()
        if role not in {"system", "user", "assistant"} or not content:
            continue
        if (
            role == "user"
            and content == current_query.strip()
            and index == len(conversation_history) - 1
        ):
            continue
        normalized.append({"role": role, "content": content})

    return normalized[-8:]


def answer_query(
    query: str,
    settings: Optional[Settings] = None,
    conversation_history: Optional[list[dict[str, Any]]] = None,
) -> dict[str, Any]:
    if not query.strip():
        return {
            "matched": False,
            "query": query,
            "answer": None,
            "tool": None,
            "args": None,
            "result": None,
        }

    final_two_only = _detect_final_two_only(query)
    normalized_history = _normalize_conversation_history(conversation_history, query)
    LOGGER.debug("structured_flow raw_query=%s", query)
    LOGGER.debug("structured_flow history_messages=%s", len(normalized_history))

    try:
        plan = plan_query(query, conversation_history=normalized_history)
    except Exception as exc:
        LOGGER.error("Planner failed for query: %s (%s)", query, exc)
        return {
            "matched": True,
            "query": query,
            "tool": "structured_query_planner",
            "args": None,
            "result": None,
            "answer": None,
            "error": str(exc),
        }

    if not plan.matched:
        return {
            "matched": False,
            "query": query,
            "tool": None,
            "args": None,
            "result": None,
            "answer": None,
            "planner_reason": plan.reason,
            "planner_raw": plan.raw,
        }

    LOGGER.debug(
        "structured_flow raw_planner_output=%s",
        plan.raw,
    )

    repaired_queries = repair_structured_queries(plan.queries, query)
    repaired_query_dicts = [item.to_dict() for item in repaired_queries]
    LOGGER.debug("structured_flow repaired_queries=%s", repaired_query_dicts)
    if not repaired_queries:
        return {
            "matched": True,
            "query": query,
            "tool": "structured_query_planner",
            "args": [],
            "result": None,
            "answer": None,
            "planner_reason": plan.reason,
            "planner_raw": plan.raw,
            "error": "The planner handled the request but could not extract any executable NBA sub-queries.",
        }

    try:
        validated_queries = validate_structured_queries(repaired_queries)
    except Exception as exc:
        LOGGER.error("Structured query validation failed after repair: %s", exc)
        return {
            "matched": True,
            "query": query,
            "tool": "structured_query_validation",
            "args": repaired_query_dicts,
            "result": None,
            "answer": None,
            "error": f"Structured query validation failed: {exc}",
        }

    validated_query_dicts = [item.to_dict() for item in validated_queries]
    LOGGER.debug("structured_flow validated_queries=%s", validated_query_dicts)

    try:
        execution = execute_structured_queries(validated_queries, settings=settings)
    except Exception as exc:
        LOGGER.error("Executor crashed for query: %s (%s)", query, exc)
        return {
            "matched": True,
            "query": query,
            "tool": "structured_query_executor",
            "args": validated_query_dicts,
            "result": None,
            "answer": None,
            "error": str(exc),
        }

    status = str(execution.get("status") or "")
    if status == "error" and str(execution.get("result_type") or "") != "multi_query":
        return {
            "matched": True,
            "query": query,
            "tool": "structured_query_executor",
            "args": validated_query_dicts,
            "result": execution,
            "answer": None,
            "error": str(execution.get("message") or "NBA query execution failed."),
        }

    answer = _format_execution_result(execution, final_two_only=final_two_only)
    if status == "error" and not answer:
        return {
            "matched": True,
            "query": query,
            "tool": "structured_query_executor",
            "args": validated_query_dicts,
            "result": execution,
            "answer": None,
            "error": str(execution.get("message") or "NBA query execution failed."),
        }

    return {
        "matched": True,
        "query": query,
        "tool": "structured_query_executor",
        "args": validated_query_dicts,
        "plan": validated_query_dicts,
        "planner_reason": plan.reason,
        "planner_raw": plan.raw,
        "result": execution,
        "answer": answer,
    }
