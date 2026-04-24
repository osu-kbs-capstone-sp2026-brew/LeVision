from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

import requests

from .query_schema import (
    ALLOWED_ENTITY_TYPES,
    ALLOWED_INTENTS,
    ALLOWED_OPERATIONS,
    ALLOWED_RELATIVE_DATE_VALUES,
    ALLOWED_SCOPE_TYPES,
    ALLOWED_STATS,
    QueryScope,
    StructuredQuery,
    coerce_structured_queries,
    planner_envelope_json_schema,
)
from .settings import load_env_files

LOGGER = logging.getLogger("nba_pipeline.query_planner")
OPENAI_CHAT_COMPLETIONS_URL = "https://api.openai.com/v1/chat/completions"
LOCAL_TZ = ZoneInfo("America/New_York")


@dataclass(slots=True)
class QueryPlan:
    matched: bool
    queries: list[StructuredQuery] = field(default_factory=list)
    reason: Optional[str] = None
    raw: Optional[dict[str, Any]] = None


STAT_PHRASE_PATTERN = (
    r"(?:points?|assists?|rebounds?|steals?|blocks?|turnovers?|minutes?|fouls?|"
    r"plus(?:\s|-)?minus|\+/-|scoring|"
    r"(?:three|3)\s*(?:pt|pointer|pointers)|"
    r"field\s*goals?|free\s*throws?|"
    r"offensive\s*rebounds?|defensive\s*rebounds?|"
    r"starter|did\s*not\s*play|dnp|ejected|reason|"
    r"status|venue|points?\s*in\s*paint|fast\s*break\s*points|largest\s*lead)"
)
PLAYER_INFERENCE_PATTERNS = [
    re.compile(
        rf"^\s*(?:what\s+was|what\s+were|how\s+many|show\s+me|tell\s+me|give\s+me|who\s+had)?\s*"
        rf"(?P<player>[A-Za-z][A-Za-z .'\-]{{1,50}}?)"
        rf"(?:['’]s)?\s+{STAT_PHRASE_PATTERN}\b",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\bfor\s+(?P<player>[A-Za-z][A-Za-z .'\-]{{1,50}})\b",
        re.IGNORECASE,
    ),
]
PLAYER_STOP_TOKENS = {
    "last",
    "past",
    "recent",
    "game",
    "games",
    "season",
    "this",
    "that",
    "today",
    "yesterday",
    "tonight",
    "night",
    "over",
    "in",
    "on",
    "the",
    "of",
    "highest",
    "lowest",
    "average",
    "total",
    "sum",
    "max",
    "min",
    "between",
    "and",
}
TEAM_STOP_TOKENS = {
    "game",
    "games",
    "season",
    "today",
    "yesterday",
    "tonight",
    "night",
    "last",
    "this",
    "that",
    "in",
    "on",
}

STAT_HINTS: list[tuple[re.Pattern[str], str]] = [
    (
        re.compile(
            r"(points?\s*in\s*paint|points?\b.{0,40}\bin\s+(?:the\s+)?paint)",
            re.IGNORECASE,
        ),
        "points_in_paint",
    ),
    (re.compile(r"\b(fast\s*break\s*points)\b", re.IGNORECASE), "fast_break_points"),
    (re.compile(r"\b(home\s*points)\b", re.IGNORECASE), "home_points"),
    (re.compile(r"\b(away\s*points)\b", re.IGNORECASE), "away_points"),
    (re.compile(r"\b(largest\s*lead)\b", re.IGNORECASE), "largest_lead"),
    (re.compile(r"\b(points?|pts?|scoring|score)\b", re.IGNORECASE), "points"),
    (re.compile(r"\b(assists?|ast)\b", re.IGNORECASE), "assists"),
    (re.compile(r"\b(rebounds?|reb)\b", re.IGNORECASE), "rebounds"),
    (re.compile(r"\b(offensive\s+rebounds?|oreb)\b", re.IGNORECASE), "offensive_rebounds"),
    (re.compile(r"\b(defensive\s+rebounds?|dreb)\b", re.IGNORECASE), "defensive_rebounds"),
    (re.compile(r"\b(steals?|stl)\b", re.IGNORECASE), "steals"),
    (re.compile(r"\b(blocks?|blk)\b", re.IGNORECASE), "blocks"),
    (re.compile(r"\b(turnovers?|to)\b", re.IGNORECASE), "turnovers"),
    (re.compile(r"\b(minutes?|mins?|min)\b", re.IGNORECASE), "minutes"),
    (re.compile(r"\b(fouls?|pf)\b", re.IGNORECASE), "fouls"),
    (re.compile(r"\b(plus\s*minus|plus-minus|\+/-)\b", re.IGNORECASE), "plus_minus"),
    (re.compile(r"\b(starter)\b", re.IGNORECASE), "starter"),
    (re.compile(r"\b(did\s*not\s*play|dnp)\b", re.IGNORECASE), "did_not_play"),
    (re.compile(r"\b(ejected)\b", re.IGNORECASE), "ejected"),
    (re.compile(r"\b(reason)\b", re.IGNORECASE), "reason"),
    (re.compile(r"\b(fgm|field\s*goals?\s*made)\b", re.IGNORECASE), "fg_made"),
    (re.compile(r"\b(fga|field\s*goals?\s*attempted)\b", re.IGNORECASE), "fg_attempted"),
    (
        re.compile(
            r"\b((?:3|three)\s*(?:pt|pointer|pointers?)\s*(?:made|makes?)|3pm)\b",
            re.IGNORECASE,
        ),
        "three_ptr_made",
    ),
    (
        re.compile(
            r"\b((?:3|three)\s*(?:pt|pointer|pointers?)\s*(?:attempted|attempts?)|3pa)\b",
            re.IGNORECASE,
        ),
        "three_ptr_attempted",
    ),
    (re.compile(r"\b(ftm|free\s*throws?\s*made)\b", re.IGNORECASE), "ft_made"),
    (re.compile(r"\b(fta|free\s*throws?\s*attempted)\b", re.IGNORECASE), "ft_attempted"),
    (re.compile(r"\b(venue)\b", re.IGNORECASE), "venue"),
    (re.compile(r"\b(status)\b", re.IGNORECASE), "status"),
]
RELATIVE_DATE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\blast\s+night\b", re.IGNORECASE), "last_night"),
    (re.compile(r"\byesterday\b", re.IGNORECASE), "yesterday"),
    (re.compile(r"\btonight\b", re.IGNORECASE), "tonight"),
    (re.compile(r"\btoday\b", re.IGNORECASE), "today"),
]
ORDINAL_WORD_TO_RANK: dict[str, int] = {
    "first": 1,
    "second": 2,
    "third": 3,
    "fourth": 4,
    "fifth": 5,
    "sixth": 6,
    "seventh": 7,
    "eighth": 8,
    "ninth": 9,
    "tenth": 10,
}
RANKED_EXTREMA_PATTERN = re.compile(
    r"\b(?:(?P<word>first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth)|"
    r"(?P<num>\d{1,2})(?:st|nd|rd|th))\s+(?P<qualifier>highest|lowest|most|fewest|least)\b",
    re.IGNORECASE,
)


def _planner_model() -> str:
    return (
        os.getenv("NBA_QUERY_PLANNER_MODEL")
        or os.getenv("LEVISION_OPENAI_MODEL")
        or "gpt-5.4"
    )


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "on"}


def _local_now() -> datetime:
    return datetime.now(LOCAL_TZ)


def _normalize_text_for_repair(value: Any) -> str:
    text = str(value or "").strip()
    text = text.replace("\u2019", "'").replace("\u2018", "'")
    text = re.sub(r"\s+", " ", text)
    return text


def _normalize_entity_name(value: Any) -> Optional[str]:
    text = _normalize_text_for_repair(value)
    if not text:
        return None
    text = re.sub(r"^(?:the)\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"['’]s\b", "", text, flags=re.IGNORECASE)
    text = text.strip(" ,.!?\"'")
    return text or None


def _normalize_hint_text(value: Any) -> Optional[str]:
    text = _normalize_text_for_repair(value)
    if not text:
        return None
    return text.strip(" ,.!?\"'") or None


def _normalize_relative_date(value: Any) -> Optional[str]:
    text = _normalize_text_for_repair(value).lower().replace("-", "_").replace(" ", "_")
    if not text:
        return None
    if text == "last_night":
        return "last_night"
    if text in ALLOWED_RELATIVE_DATE_VALUES:
        return text
    return None


def _normalize_team_list(values: Any) -> Optional[list[str]]:
    if values is None:
        return None
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, list):
        return None

    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        team = _normalize_entity_name(value)
        if not team:
            continue
        key = team.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(team)
    return normalized or None


def _normalize_rank(value: Any) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    if text in ORDINAL_WORD_TO_RANK:
        return ORDINAL_WORD_TO_RANK[text]
    try:
        parsed = int(text)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _is_plausible_player_name(value: str) -> bool:
    if not value or any(char.isdigit() for char in value):
        return False
    words = [token for token in re.split(r"\s+", value) if token]
    if not words or len(words) > 4:
        return False
    lowered = [token.lower() for token in words]
    if all(token in PLAYER_STOP_TOKENS for token in lowered):
        return False
    return True


def _infer_player_from_query(user_query: str) -> Optional[str]:
    normalized = _normalize_text_for_repair(user_query)
    if not normalized:
        return None

    for pattern in PLAYER_INFERENCE_PATTERNS:
        match = pattern.search(normalized)
        if not match:
            continue
        player = _normalize_entity_name(match.group("player"))
        if player and _is_plausible_player_name(player):
            return player
    return None


def _infer_stat_from_query(user_query: str) -> Optional[str]:
    for pattern, stat_name in STAT_HINTS:
        if pattern.search(user_query):
            return stat_name
    return None


def _infer_rank_from_query(user_query: str) -> Optional[int]:
    match = RANKED_EXTREMA_PATTERN.search(user_query)
    if not match:
        return None
    ordinal_word = str(match.group("word") or "").lower()
    if ordinal_word:
        return ORDINAL_WORD_TO_RANK.get(ordinal_word)
    try:
        numeric_rank = int(str(match.group("num") or "").strip())
    except ValueError:
        return None
    return numeric_rank if numeric_rank > 0 else None


def _infer_ranked_operation_from_query(user_query: str) -> Optional[str]:
    match = RANKED_EXTREMA_PATTERN.search(user_query)
    if not match:
        return None
    qualifier = str(match.group("qualifier") or "").lower()
    if qualifier in {"lowest", "fewest", "least"}:
        return "min_single_game"
    return "max_single_game"


def _infer_recent_games_count(user_query: str) -> Optional[int]:
    match = re.search(
        r"\b(?:last|past|recent|most\s+recent)\s+(\d{1,2})\s+games?\b",
        user_query,
        re.IGNORECASE,
    )
    if not match:
        return None
    try:
        count = int(match.group(1))
    except ValueError:
        return None
    return count if count > 0 else None


def _infer_relative_date_from_query(user_query: str) -> Optional[str]:
    for pattern, relative_date in RELATIVE_DATE_PATTERNS:
        if pattern.search(user_query):
            return relative_date
    return None


def _relative_date_to_local_date(relative_date: str, now_local: Optional[datetime] = None) -> Optional[str]:
    now = now_local or _local_now()
    normalized = _normalize_relative_date(relative_date)
    if normalized == "today":
        return now.date().isoformat()
    if normalized == "tonight":
        return now.date().isoformat()
    if normalized == "yesterday":
        return (now.date() - timedelta(days=1)).isoformat()
    if normalized == "last_night":
        return (now.date() - timedelta(days=1)).isoformat()
    return None


def _infer_matchup_hints(user_query: str) -> tuple[Optional[list[str]], Optional[str]]:
    normalized = _normalize_text_for_repair(user_query)

    between_match = re.search(
        r"\bbetween\s+(?:the\s+)?(?P<team1>[A-Za-z .'\-]+?)\s+and\s+(?:the\s+)?(?P<team2>[A-Za-z .'\-]+?)(?=(?:\s*,|\s+and\s+who|\s+and\s+what|\?|$))",
        normalized,
        re.IGNORECASE,
    )
    if between_match:
        team_1 = _normalize_entity_name(between_match.group("team1"))
        team_2 = _normalize_entity_name(between_match.group("team2"))
        teams = [team for team in [team_1, team_2] if team]
        if len(teams) == 2:
            return teams, None

    versus_match = re.search(
        r"\b(?:vs\.?|versus|against)\s+(?:the\s+)?(?P<team>[A-Za-z .'\-]+?)(?=(?:\s+in\b|\s+on\b|\s+last\b|\s+yesterday|\s+today|\s+tonight|[?.!,]|$))",
        normalized,
        re.IGNORECASE,
    )
    if versus_match:
        opponent = _normalize_entity_name(versus_match.group("team"))
        if opponent:
            return None, opponent

    return None, None


def _infer_scope_type_from_query(
    user_query: str,
    *,
    scope: QueryScope,
    entity_hint: Optional[str],
) -> Optional[str]:
    if scope.game_id:
        return "specific_game"
    if scope.teams or scope.opponent:
        return "matchup_hint"
    if scope.relative_date:
        return "relative_date"
    if scope.date:
        return "date"
    if scope.start_date or scope.end_date:
        return "date_range"
    if scope.season or re.search(r"\bthis\s+season\b", user_query, re.IGNORECASE):
        return "season"
    if re.search(r"\b(last|past|recent|most\s+recent)\s+\d{1,2}\s+games?\b", user_query, re.IGNORECASE):
        return "recent_games"
    if entity_hint and re.search(r"\b(that|this)\s+game\b", entity_hint, re.IGNORECASE):
        return "game_hint"
    return None


def _infer_intent_from_query(user_query: str) -> str:
    if re.search(r"\bplay[- ]?by[- ]?play\b", user_query, re.IGNORECASE):
        return "play_by_play_query"
    if re.search(r"\b(who\s+won|winner|beat|defeated)\b", user_query, re.IGNORECASE):
        return "game_lookup"
    return "stat_query"


def _infer_entity_type_from_query(
    user_query: str,
    *,
    intent: str,
    player: Optional[str],
    team: Optional[str],
    scope: QueryScope,
) -> str:
    if intent == "game_lookup":
        if scope.type == "recent_games" and (team or scope.opponent):
            return "team"
        return "game"

    if intent == "play_by_play_query":
        return "game" if scope.game_id or scope.teams else "team"

    if re.search(r"\bthat\s+game\b", user_query, re.IGNORECASE):
        return "game"
    if player:
        return "player"
    if team:
        return "team"
    if scope.teams or scope.opponent:
        return "game" if re.search(r"\bwho\s+scored\b", user_query, re.IGNORECASE) else "player"
    return "player"


def _infer_operation_from_query(user_query: str, *, intent: str, entity_type: str, scope_type: str) -> Optional[str]:
    text = user_query.lower()
    if intent == "play_by_play_query":
        return "play_by_play"
    if intent == "game_lookup":
        if scope_type == "recent_games" and entity_type == "team":
            return "game_log"
        return "single_game_lookup"
    ranked_operation = _infer_ranked_operation_from_query(text)
    if ranked_operation:
        return ranked_operation
    if "highest" in text or "season high" in text or "max" in text or "most points" in text:
        return "max_single_game"
    if "lowest" in text or "min" in text:
        return "min_single_game"
    if "average" in text or "avg" in text or "mean" in text:
        return "average"
    if "total" in text or "sum" in text:
        return "sum"
    if "latest game" in text or "last game" in text or "most recent game" in text:
        return "latest_game"
    if scope_type in {"matchup_hint", "game_hint", "specific_game", "date", "relative_date"}:
        return "latest_game"
    if re.search(r"\b(last|past|recent|most\s+recent)\s+\d{1,2}\s+games?\b", text):
        return "game_log"
    return "game_log" if intent == "stat_query" else None


def _infer_game_reference_hint(user_query: str) -> Optional[str]:
    match = re.search(r"\b(that|this)\s+game\b", user_query, re.IGNORECASE)
    if not match:
        return None
    return match.group(0).lower()


def repair_structured_query(
    query: StructuredQuery,
    user_query: str,
    *,
    now_local: Optional[datetime] = None,
) -> StructuredQuery:
    normalized_query_text = _normalize_text_for_repair(user_query)
    current_local = now_local or _local_now()

    repaired_scope = QueryScope(
        type=str(query.scope.type or ""),
        count=query.scope.count,
        season=str(query.scope.season or "").strip() or None,
        date=str(query.scope.date or "").strip() or None,
        start_date=str(query.scope.start_date or "").strip() or None,
        end_date=str(query.scope.end_date or "").strip() or None,
        relative_date=_normalize_relative_date(query.scope.relative_date),  # type: ignore[arg-type]
        before_now=query.scope.before_now,
        game_id=str(query.scope.game_id or "").strip() or None,
        opponent=_normalize_entity_name(query.scope.opponent),
        teams=_normalize_team_list(query.scope.teams),
    )
    repaired_player = _normalize_entity_name(query.player)
    repaired_team = _normalize_entity_name(query.team)
    repaired_entity_hint = _normalize_hint_text(query.entity_hint)
    repaired_intent = str(query.intent or "").strip().lower()
    repaired_entity = str(query.entity_type or "").strip().lower()
    repaired_operation = str(query.operation or "").strip().lower()
    repaired_scope_type = str(repaired_scope.type or "").strip().lower()
    repaired_stat = str(query.stat or "").strip().lower() if query.stat is not None else None
    repaired_rank = _normalize_rank(query.rank)

    if not repaired_entity_hint:
        inferred_game_hint = _infer_game_reference_hint(normalized_query_text)
        if inferred_game_hint:
            repaired_entity_hint = inferred_game_hint

    inferred_teams, inferred_opponent = _infer_matchup_hints(normalized_query_text)
    if repaired_scope.teams is None and inferred_teams:
        repaired_scope.teams = inferred_teams
    if repaired_scope.opponent is None and inferred_opponent:
        repaired_scope.opponent = inferred_opponent

    if repaired_scope.relative_date is None:
        repaired_scope.relative_date = _infer_relative_date_from_query(normalized_query_text)  # type: ignore[assignment]
    if repaired_scope.relative_date and repaired_scope.date is None:
        repaired_scope.date = _relative_date_to_local_date(repaired_scope.relative_date, current_local)

    if repaired_intent not in ALLOWED_INTENTS:
        repaired_intent = _infer_intent_from_query(normalized_query_text)

    if repaired_player is None and repaired_intent == "stat_query":
        inferred_player = _infer_player_from_query(normalized_query_text)
        if inferred_player:
            repaired_player = inferred_player
            LOGGER.debug("Planner repair inferred player='%s' from query='%s'", inferred_player, user_query)

    if repaired_stat is None or repaired_stat in {"stat", "stats"}:
        inferred_stat = _infer_stat_from_query(normalized_query_text)
        if inferred_stat:
            repaired_stat = inferred_stat
    if repaired_rank is None:
        repaired_rank = _infer_rank_from_query(normalized_query_text)

    if repaired_scope_type not in ALLOWED_SCOPE_TYPES:
        inferred_scope_type = _infer_scope_type_from_query(
            normalized_query_text,
            scope=repaired_scope,
            entity_hint=repaired_entity_hint,
        )
        if inferred_scope_type:
            repaired_scope_type = inferred_scope_type
        elif repaired_intent == "game_lookup":
            repaired_scope_type = "game_hint"
        else:
            repaired_scope_type = "recent_games"

    repaired_scope.type = repaired_scope_type  # type: ignore[assignment]

    if repaired_entity not in ALLOWED_ENTITY_TYPES:
        repaired_entity = _infer_entity_type_from_query(
            normalized_query_text,
            intent=repaired_intent,
            player=repaired_player,
            team=repaired_team,
            scope=repaired_scope,
        )

    if repaired_intent == "stat_query" and repaired_entity == "player" and not repaired_player and repaired_team:
        repaired_entity = "team"

    if repaired_scope_type == "recent_games":
        if repaired_scope.count is None or repaired_scope.count <= 0:
            repaired_scope.count = _infer_recent_games_count(normalized_query_text) or 5
        if repaired_scope.before_now is None:
            repaired_scope.before_now = True
    elif repaired_scope_type == "season":
        repaired_scope.season = repaired_scope.season or "current"
    elif repaired_scope_type == "relative_date":
        repaired_scope.relative_date = repaired_scope.relative_date or _infer_relative_date_from_query(normalized_query_text)  # type: ignore[assignment]
        repaired_scope.date = repaired_scope.date or (
            _relative_date_to_local_date(repaired_scope.relative_date or "", current_local)
        )
    elif repaired_scope_type in {"matchup_hint", "game_hint"} and repaired_scope.before_now is None:
        repaired_scope.before_now = True

    if repaired_intent == "game_lookup" and repaired_entity == "team" and repaired_scope_type != "recent_games":
        repaired_entity = "game"

    inferred_operation = _infer_operation_from_query(
        normalized_query_text,
        intent=repaired_intent,
        entity_type=repaired_entity,
        scope_type=repaired_scope_type,
    )
    if repaired_operation not in ALLOWED_OPERATIONS:
        if inferred_operation:
            repaired_operation = inferred_operation
    elif (
        inferred_operation in {"max_single_game", "min_single_game"}
        and repaired_operation in {"game_log", "latest_game"}
        and repaired_intent == "stat_query"
        and (
            repaired_entity == "game"
            or repaired_scope_type in {"matchup_hint", "game_hint", "specific_game", "date", "relative_date"}
        )
    ):
        repaired_operation = inferred_operation

    if repaired_intent == "play_by_play_query":
        repaired_operation = "play_by_play"
        repaired_stat = None
        repaired_rank = None
    elif repaired_intent != "stat_query":
        repaired_rank = None

    if repaired_intent == "game_lookup" and repaired_operation == "game_log" and repaired_scope_type != "recent_games":
        repaired_operation = "single_game_lookup"

    if (
        repaired_intent == "stat_query"
        and repaired_scope_type in {"matchup_hint", "game_hint", "specific_game", "date", "relative_date"}
        and repaired_operation == "game_log"
    ):
        repaired_operation = "latest_game"
    if repaired_rank is not None and repaired_operation not in {"max_single_game", "min_single_game"}:
        repaired_operation = inferred_operation or "max_single_game"

    return StructuredQuery(
        intent=repaired_intent,  # type: ignore[arg-type]
        entity_type=repaired_entity,  # type: ignore[arg-type]
        player=repaired_player,
        team=repaired_team,
        entity_hint=repaired_entity_hint,
        stat=repaired_stat,  # type: ignore[arg-type]
        rank=repaired_rank,
        operation=repaired_operation,  # type: ignore[arg-type]
        scope=repaired_scope,
    )


def repair_structured_queries(
    queries: list[StructuredQuery],
    user_query: str,
    *,
    now_local: Optional[datetime] = None,
) -> list[StructuredQuery]:
    current_local = now_local or _local_now()
    return [
        repair_structured_query(query, user_query, now_local=current_local)
        for query in queries
    ]


def _strip_code_fences(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _extract_json_object(text: str) -> dict[str, Any]:
    cleaned = _strip_code_fences(text)

    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    start = cleaned.find("{")
    if start < 0:
        raise ValueError("Planner response did not include JSON")

    depth = 0
    end = -1
    for idx in range(start, len(cleaned)):
        char = cleaned[idx]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                end = idx + 1
                break

    if end < 0:
        raise ValueError("Planner response JSON was incomplete")

    parsed = json.loads(cleaned[start:end])
    if not isinstance(parsed, dict):
        raise ValueError("Planner response JSON must be an object")
    return parsed


def _system_prompt() -> str:
    return (
        "You are an NBA planner for a deterministic backend. "
        "Return ONLY the exact planner JSON envelope with keys should_handle, reason, and queries. "
        "Never answer with NBA stats. Never browse the web. Never write SQL. "
        "Use prior conversation context only to resolve references in the current request, such as pronouns or omitted team/player names. "
        "Never invent players, teams, dates, game_id values, or stats. "
        "Use null instead of guessing when a field cannot be safely filled. "
        "Never output off-schema keys. Never output empty strings for enum fields. "
        "If the user refers to a game indirectly, use hint scopes such as relative_date, matchup_hint, or game_hint instead of inventing scope.date or scope.game_id. "
        "For ranked game leader questions such as second most points in that game, use operation max_single_game or min_single_game with rank set to the requested ordinal. "
        "If the user asks a compound question, decompose it into multiple query objects inside queries. "
        "Normalize semantics: scoring -> points, highest scoring game -> operation max_single_game with stat points, "
        "last/past/most recent N games -> scope.type recent_games with count N and before_now true, "
        "this season -> scope.type season with season current, "
        "play-by-play -> intent play_by_play_query with operation play_by_play. "
        f"Allowed intent values: {sorted(ALLOWED_INTENTS)}. "
        f"Allowed entity_type values: {sorted(ALLOWED_ENTITY_TYPES)}. "
        f"Allowed operation values: {sorted(ALLOWED_OPERATIONS)}. "
        f"Allowed scope.type values: {sorted(ALLOWED_SCOPE_TYPES)}. "
        f"Relative date values: {sorted(ALLOWED_RELATIVE_DATE_VALUES)}. "
        f"Common stat values: {sorted(ALLOWED_STATS)}."
    )


def _few_shot_examples() -> list[tuple[str, dict[str, Any]]]:
    return [
        (
            "LeBron's points in the last 5 games",
            {
                "should_handle": True,
                "reason": "player stat query over a recent-games window",
                "queries": [
                    {
                        "intent": "stat_query",
                        "entity_type": "player",
                        "player": "LeBron James",
                        "team": None,
                        "entity_hint": None,
                        "stat": "points",
                        "rank": None,
                        "operation": "game_log",
                        "scope": {
                            "type": "recent_games",
                            "count": 5,
                            "season": None,
                            "date": None,
                            "start_date": None,
                            "end_date": None,
                            "relative_date": None,
                            "before_now": True,
                            "game_id": None,
                            "opponent": None,
                            "teams": None,
                        },
                    }
                ],
            },
        ),
        (
            "what was bam adebayo's highest scoring game this season?",
            {
                "should_handle": True,
                "reason": "player season stat maximum",
                "queries": [
                    {
                        "intent": "stat_query",
                        "entity_type": "player",
                        "player": "Bam Adebayo",
                        "team": None,
                        "entity_hint": None,
                        "stat": "points",
                        "rank": None,
                        "operation": "max_single_game",
                        "scope": {
                            "type": "season",
                            "count": None,
                            "season": "current",
                            "date": None,
                            "start_date": None,
                            "end_date": None,
                            "relative_date": None,
                            "before_now": True,
                            "game_id": None,
                            "opponent": None,
                            "teams": None,
                        },
                    }
                ],
            },
        ),
        (
            "who won yesterday between the spurs and the blazers",
            {
                "should_handle": True,
                "reason": "single game lookup from matchup and relative date hints",
                "queries": [
                    {
                        "intent": "game_lookup",
                        "entity_type": "game",
                        "player": None,
                        "team": None,
                        "entity_hint": None,
                        "stat": None,
                        "rank": None,
                        "operation": "single_game_lookup",
                        "scope": {
                            "type": "matchup_hint",
                            "count": None,
                            "season": None,
                            "date": None,
                            "start_date": None,
                            "end_date": None,
                            "relative_date": "yesterday",
                            "before_now": True,
                            "game_id": None,
                            "opponent": None,
                            "teams": ["Spurs", "Blazers"],
                        },
                    }
                ],
            },
        ),
        (
            "who won last night between the spurs and the blazers, and who scored the most points in that game",
            {
                "should_handle": True,
                "reason": "compound query with a matchup lookup followed by a game-scoped stat query",
                "queries": [
                    {
                        "intent": "game_lookup",
                        "entity_type": "game",
                        "player": None,
                        "team": None,
                        "entity_hint": None,
                        "stat": None,
                        "rank": None,
                        "operation": "single_game_lookup",
                        "scope": {
                            "type": "matchup_hint",
                            "count": None,
                            "season": None,
                            "date": None,
                            "start_date": None,
                            "end_date": None,
                            "relative_date": "last_night",
                            "before_now": True,
                            "game_id": None,
                            "opponent": None,
                            "teams": ["Spurs", "Blazers"],
                        },
                    },
                    {
                        "intent": "stat_query",
                        "entity_type": "game",
                        "player": None,
                        "team": None,
                        "entity_hint": "that game",
                        "stat": "points",
                        "rank": None,
                        "operation": "max_single_game",
                        "scope": {
                            "type": "game_hint",
                            "count": None,
                            "season": None,
                            "date": None,
                            "start_date": None,
                            "end_date": None,
                            "relative_date": None,
                            "before_now": True,
                            "game_id": None,
                            "opponent": None,
                            "teams": None,
                        },
                    },
                ],
            },
        ),
        (
            "how many points did lebron score vs the rockets",
            {
                "should_handle": True,
                "reason": "player stat query using an opponent matchup hint",
                "queries": [
                    {
                        "intent": "stat_query",
                        "entity_type": "player",
                        "player": "LeBron James",
                        "team": None,
                        "entity_hint": None,
                        "stat": "points",
                        "rank": None,
                        "operation": "latest_game",
                        "scope": {
                            "type": "matchup_hint",
                            "count": None,
                            "season": None,
                            "date": None,
                            "start_date": None,
                            "end_date": None,
                            "relative_date": None,
                            "before_now": True,
                            "game_id": None,
                            "opponent": "Rockets",
                            "teams": None,
                        },
                    }
                ],
            },
        ),
        (
            "who scored the second most points in that game",
            {
                "should_handle": True,
                "reason": "game-scoped follow-up asking for the second-ranked scorer from the previously resolved game",
                "queries": [
                    {
                        "intent": "stat_query",
                        "entity_type": "game",
                        "player": None,
                        "team": None,
                        "entity_hint": "that game",
                        "stat": "points",
                        "rank": 2,
                        "operation": "max_single_game",
                        "scope": {
                            "type": "game_hint",
                            "count": None,
                            "season": None,
                            "date": None,
                            "start_date": None,
                            "end_date": None,
                            "relative_date": None,
                            "before_now": True,
                            "game_id": None,
                            "opponent": None,
                            "teams": None,
                        },
                    }
                ],
            },
        ),
    ]


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
            and content == current_query
            and index == len(conversation_history) - 1
        ):
            continue
        normalized.append({"role": role, "content": content})

    return normalized[-8:]


def _format_conversation_context(conversation_history: list[dict[str, str]]) -> Optional[str]:
    if not conversation_history:
        return None

    lines = [
        "Conversation context (oldest to newest). Use this only to resolve references in the current request:",
    ]
    for message in conversation_history:
        role = message["role"].capitalize()
        lines.append(f"{role}: {message['content']}")
    return "\n".join(lines)


def _planner_messages(
    user_query: str,
    conversation_history: Optional[list[dict[str, Any]]] = None,
) -> list[dict[str, str]]:
    normalized_history = _normalize_conversation_history(conversation_history, user_query)
    conversation_context = _format_conversation_context(normalized_history)
    messages: list[dict[str, str]] = [{"role": "system", "content": _system_prompt()}]
    for example_query, example_plan in _few_shot_examples():
        messages.append(
            {
                "role": "user",
                "content": f"Plan this NBA request. User request: {example_query}",
            }
        )
        messages.append(
            {
                "role": "assistant",
                "content": json.dumps(example_plan, ensure_ascii=True),
            }
        )
    messages.append(
        {
            "role": "user",
            "content": (
                "Plan this NBA request. "
                "If it is unrelated to NBA data retrieval, return should_handle=false and queries=[]. "
                f"{conversation_context + chr(10) if conversation_context else ''}"
                f"Current user request: {user_query}"
            ),
        }
    )
    return messages


def _extract_message_content(body: dict[str, Any]) -> str:
    choices = body.get("choices") or []
    if not choices:
        return ""
    message = ((choices[0] or {}).get("message") or {})
    content = message.get("content") or ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        text_parts: list[str] = []
        for item in content:
            if isinstance(item, dict) and item.get("type") == "text":
                text_parts.append(str(item.get("text") or ""))
        return "".join(text_parts)
    return ""


def _openai_plan_request(api_key: str, model: str, messages: list[dict[str, str]]) -> dict[str, Any]:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    base_payload: dict[str, Any] = {
        "model": model,
        "messages": messages,
    }

    attempts = [
        {
            "response_format": {
                "type": "json_schema",
                "json_schema": planner_envelope_json_schema(),
            }
        },
        {"response_format": {"type": "json_object"}},
        {},
    ]

    errors: list[str] = []

    for extra in attempts:
        payload = {**base_payload, **extra}
        response = requests.post(
            OPENAI_CHAT_COMPLETIONS_URL,
            headers=headers,
            json=payload,
            timeout=45,
        )

        response_text = response.text
        if not response.ok:
            errors.append(f"{response.status_code}: {response_text[:300]}")
            if extra and response.status_code == 400 and "response_format" in response_text.lower():
                continue
            raise RuntimeError(f"Planner request failed: {response.status_code}: {response_text[:300]}")

        try:
            body = response.json()
        except ValueError as exc:
            raise RuntimeError("Planner returned non-JSON response") from exc

        content = _extract_message_content(body)
        if not content.strip():
            errors.append("missing planner message content")
            continue

        parsed = _extract_json_object(content)
        return parsed

    raise RuntimeError(f"Planner could not parse response: {errors}")


def _normalize_planner_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload or {})
    should_handle = _bool_value(normalized.get("should_handle"))
    reason = str(normalized.get("reason") or "").strip()

    queries = normalized.get("queries")
    if not isinstance(queries, list):
        legacy_query = normalized.get("query")
        if isinstance(legacy_query, dict):
            queries = [legacy_query]
        else:
            queries = []

    normalized["should_handle"] = should_handle
    normalized["reason"] = reason
    normalized["queries"] = queries
    normalized.pop("query", None)
    return normalized


def plan_query(
    user_query: str,
    conversation_history: Optional[list[dict[str, Any]]] = None,
) -> QueryPlan:
    query_text = str(user_query or "").strip()
    if not query_text:
        return QueryPlan(matched=False, reason="empty_query")

    LOGGER.debug("structured_flow raw_query=%s", query_text)

    load_env_files()
    api_key = (
        os.getenv("NBA_QUERY_PLANNER_API_KEY")
        or os.getenv("OPENAI_API_KEY")
        or os.getenv("LEVISION_CHAT_API_KEY")
    )
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for NBA query planning")

    model = _planner_model()
    planner_raw = _openai_plan_request(
        api_key=api_key,
        model=model,
        messages=_planner_messages(
            query_text,
            conversation_history=conversation_history,
        ),
    )
    planner_payload = _normalize_planner_payload(planner_raw)
    print(f"[nba_planner_raw] {json.dumps(planner_raw, ensure_ascii=True, default=str)}")
    LOGGER.debug("structured_flow planner_raw=%s", json.dumps(planner_raw, ensure_ascii=True, default=str))
    LOGGER.debug(
        "structured_flow planner_payload=%s",
        json.dumps(planner_payload, ensure_ascii=True, default=str),
    )

    should_handle = bool(planner_payload.get("should_handle"))
    reason = str(planner_payload.get("reason") or "").strip() or None

    if not should_handle:
        LOGGER.debug("Planner declined query. reason=%s query=%s", reason, query_text)
        return QueryPlan(matched=False, reason=reason, raw=planner_payload)

    queries = coerce_structured_queries(planner_payload.get("queries"))
    LOGGER.debug(
        "structured_flow planner_queries=%s",
        json.dumps([query.to_dict() for query in queries], ensure_ascii=True, default=str),
    )
    return QueryPlan(matched=True, queries=queries, reason=reason, raw=planner_payload)
