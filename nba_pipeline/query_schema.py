from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import re
from typing import Any, Literal, Optional

Intent = Literal["stat_query", "play_by_play_query", "game_lookup"]
EntityType = Literal["player", "team", "game"]
Operation = Literal[
    "game_log",
    "max_single_game",
    "min_single_game",
    "average",
    "sum",
    "latest_game",
    "single_game_lookup",
    "play_by_play",
]
ScopeType = Literal[
    "recent_games",
    "season",
    "date",
    "date_range",
    "relative_date",
    "matchup_hint",
    "specific_game",
    "game_hint",
]
RelativeDateValue = Literal["today", "yesterday", "tonight", "last_night"]
StatName = str


ALLOWED_INTENTS = {"stat_query", "play_by_play_query", "game_lookup"}
ALLOWED_ENTITY_TYPES = {"player", "team", "game"}
ALLOWED_OPERATIONS = {
    "game_log",
    "max_single_game",
    "min_single_game",
    "average",
    "sum",
    "latest_game",
    "single_game_lookup",
    "play_by_play",
}
ALLOWED_SCOPE_TYPES = {
    "recent_games",
    "season",
    "date",
    "date_range",
    "relative_date",
    "matchup_hint",
    "specific_game",
    "game_hint",
}
ALLOWED_RELATIVE_DATE_VALUES = {"today", "yesterday", "tonight", "last_night"}
ALLOWED_STATS = {
    "points",
    "assists",
    "rebounds",
    "steals",
    "blocks",
    "turnovers",
    "minutes",
    "fouls",
    "plus_minus",
    "starter",
    "did_not_play",
    "reason",
    "ejected",
    "offensive_rebounds",
    "defensive_rebounds",
    "fg_made",
    "fg_attempted",
    "three_ptr_made",
    "three_ptr_attempted",
    "ft_made",
    "ft_attempted",
    "season",
    "date",
    "status",
    "venue",
    "home_team",
    "away_team",
    "home_points",
    "away_points",
    "field_goal_percentage",
    "three_point_fg_made",
    "three_point_fg_attempted",
    "three_point_fg_percentage",
    "free_throws_made",
    "free_throws_attempted",
    "free_throw_percentage",
    "total_rebounds",
    "team_turnovers",
    "total_turnovers",
    "technical_fouls",
    "total_technical_fouls",
    "flagrant_fouls",
    "turnover_points",
    "fast_break_points",
    "points_in_paint",
    "largest_lead",
}

STAT_SYNONYMS: dict[str, str] = {
    "point": "points",
    "pts": "points",
    "scoring": "points",
    "score": "points",
    "assist": "assists",
    "ast": "assists",
    "rebound": "rebounds",
    "reb": "rebounds",
    "stl": "steals",
    "blk": "blocks",
    "turnover": "turnovers",
    "to": "turnovers",
    "minute": "minutes",
    "min": "minutes",
    "mins": "minutes",
    "pf": "fouls",
    "dnp": "did_not_play",
    "off_rebounds": "offensive_rebounds",
    "offensive_rebound": "offensive_rebounds",
    "def_rebounds": "defensive_rebounds",
    "defensive_rebound": "defensive_rebounds",
    "plusminus": "plus_minus",
    "plus_minus": "plus_minus",
    "+/-": "plus_minus",
    "fgm": "fg_made",
    "fga": "fg_attempted",
    "fgmade": "fg_made",
    "fgattempted": "fg_attempted",
    "3pm": "three_ptr_made",
    "3pa": "three_ptr_attempted",
    "3ptm": "three_ptr_made",
    "3pta": "three_ptr_attempted",
    "3_pointer": "three_ptr_made",
    "3_pointers": "three_ptr_made",
    "three_pointer": "three_ptr_made",
    "three_pointers": "three_ptr_made",
    "three_point": "three_ptr_made",
    "three_points": "three_ptr_made",
    "three_point_made": "three_ptr_made",
    "three_point_attempted": "three_ptr_attempted",
    "three_pointers_made": "three_ptr_made",
    "three_pointers_attempted": "three_ptr_attempted",
    "threes_made": "three_ptr_made",
    "threes_attempted": "three_ptr_attempted",
    "ftm": "ft_made",
    "fta": "ft_attempted",
}

OPERATION_SYNONYMS: dict[str, str] = {
    "highest": "max_single_game",
    "max": "max_single_game",
    "season_high": "max_single_game",
    "lowest": "min_single_game",
    "min": "min_single_game",
    "mean": "average",
    "total": "sum",
    "latest": "latest_game",
    "single_game": "single_game_lookup",
}

RELATIVE_DATE_SYNONYMS: dict[str, str] = {
    "last night": "last_night",
    "last_night": "last_night",
    "yesterday": "yesterday",
    "today": "today",
    "tonight": "tonight",
}

QUERY_ALLOWED_KEYS = {
    "intent",
    "entity_type",
    "player",
    "team",
    "entity_hint",
    "stat",
    "rank",
    "operation",
    "scope",
}
SCOPE_ALLOWED_KEYS = {
    "type",
    "count",
    "season",
    "date",
    "start_date",
    "end_date",
    "relative_date",
    "before_now",
    "game_id",
    "opponent",
    "teams",
}


@dataclass(slots=True)
class QueryScope:
    type: ScopeType
    count: Optional[int] = None
    season: Optional[str] = None
    date: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    relative_date: Optional[RelativeDateValue] = None
    before_now: Optional[bool] = None
    game_id: Optional[str] = None
    opponent: Optional[str] = None
    teams: Optional[list[str]] = None


@dataclass(slots=True)
class StructuredQuery:
    intent: Intent
    entity_type: EntityType
    player: Optional[str]
    team: Optional[str]
    entity_hint: Optional[str]
    stat: Optional[StatName]
    rank: Optional[int]
    operation: Operation
    scope: QueryScope

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_hint_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("\u2019", "'").replace("\u2018", "'")
    text = re.sub(r"\s+", " ", text)
    return text.strip(" ,.!?\"'") or None


def _normalize_entity_name(value: Any) -> Optional[str]:
    text = _normalize_hint_text(value)
    if not text:
        return None
    text = re.sub(r"^(?:the)\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"['’]s\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" ,.!?\"'") or None


def _normalize_stat(stat_value: Any) -> Optional[str]:
    if stat_value is None:
        return None
    text = _normalize_text(stat_value)
    if not text:
        return None
    text = re.sub(r"[\s\-]+", "_", text)
    text = STAT_SYNONYMS.get(text, text)
    return text


def _is_valid_stat_name(stat_value: str) -> bool:
    return bool(re.fullmatch(r"[a-z0-9_]{1,64}", stat_value))


def _normalize_operation(operation_value: Any) -> str:
    text = _normalize_text(operation_value)
    text = text.replace(" ", "_")
    return OPERATION_SYNONYMS.get(text, text)


def _normalize_scope_type(scope_type: Any) -> str:
    text = _normalize_text(scope_type)
    text = text.replace(" ", "_")
    return text


def _normalize_relative_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = _normalize_text(value)
    if not text:
        return None
    text = text.replace("-", "_")
    text = text.replace(" ", "_")
    if text == "last_night":
        return "last_night"
    if text in RELATIVE_DATE_SYNONYMS:
        return RELATIVE_DATE_SYNONYMS[text]
    text = text.replace("_", " ")
    return RELATIVE_DATE_SYNONYMS.get(text)


def _require_date(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"scope.{field} is required")
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"scope.{field} must be YYYY-MM-DD") from exc
    return text


def _to_optional_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = _normalize_text(value)
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def _to_optional_int(value: Any, *, field_name: str) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = int(text)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer") from exc
    if parsed <= 0:
        raise ValueError(f"{field_name} must be > 0")
    return parsed


def _normalize_string_list(values: Any) -> Optional[list[str]]:
    if values is None:
        return None
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, list):
        return None

    normalized: list[str] = []
    seen: set[str] = set()
    for item in values:
        candidate = _normalize_entity_name(item)
        if not candidate:
            continue
        key = candidate.lower()
        if key in seen:
            continue
        seen.add(key)
        normalized.append(candidate)

    return normalized or None


def _reject_extra_keys(data: dict[str, Any], allowed: set[str], label: str) -> None:
    extras = sorted(key for key in data.keys() if key not in allowed)
    if extras:
        raise ValueError(f"{label} contains unsupported keys: {', '.join(extras)}")


def _coerce_scope_input(data: dict[str, Any]) -> dict[str, Any]:
    scope_input = data.get("scope")
    scope = scope_input if isinstance(scope_input, dict) else {}
    filters = data.get("filters")
    if isinstance(filters, dict):
        if scope.get("opponent") is None and filters.get("opponent") is not None:
            scope = {**scope, "opponent": filters.get("opponent")}
        if scope.get("teams") is None and filters.get("teams") is not None:
            scope = {**scope, "teams": filters.get("teams")}
    if scope.get("start_date") is None and scope.get("start") is not None:
        scope = {**scope, "start_date": scope.get("start")}
    if scope.get("end_date") is None and scope.get("end") is not None:
        scope = {**scope, "end_date": scope.get("end")}
    return scope


def _validate_subject(query: StructuredQuery) -> None:
    if query.intent == "stat_query" and query.entity_type == "player":
        if not query.player and not query.entity_hint:
            raise ValueError("player stat_query requires player or entity_hint")
    if query.intent == "stat_query" and query.entity_type == "team":
        if not query.team and not query.entity_hint:
            raise ValueError("team stat_query requires team or entity_hint")
    if query.intent == "game_lookup" and query.entity_type == "team" and query.scope.type == "recent_games":
        if not query.team and not query.entity_hint:
            raise ValueError("team game_lookup requires team or entity_hint")
    if query.intent == "play_by_play_query":
        has_team_hint = bool(query.team or query.entity_hint or query.scope.teams or query.scope.opponent)
        has_game_hint = bool(query.scope.game_id or query.scope.date or query.scope.relative_date)
        if not has_team_hint and not has_game_hint:
            raise ValueError("play_by_play_query requires a game or team hint")


def _validate_scope(query: StructuredQuery) -> QueryScope:
    scope = query.scope
    scope_type = _normalize_scope_type(scope.type)
    if scope_type not in ALLOWED_SCOPE_TYPES:
        raise ValueError(f"Unsupported scope.type '{scope_type}'")

    count = _to_optional_int(scope.count, field_name="scope.count")
    season = str(scope.season or "").strip() or None
    date = str(scope.date or "").strip() or None
    start_date = str(scope.start_date or "").strip() or None
    end_date = str(scope.end_date or "").strip() or None
    relative_date = _normalize_relative_date(scope.relative_date)
    before_now = _to_optional_bool(scope.before_now)
    game_id = str(scope.game_id or "").strip() or None
    opponent = _normalize_entity_name(scope.opponent)
    teams = _normalize_string_list(scope.teams)

    if scope_type == "recent_games":
        if count is None:
            raise ValueError("scope.count is required for recent_games")
        if before_now is None:
            before_now = True

    if scope_type == "season":
        season = season or "current"

    if scope_type == "date":
        date = _require_date(date, "date")

    if scope_type == "date_range":
        start_date = _require_date(start_date, "start_date")
        end_date = _require_date(end_date, "end_date")

    if scope_type == "relative_date":
        if not relative_date:
            raise ValueError("scope.relative_date is required for relative_date")
        if date:
            date = _require_date(date, "date")

    if scope_type == "specific_game" and not game_id:
        raise ValueError("scope.game_id is required for specific_game")

    if scope_type == "matchup_hint":
        if not opponent and not teams:
            raise ValueError("matchup_hint requires scope.opponent or scope.teams")
        if date:
            date = _require_date(date, "date")
        if relative_date and relative_date not in ALLOWED_RELATIVE_DATE_VALUES:
            raise ValueError(f"Unsupported scope.relative_date '{relative_date}'")

    if scope_type == "game_hint":
        if date:
            date = _require_date(date, "date")
        if not any([game_id, date, relative_date, opponent, teams, query.entity_hint]):
            raise ValueError("game_hint requires a contextual game reference")

    if relative_date and relative_date not in ALLOWED_RELATIVE_DATE_VALUES:
        raise ValueError(f"Unsupported scope.relative_date '{relative_date}'")

    return QueryScope(
        type=scope_type,  # type: ignore[arg-type]
        count=count,
        season=season,
        date=date,
        start_date=start_date,
        end_date=end_date,
        relative_date=relative_date,  # type: ignore[arg-type]
        before_now=before_now,
        game_id=game_id,
        opponent=opponent,
        teams=teams,
    )


def parse_structured_query(data: dict[str, Any]) -> StructuredQuery:
    if not isinstance(data, dict):
        raise ValueError("Planner output must be a JSON object")

    _reject_extra_keys(data, QUERY_ALLOWED_KEYS, "query")

    intent = _normalize_text(data.get("intent"))
    if intent not in ALLOWED_INTENTS:
        raise ValueError(f"Unsupported intent '{intent}'")

    entity_type = _normalize_text(data.get("entity_type"))
    if entity_type not in ALLOWED_ENTITY_TYPES:
        raise ValueError(f"Unsupported entity_type '{entity_type}'")

    player = _normalize_entity_name(data.get("player"))
    team = _normalize_entity_name(data.get("team"))
    entity_hint = _normalize_hint_text(data.get("entity_hint"))

    stat_raw = _normalize_stat(data.get("stat"))
    stat: Optional[str] = stat_raw
    if intent == "stat_query":
        if not stat:
            raise ValueError("stat is required for stat_query")
        if not _is_valid_stat_name(stat):
            raise ValueError(f"Unsupported stat '{stat_raw}'")
    else:
        stat = None

    operation = _normalize_operation(data.get("operation"))
    if operation not in ALLOWED_OPERATIONS:
        raise ValueError(f"Unsupported operation '{operation}'")

    rank = _to_optional_int(data.get("rank"), field_name="rank")
    if rank is not None and intent != "stat_query":
        raise ValueError("rank is only supported for stat_query")
    if rank is not None and operation not in {"max_single_game", "min_single_game"}:
        raise ValueError("rank requires max_single_game or min_single_game operation")

    scope_input = data.get("scope") or {}
    if not isinstance(scope_input, dict):
        raise ValueError("scope must be an object")
    _reject_extra_keys(scope_input, SCOPE_ALLOWED_KEYS, "scope")

    query = StructuredQuery(
        intent=intent,  # type: ignore[arg-type]
        entity_type=entity_type,  # type: ignore[arg-type]
        player=player,
        team=team,
        entity_hint=entity_hint,
        stat=stat,
        rank=rank,
        operation=operation,  # type: ignore[arg-type]
        scope=QueryScope(
            type=_normalize_scope_type(scope_input.get("type")) or "recent_games",  # type: ignore[arg-type]
            count=scope_input.get("count"),
            season=scope_input.get("season"),
            date=scope_input.get("date"),
            start_date=scope_input.get("start_date"),
            end_date=scope_input.get("end_date"),
            relative_date=scope_input.get("relative_date"),
            before_now=scope_input.get("before_now"),
            game_id=scope_input.get("game_id"),
            opponent=scope_input.get("opponent"),
            teams=scope_input.get("teams"),
        ),
    )

    query.scope = _validate_scope(query)

    if query.intent == "play_by_play_query":
        query.operation = "play_by_play"
        query.stat = None
    elif query.operation == "play_by_play":
        raise ValueError("play_by_play operation requires play_by_play_query intent")

    _validate_subject(query)
    return query


def coerce_structured_query(data: dict[str, Any]) -> StructuredQuery:
    if not isinstance(data, dict):
        data = {}

    scope_input = _coerce_scope_input(data)

    player_value = data.get("player")
    if player_value is None:
        player_value = data.get("name")
    if player_value is None:
        player_value = data.get("player_name")

    team_value = data.get("team")
    if team_value is None:
        team_value = data.get("team_name")

    entity_hint = data.get("entity_hint")
    if entity_hint is None and player_value is None and team_value is None:
        entity_hint = data.get("name") or data.get("subject")

    return StructuredQuery(
        intent=_normalize_text(data.get("intent")) or "stat_query",  # type: ignore[arg-type]
        entity_type=_normalize_text(data.get("entity_type")) or "player",  # type: ignore[arg-type]
        player=_normalize_entity_name(player_value),
        team=_normalize_entity_name(team_value),
        entity_hint=_normalize_hint_text(entity_hint),
        stat=_normalize_stat(data.get("stat")),  # type: ignore[arg-type]
        rank=_to_optional_int(data.get("rank"), field_name="rank"),
        operation=_normalize_operation(data.get("operation")) or "game_log",  # type: ignore[arg-type]
        scope=QueryScope(
            type=_normalize_scope_type(scope_input.get("type")) or "recent_games",  # type: ignore[arg-type]
            count=scope_input.get("count"),
            season=str(scope_input.get("season") or "").strip() or None,
            date=str(scope_input.get("date") or "").strip() or None,
            start_date=str(scope_input.get("start_date") or "").strip() or None,
            end_date=str(scope_input.get("end_date") or "").strip() or None,
            relative_date=_normalize_relative_date(scope_input.get("relative_date")),  # type: ignore[arg-type]
            before_now=_to_optional_bool(scope_input.get("before_now")),
            game_id=str(scope_input.get("game_id") or "").strip() or None,
            opponent=_normalize_entity_name(scope_input.get("opponent")),
            teams=_normalize_string_list(scope_input.get("teams")),
        ),
    )


def coerce_structured_queries(items: Any) -> list[StructuredQuery]:
    if not isinstance(items, list):
        return []
    return [coerce_structured_query(item) for item in items if isinstance(item, dict)]


def validate_structured_query(query: StructuredQuery) -> StructuredQuery:
    return parse_structured_query(query.to_dict())


def validate_structured_queries(queries: list[StructuredQuery]) -> list[StructuredQuery]:
    return [validate_structured_query(query) for query in queries]


def planner_envelope_json_schema() -> dict[str, Any]:
    nullable_string = {"type": ["string", "null"]}
    nullable_boolean = {"type": ["boolean", "null"]}
    nullable_integer = {"type": ["integer", "null"]}
    nullable_string_array = {
        "anyOf": [
            {"type": "array", "items": {"type": "string"}},
            {"type": "null"},
        ]
    }

    return {
        "name": "nba_query_plan",
        "strict": True,
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["should_handle", "reason", "queries"],
            "properties": {
                "should_handle": {"type": "boolean"},
                "reason": {"type": "string"},
                "queries": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": [
                            "intent",
                            "entity_type",
                            "player",
                            "team",
                            "entity_hint",
                            "stat",
                            "rank",
                            "operation",
                            "scope",
                        ],
                        "properties": {
                            "intent": {"type": "string", "enum": sorted(ALLOWED_INTENTS)},
                            "entity_type": {"type": "string", "enum": sorted(ALLOWED_ENTITY_TYPES)},
                            "player": nullable_string,
                            "team": nullable_string,
                            "entity_hint": nullable_string,
                            "stat": nullable_string,
                            "rank": nullable_integer,
                            "operation": {"type": "string", "enum": sorted(ALLOWED_OPERATIONS)},
                            "scope": {
                                "type": "object",
                                "additionalProperties": False,
                                "required": [
                                    "type",
                                    "count",
                                    "season",
                                    "date",
                                    "start_date",
                                    "end_date",
                                    "relative_date",
                                    "before_now",
                                    "game_id",
                                    "opponent",
                                    "teams",
                                ],
                                "properties": {
                                    "type": {"type": "string", "enum": sorted(ALLOWED_SCOPE_TYPES)},
                                    "count": nullable_integer,
                                    "season": nullable_string,
                                    "date": nullable_string,
                                    "start_date": nullable_string,
                                    "end_date": nullable_string,
                                    "relative_date": {
                                        "type": ["string", "null"],
                                        "enum": [*sorted(ALLOWED_RELATIVE_DATE_VALUES), None],
                                    },
                                    "before_now": nullable_boolean,
                                    "game_id": nullable_string,
                                    "opponent": nullable_string,
                                    "teams": nullable_string_array,
                                },
                            },
                        },
                    },
                },
            },
        },
    }
