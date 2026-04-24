from __future__ import annotations

from dataclasses import asdict, dataclass, field
import logging
from datetime import datetime, timedelta
from typing import Any, Optional
from zoneinfo import ZoneInfo

from .data_service import (
    DataService,
    EntityAmbiguityError,
    EntityNotFoundError,
)
from .query_schema import ALLOWED_OPERATIONS, QueryScope, StructuredQuery
from .settings import Settings

LOGGER = logging.getLogger("nba_pipeline.query_executor")
LOCAL_TZ = ZoneInfo("America/New_York")


@dataclass(slots=True)
class ExecutionContext:
    last_game_event_id: Optional[str] = None
    last_game: Optional[dict[str, Any]] = None
    results: list[dict[str, Any]] = field(default_factory=list)


def _parse_local_datetime(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None

    candidate = text
    if candidate.endswith("Z"):
        candidate = f"{candidate[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=LOCAL_TZ)
    return parsed.astimezone(LOCAL_TZ)


def _relative_date_to_local_date(relative_date: Any) -> Optional[str]:
    text = str(relative_date or "").strip().lower().replace("-", "_").replace(" ", "_")
    if not text:
        return None

    today = datetime.now(LOCAL_TZ).date()
    if text in {"today", "tonight"}:
        return today.isoformat()
    if text in {"yesterday", "last_night"}:
        return (today - timedelta(days=1)).isoformat()
    return None


def _to_numeric(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value or "").strip()
    if not text:
        return None

    text = text.replace(" ", "")
    if text.startswith("+"):
        text = text[1:]
    try:
        return float(text)
    except ValueError:
        return None


def _sort_games_desc(games: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def _key(row: dict[str, Any]) -> datetime:
        parsed = _parse_local_datetime(row.get("game_datetime_local"))
        if parsed is not None:
            return parsed

        date_text = str(row.get("date") or "").strip()
        if date_text:
            parsed_date = _parse_local_datetime(f"{date_text}T00:00:00-05:00")
            if parsed_date is not None:
                return parsed_date

        return datetime(1970, 1, 1, tzinfo=LOCAL_TZ)

    return sorted(games, key=_key, reverse=True)


def _extract_games(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if isinstance(payload.get("games"), list):
        return [row for row in payload["games"] if isinstance(row, dict)]

    result = payload.get("result")
    if isinstance(result, dict):
        return [result]

    return []


def _aggregate_games(
    operation: str,
    games: list[dict[str, Any]],
) -> dict[str, Any]:
    numeric_rows: list[tuple[dict[str, Any], float]] = []
    for row in games:
        numeric_value = _to_numeric(row.get("stat_value", row.get("value")))
        if numeric_value is None:
            continue
        numeric_rows.append((row, numeric_value))

    if not numeric_rows:
        return {
            "status": "no_data",
            "message": "No numeric stat values were available for the requested scope.",
            "games": games,
        }

    if operation == "sum":
        total = sum(item[1] for item in numeric_rows)
        return {
            "status": "ok",
            "operation": operation,
            "value": total,
            "sample_size": len(numeric_rows),
            "games": [item[0] for item in numeric_rows],
        }

    if operation == "average":
        average = sum(item[1] for item in numeric_rows) / len(numeric_rows)
        return {
            "status": "ok",
            "operation": operation,
            "value": average,
            "sample_size": len(numeric_rows),
            "games": [item[0] for item in numeric_rows],
        }

    if operation == "max_single_game":
        best_row, best_value = max(numeric_rows, key=lambda item: item[1])
        return {
            "status": "ok",
            "operation": operation,
            "value": best_value,
            "sample_size": len(numeric_rows),
            "game": best_row,
        }

    if operation == "min_single_game":
        best_row, best_value = min(numeric_rows, key=lambda item: item[1])
        return {
            "status": "ok",
            "operation": operation,
            "value": best_value,
            "sample_size": len(numeric_rows),
            "game": best_row,
        }

    raise ValueError(f"Unsupported aggregate operation '{operation}'")


def _query_subject(query: StructuredQuery) -> Optional[str]:
    if query.entity_type == "player":
        return query.player or query.entity_hint
    if query.entity_type == "team":
        return query.team or query.entity_hint
    return query.entity_hint or query.team


def _scope_target_date(scope: QueryScope) -> Optional[str]:
    if scope.date:
        return scope.date
    return _relative_date_to_local_date(scope.relative_date)


def _extract_team_display_name(team: Any) -> Optional[str]:
    if not isinstance(team, dict):
        return None
    display = str(team.get("display_name") or "").strip()
    if display:
        return display
    abbreviation = str(team.get("abbreviation") or "").strip()
    if abbreviation:
        return abbreviation
    location = str(team.get("location") or "").strip()
    name = str(team.get("name") or "").strip()
    combined = " ".join(part for part in [location, name] if part).strip()
    return combined or None


def _extract_event_id_from_result(payload: dict[str, Any]) -> Optional[str]:
    direct = str(payload.get("resolved_event_id") or "").strip()
    if direct:
        return direct

    game = payload.get("game")
    if isinstance(game, dict):
        event_id = str(game.get("event_id") or game.get("game_id") or "").strip()
        if event_id:
            return event_id

    result = payload.get("result")
    if isinstance(result, dict):
        event_id = str(result.get("event_id") or result.get("game_id") or "").strip()
        if event_id:
            return event_id
        nested_game = result.get("game")
        if isinstance(nested_game, dict):
            event_id = str(nested_game.get("event_id") or nested_game.get("game_id") or "").strip()
            if event_id:
                return event_id

    aggregate = payload.get("aggregate")
    if isinstance(aggregate, dict):
        aggregate_game = aggregate.get("game")
        if isinstance(aggregate_game, dict):
            event_id = str(aggregate_game.get("event_id") or aggregate_game.get("game_id") or "").strip()
            if event_id:
                return event_id

    games = payload.get("games")
    if isinstance(games, list) and len(games) == 1 and isinstance(games[0], dict):
        event_id = str(games[0].get("event_id") or games[0].get("game_id") or "").strip()
        if event_id:
            return event_id

    return None


def _extract_game_from_result(payload: dict[str, Any]) -> Optional[dict[str, Any]]:
    game = payload.get("game")
    if isinstance(game, dict):
        return game
    result = payload.get("result")
    if isinstance(result, dict):
        nested_game = result.get("game")
        if isinstance(nested_game, dict):
            return nested_game
        if result.get("event_id") or result.get("game_id"):
            return result
    aggregate = payload.get("aggregate")
    if isinstance(aggregate, dict) and isinstance(aggregate.get("game"), dict):
        return aggregate.get("game")
    return None


def _resolve_game_for_query(
    service: DataService,
    query: StructuredQuery,
    context: ExecutionContext,
    *,
    subquery_index: Optional[int] = None,
) -> dict[str, Any]:
    scope = query.scope
    context_event_id = context.last_game_event_id
    target_date = _scope_target_date(scope)

    if scope.game_id:
        resolved = service.resolve_game_by_hints(event_id=scope.game_id)
        LOGGER.debug(
            "executor subquery_index=%s resolved_game event_id=%s source=%s mode=explicit_game_id",
            subquery_index,
            resolved.get("event_id"),
            resolved.get("source"),
        )
        return resolved

    if (
        scope.type == "game_hint"
        and context_event_id
        and not any([scope.teams, scope.opponent, scope.date, scope.relative_date, query.team])
    ):
        resolved = service.resolve_game_by_hints(event_id=context_event_id)
        LOGGER.debug(
            "executor subquery_index=%s resolved_game event_id=%s source=%s mode=context_last_game",
            subquery_index,
            resolved.get("event_id"),
            resolved.get("source"),
        )
        return resolved

    team_query = query.team or (query.entity_hint if query.entity_type == "team" else None)
    opponent_query = scope.opponent
    teams = list(scope.teams or [])

    if query.entity_type == "player":
        player_query = query.player or query.entity_hint
        if not player_query:
            raise ValueError("player stat query requires player or entity_hint")
        resolved_player = service.resolve_player_and_team(player_query)
        if not team_query:
            team_query = str(resolved_player.get("team_abbreviation") or "").strip() or None
            team_info = resolved_player.get("team")
            if not team_query and isinstance(team_info, dict):
                team_query = _extract_team_display_name(team_info)
        if not team_query:
            raise ValueError(f"Could not resolve a current team for player '{player_query}'")

    if team_query and not teams:
        teams = [team_query]
    if opponent_query and teams and all(opponent_query.lower() != item.lower() for item in teams):
        teams.append(opponent_query)

    resolved = service.resolve_game_by_hints(
        team_query=team_query,
        opponent_query=opponent_query,
        teams=teams or None,
        target_date=target_date,
        event_id=None,
        before_now=scope.before_now if scope.before_now is not None else True,
    )
    LOGGER.debug(
        "executor subquery_index=%s resolved_game event_id=%s source=%s scope_type=%s target_date=%s team=%s opponent=%s teams=%s",
        subquery_index,
        resolved.get("event_id"),
        resolved.get("source"),
        scope.type,
        target_date,
        team_query,
        opponent_query,
        teams or None,
    )
    return resolved


def _execute_player_or_team_event_stat(
    service: DataService,
    query: StructuredQuery,
    event_id: str,
) -> dict[str, Any]:
    subject = _query_subject(query)
    if not subject:
        raise ValueError(f"{query.entity_type} stat query requires a subject hint")

    if query.entity_type == "player":
        return service.get_player_game_stat_by_event_id(
            player_query=subject,
            stat_name=query.stat or "",
            event_id=event_id,
        )
    return service.get_team_game_stat_by_event_id(
        team_query=subject,
        stat_name=query.stat or "",
        event_id=event_id,
    )


def _execute_game_stat_query(
    service: DataService,
    query: StructuredQuery,
    context: ExecutionContext,
    *,
    subquery_index: Optional[int] = None,
) -> dict[str, Any]:
    if query.operation not in {"max_single_game", "min_single_game"}:
        return {
            "status": "error",
            "error_type": "unsupported_operation",
            "message": f"Unsupported operation '{query.operation}' for game stat_query",
            "query": query.to_dict(),
        }

    requested_rank = query.rank or 1
    resolved_game = _resolve_game_for_query(
        service,
        query,
        context,
        subquery_index=subquery_index,
    )
    event_id = str(resolved_game.get("event_id") or "").strip()
    if not event_id:
        return {
            "status": "error",
            "error_type": "resolution_failed",
            "message": "Could not resolve an event_id for the requested game scope",
            "query": query.to_dict(),
        }

    leader_payload = service.get_game_stat_leader(
        event_id=event_id,
        stat_name=query.stat or "",
        direction="max" if query.operation == "max_single_game" else "min",
        leader_entity_type="player",
        rank=requested_rank,
    )
    leader = leader_payload.get("leader")
    if not isinstance(leader, dict):
        available_count = int(leader_payload.get("available_count") or 0)
        if available_count and requested_rank > available_count:
            message = (
                f"Only {available_count} player stat rows were available for that game, "
                f"so rank {requested_rank} is out of range."
            )
        else:
            message = "No player stat values were available for that game."
        return {
            "status": "no_data",
            "result_type": "stat_query",
            "query": query.to_dict(),
            "operation": query.operation,
            "stat": leader_payload.get("stat"),
            "scope": asdict(query.scope),
            "resolved_event_id": event_id,
            "game": leader_payload.get("game"),
            "rank": leader_payload.get("requested_rank") or requested_rank,
            "message": message,
            "sources": leader_payload.get("sources") or [],
        }

    return {
        "status": "ok",
        "result_type": "stat_query",
        "query": query.to_dict(),
        "operation": query.operation,
        "stat": leader_payload.get("stat"),
        "scope": asdict(query.scope),
        "resolved_event_id": event_id,
        "game": leader_payload.get("game"),
        "rank": leader_payload.get("requested_rank") or requested_rank,
        "result": {
            "game": leader_payload.get("game"),
            "leader": leader,
        },
        "sources": leader_payload.get("sources") or [],
    }


def _execute_stat_query(
    service: DataService,
    query: StructuredQuery,
    context: ExecutionContext,
    *,
    subquery_index: Optional[int] = None,
) -> dict[str, Any]:
    if not query.stat:
        return {
            "status": "error",
            "error_type": "validation",
            "message": "stat is required for stat_query",
            "query": query.to_dict(),
        }

    if query.entity_type == "game":
        return _execute_game_stat_query(
            service,
            query,
            context,
            subquery_index=subquery_index,
        )

    scope = query.scope
    subject = _query_subject(query)
    if not subject:
        return {
            "status": "error",
            "error_type": "validation",
            "message": f"{query.entity_type} stat_query requires a subject hint",
            "query": query.to_dict(),
        }

    resolved_game = None
    if query.entity_type == "player":
        if scope.type == "recent_games":
            payload = service.get_player_last_n_games_stat(
                player_query=subject,
                stat_name=query.stat,
                n=scope.count or 1,
            )
        elif scope.type == "date":
            payload = service.get_player_game_stat_by_date(
                player_query=subject,
                stat_name=query.stat,
                target_date=scope.date or "",
            )
        elif scope.type == "date_range":
            payload = service.get_player_stat_log_for_date_range(
                player_query=subject,
                stat_name=query.stat,
                start_date=scope.start_date or "",
                end_date=scope.end_date or "",
                before_now=scope.before_now if scope.before_now is not None else True,
            )
        elif scope.type == "season":
            payload = service.get_player_season_stat_log(
                player_query=subject,
                stat_name=query.stat,
                season=scope.season or "current",
            )
        elif scope.type in {"specific_game", "relative_date", "matchup_hint", "game_hint"}:
            resolved_game = _resolve_game_for_query(
                service,
                query,
                context,
                subquery_index=subquery_index,
            )
            payload = _execute_player_or_team_event_stat(
                service,
                query,
                str(resolved_game.get("event_id") or ""),
            )
        else:
            return {
                "status": "error",
                "error_type": "unsupported_scope",
                "message": f"Unsupported scope.type '{scope.type}' for player stat query",
                "query": query.to_dict(),
            }
    elif query.entity_type == "team":
        if scope.type == "recent_games":
            payload = service.get_team_last_n_games_stat(
                team_query=subject,
                stat_name=query.stat,
                n=scope.count or 1,
            )
        elif scope.type == "date":
            payload = service.get_team_game_stat_by_date(
                team_query=subject,
                stat_name=query.stat,
                target_date=scope.date or "",
            )
        elif scope.type == "date_range":
            payload = service.get_team_stat_log_for_date_range(
                team_query=subject,
                stat_name=query.stat,
                start_date=scope.start_date or "",
                end_date=scope.end_date or "",
                before_now=scope.before_now if scope.before_now is not None else True,
            )
        elif scope.type == "season":
            payload = service.get_team_season_stat_log(
                team_query=subject,
                stat_name=query.stat,
                season=scope.season or "current",
            )
        elif scope.type in {"specific_game", "relative_date", "matchup_hint", "game_hint"}:
            resolved_game = _resolve_game_for_query(
                service,
                query,
                context,
                subquery_index=subquery_index,
            )
            payload = _execute_player_or_team_event_stat(
                service,
                query,
                str(resolved_game.get("event_id") or ""),
            )
        else:
            return {
                "status": "error",
                "error_type": "unsupported_scope",
                "message": f"Unsupported scope.type '{scope.type}' for team stat query",
                "query": query.to_dict(),
            }
    else:
        return {
            "status": "error",
            "error_type": "validation",
            "message": f"Unsupported entity_type '{query.entity_type}' for stat_query",
            "query": query.to_dict(),
        }

    games = _sort_games_desc(_extract_games(payload))
    count = scope.count if scope.type == "recent_games" else None
    if count is not None:
        games = games[:count]

    resolved_event_id = str(resolved_game.get("event_id") or "").strip() if isinstance(resolved_game, dict) else None
    base_response: dict[str, Any] = {
        "status": "ok",
        "result_type": "stat_query",
        "query": query.to_dict(),
        "player": payload.get("player"),
        "team": payload.get("team"),
        "stat": payload.get("stat"),
        "scope": payload.get("scope") or asdict(query.scope),
        "sources": payload.get("sources") or [],
        "resolved_event_id": resolved_event_id,
        "game": resolved_game.get("game") if isinstance(resolved_game, dict) else None,
    }

    if query.operation == "game_log":
        base_response["operation"] = "game_log"
        base_response["games"] = games
        base_response["returned_games"] = len(games)
        if not games:
            base_response["status"] = "no_data"
            base_response["message"] = "No completed games matched the requested scope."
        return base_response

    if query.operation == "latest_game":
        latest = games[0] if games else None
        base_response["operation"] = "latest_game"
        base_response["game"] = latest or base_response.get("game")
        if latest is None:
            base_response["status"] = "no_data"
            base_response["message"] = "No completed games matched the requested scope."
        return base_response

    if query.operation in {"sum", "average", "max_single_game", "min_single_game"}:
        aggregate = _aggregate_games(query.operation, games)
        if aggregate.get("status") != "ok":
            return {
                **base_response,
                "status": "no_data",
                "operation": query.operation,
                "games": games,
                "message": aggregate.get("message")
                or "No numeric stat values were available for the requested scope.",
            }

        return {
            **base_response,
            "operation": query.operation,
            "aggregate": aggregate,
            "games": games,
        }

    return {
        "status": "error",
        "error_type": "unsupported_operation",
        "message": f"Unsupported operation '{query.operation}' for stat query",
        "query": query.to_dict(),
    }


def _execute_play_by_play_query(
    service: DataService,
    query: StructuredQuery,
    context: ExecutionContext,
    *,
    subquery_index: Optional[int] = None,
) -> dict[str, Any]:
    resolved_game = _resolve_game_for_query(
        service,
        query,
        context,
        subquery_index=subquery_index,
    )
    event_id = str(resolved_game.get("event_id") or "").strip()
    if not event_id:
        return {
            "status": "error",
            "error_type": "resolution_failed",
            "message": "Could not resolve a game for play-by-play",
            "query": query.to_dict(),
        }

    payload = service.get_game_play_by_play(event_id=event_id)
    return {
        "status": "ok",
        "result_type": "play_by_play_query",
        "query": query.to_dict(),
        "operation": "play_by_play",
        "resolved_event_id": event_id,
        "game": resolved_game.get("game"),
        "result": payload,
    }


def _execute_game_lookup_query(
    service: DataService,
    query: StructuredQuery,
    context: ExecutionContext,
    *,
    subquery_index: Optional[int] = None,
) -> dict[str, Any]:
    scope = query.scope

    if query.entity_type == "team" and scope.type == "recent_games":
        subject = _query_subject(query)
        if not subject:
            return {
                "status": "error",
                "error_type": "validation",
                "message": "team is required for team game_lookup",
                "query": query.to_dict(),
            }
        payload = service.get_team_recent_games(team_query=subject, n=scope.count or 5)
        return {
            "status": "ok",
            "result_type": "game_lookup",
            "query": query.to_dict(),
            "operation": "game_log",
            "result": payload,
        }

    if query.entity_type == "game":
        resolved_game = _resolve_game_for_query(
            service,
            query,
            context,
            subquery_index=subquery_index,
        )
        event_id = str(resolved_game.get("event_id") or "").strip()
        return {
            "status": "ok",
            "result_type": "game_lookup",
            "query": query.to_dict(),
            "operation": "single_game_lookup",
            "resolved_event_id": event_id,
            "result": resolved_game.get("game"),
            "game": resolved_game.get("game"),
            "sources": [resolved_game.get("source")] if resolved_game.get("source") else [],
        }

    return {
        "status": "error",
        "error_type": "unsupported_query",
        "message": "Unsupported game_lookup query shape",
        "query": query.to_dict(),
    }


def _execute_query_with_service(
    service: DataService,
    query: StructuredQuery,
    context: ExecutionContext,
    *,
    subquery_index: Optional[int] = None,
) -> dict[str, Any]:
    LOGGER.debug(
        "executor subquery_index=%s route intent=%s operation=%s scope=%s",
        subquery_index,
        query.intent,
        query.operation,
        query.scope.type,
    )

    if query.intent == "stat_query":
        return _execute_stat_query(
            service,
            query,
            context,
            subquery_index=subquery_index,
        )
    if query.intent == "play_by_play_query":
        return _execute_play_by_play_query(
            service,
            query,
            context,
            subquery_index=subquery_index,
        )
    if query.intent == "game_lookup":
        return _execute_game_lookup_query(
            service,
            query,
            context,
            subquery_index=subquery_index,
        )

    return {
        "status": "error",
        "error_type": "unsupported_intent",
        "message": f"Unsupported intent '{query.intent}'",
        "query": query.to_dict(),
    }


def execute_structured_query(
    query: StructuredQuery,
    settings: Optional[Settings] = None,
    *,
    service: Optional[DataService] = None,
    execution_context: Optional[ExecutionContext] = None,
    subquery_index: Optional[int] = None,
) -> dict[str, Any]:
    operation = str(getattr(query, "operation", "") or "").strip()
    if not operation:
        return {
            "status": "error",
            "error_type": "validation",
            "message": "Structured query validation failed: operation is required",
            "query": query.to_dict(),
        }
    if operation not in ALLOWED_OPERATIONS:
        return {
            "status": "error",
            "error_type": "validation",
            "message": f"Structured query validation failed: unsupported operation '{operation}'",
            "query": query.to_dict(),
        }

    shared_service = service or DataService(settings=settings)
    shared_context = execution_context or ExecutionContext()

    try:
        payload = _execute_query_with_service(
            shared_service,
            query,
            shared_context,
            subquery_index=subquery_index,
        )
    except EntityAmbiguityError as exc:
        LOGGER.info(
            "Entity ambiguity for query. type=%s query=%s candidates=%s",
            exc.entity_type,
            exc.query,
            exc.candidates,
        )
        payload = {
            "status": "clarification",
            "error_type": "entity_ambiguity",
            "query": query.to_dict(),
            "entity_type": exc.entity_type,
            "query_text": exc.query,
            "candidates": exc.candidates,
            "message": (
                f"I found multiple {exc.entity_type} matches for '{exc.query}'. "
                "Please clarify which one you mean."
            ),
        }
    except EntityNotFoundError as exc:
        payload = {
            "status": "error",
            "error_type": "entity_not_found",
            "query": query.to_dict(),
            "entity_type": exc.entity_type,
            "query_text": exc.query,
            "message": str(exc),
        }
    except Exception as exc:
        LOGGER.exception("Structured query execution failed")
        payload = {
            "status": "error",
            "error_type": "execution_failed",
            "query": query.to_dict(),
            "message": str(exc),
        }

    if subquery_index is not None:
        payload["subquery_index"] = subquery_index
    return payload


def _combined_status(results: list[dict[str, Any]]) -> str:
    statuses = [str(item.get("status") or "") for item in results]
    if not statuses:
        return "error"
    if any(status == "clarification" for status in statuses):
        return "clarification"
    if all(status in {"ok", "no_data"} for status in statuses):
        return "ok" if any(status == "ok" for status in statuses) else "no_data"
    if any(status == "error" for status in statuses) and any(status in {"ok", "no_data"} for status in statuses):
        return "partial"
    if all(status == "error" for status in statuses):
        return "error"
    return "partial"


def execute_structured_queries(
    queries: list[StructuredQuery],
    settings: Optional[Settings] = None,
) -> dict[str, Any]:
    if not queries:
        return {
            "status": "error",
            "error_type": "validation",
            "message": "Structured query list is empty",
            "results": [],
        }

    service = DataService(settings=settings)
    context = ExecutionContext()
    results: list[dict[str, Any]] = []

    for index, query in enumerate(queries):
        LOGGER.debug(
            "structured_flow executing_subquery index=%s query=%s",
            index,
            query.to_dict(),
        )
        result = execute_structured_query(
            query,
            settings=settings,
            service=service,
            execution_context=context,
            subquery_index=index,
        )
        results.append(result)
        context.results.append(result)

        resolved_event_id = _extract_event_id_from_result(result)
        if resolved_event_id:
            context.last_game_event_id = resolved_event_id
            context.last_game = _extract_game_from_result(result)
            LOGGER.debug(
                "structured_flow subquery_index=%s context_last_game_event_id=%s",
                index,
                resolved_event_id,
            )

    return {
        "status": _combined_status(results),
        "result_type": "multi_query",
        "results": results,
        "query_count": len(queries),
        "resolved_context": {
            "last_game_event_id": context.last_game_event_id,
        },
    }
