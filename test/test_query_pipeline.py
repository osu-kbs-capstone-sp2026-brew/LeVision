from __future__ import annotations

import unittest
from datetime import datetime

from nba_pipeline.data_service import DataService
from nba_pipeline.query_executor import (
    ExecutionContext,
    _execute_game_stat_query,
    _resolve_game_for_query,
)
from nba_pipeline.query_planner import LOCAL_TZ, repair_structured_query
from nba_pipeline.query_schema import (
    QueryScope,
    StructuredQuery,
    parse_structured_query,
    validate_structured_query,
)


class FakeGameResolver:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def resolve_game_by_hints(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(kwargs)
        event_id = str(kwargs.get("event_id") or "resolved-event")
        return {
            "event_id": event_id,
            "game": {"event_id": event_id},
            "source": "supabase",
        }


class FakeRankedLeaderService(FakeGameResolver):
    def __init__(self) -> None:
        super().__init__()
        self.last_leader_kwargs: dict[str, object] | None = None

    def get_game_stat_leader(self, **kwargs: object) -> dict[str, object]:
        self.last_leader_kwargs = kwargs
        return {
            "leader": {
                "entity_type": "player",
                "name": "Darius Garland",
                "stat_value": 21,
                "rank": kwargs.get("rank", 1),
            },
            "game": {"event_id": kwargs.get("event_id")},
            "stat": {"field": "points", "label": "points"},
            "requested_rank": kwargs.get("rank", 1),
            "available_count": 10,
            "sources": ["supabase"],
        }


class QueryPipelineTests(unittest.TestCase):
    def test_repair_adds_relative_date_and_matchup_hints(self) -> None:
        query = StructuredQuery(
            intent="game_lookup",
            entity_type="game",
            player=None,
            team=None,
            entity_hint=None,
            stat=None,
            rank=None,
            operation="single_game_lookup",
            scope=QueryScope(type="matchup_hint"),
        )

        repaired = repair_structured_query(
            query,
            "who won last night between the spurs and the blazers",
            now_local=datetime(2026, 4, 22, 10, 0, 0, tzinfo=LOCAL_TZ),
        )

        self.assertEqual(repaired.scope.type, "matchup_hint")
        self.assertEqual(repaired.scope.relative_date, "last_night")
        self.assertEqual(repaired.scope.date, "2026-04-21")
        self.assertEqual(repaired.scope.teams, ["spurs", "blazers"])

    def test_validation_allows_game_hint_with_entity_hint(self) -> None:
        query = StructuredQuery(
            intent="stat_query",
            entity_type="game",
            player=None,
            team=None,
            entity_hint="that game",
            stat="points",
            rank=2,
            operation="max_single_game",
            scope=QueryScope(
                type="game_hint",
                before_now=True,
            ),
        )

        validated = validate_structured_query(query)
        self.assertEqual(validated.scope.type, "game_hint")
        self.assertEqual(validated.entity_hint, "that game")

    def test_parse_rejects_off_schema_fields(self) -> None:
        with self.assertRaises(ValueError):
            parse_structured_query(
                {
                    "intent": "game_lookup",
                    "entity_type": "game",
                    "player": None,
                    "team": None,
                    "entity_hint": None,
                    "stat": None,
                    "operation": "single_game_lookup",
                    "filters": {"opponent": "Rockets"},
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
            )

    def test_game_hint_uses_previous_resolved_game(self) -> None:
        query = StructuredQuery(
            intent="stat_query",
            entity_type="game",
            player=None,
            team=None,
            entity_hint="that game",
            stat="points",
            rank=None,
            operation="max_single_game",
            scope=QueryScope(
                type="game_hint",
                before_now=True,
            ),
        )
        context = ExecutionContext(last_game_event_id="401000123")
        resolver = FakeGameResolver()

        resolved = _resolve_game_for_query(resolver, query, context, subquery_index=1)

        self.assertEqual(resolved["event_id"], "401000123")
        self.assertEqual(len(resolver.calls), 1)
        self.assertEqual(resolver.calls[0]["event_id"], "401000123")

    def test_repair_promotes_second_most_followup_to_ranked_game_stat(self) -> None:
        query = StructuredQuery(
            intent="stat_query",
            entity_type="game",
            player=None,
            team=None,
            entity_hint="that game",
            stat="points",
            rank=None,
            operation="latest_game",
            scope=QueryScope(
                type="game_hint",
                before_now=True,
            ),
        )

        repaired = repair_structured_query(
            query,
            "who scored the second most points?",
            now_local=datetime(2026, 4, 22, 10, 0, 0, tzinfo=LOCAL_TZ),
        )

        self.assertEqual(repaired.operation, "max_single_game")
        self.assertEqual(repaired.rank, 2)
        self.assertEqual(repaired.entity_hint, "that game")

    def test_repair_converts_single_game_lookup_stat_query_to_latest_game(self) -> None:
        query = StructuredQuery(
            intent="stat_query",
            entity_type="team",
            player=None,
            team="Cavs",
            entity_hint=None,
            stat="points_in_paint",
            rank=None,
            operation="single_game_lookup",
            scope=QueryScope(
                type="matchup_hint",
                relative_date="last_night",
                before_now=True,
                opponent="Raptors",
            ),
        )

        repaired = repair_structured_query(
            query,
            "how many points in the paint did the cavs score against the raptors last night",
            now_local=datetime(2026, 4, 24, 10, 0, 0, tzinfo=LOCAL_TZ),
        )

        self.assertEqual(repaired.intent, "stat_query")
        self.assertEqual(repaired.operation, "latest_game")
        self.assertEqual(repaired.scope.type, "matchup_hint")

    def test_game_stat_query_forwards_rank_to_service(self) -> None:
        query = StructuredQuery(
            intent="stat_query",
            entity_type="game",
            player=None,
            team=None,
            entity_hint="that game",
            stat="points",
            rank=2,
            operation="max_single_game",
            scope=QueryScope(
                type="game_hint",
                before_now=True,
            ),
        )
        context = ExecutionContext(last_game_event_id="401000123")
        service = FakeRankedLeaderService()

        result = _execute_game_stat_query(service, query, context, subquery_index=1)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(service.last_leader_kwargs["rank"], 2)
        self.assertEqual(result["result"]["leader"]["rank"], 2)

    def test_team_points_uses_home_or_away_score_from_context(self) -> None:
        service = object.__new__(DataService)

        home_value = service._extract_stat_value(
            {
                "team_is_home": True,
                "home_points": 112,
                "away_points": 104,
            },
            "points",
        )
        away_value = service._extract_stat_value(
            {
                "team_is_home": False,
                "home_points": 112,
                "away_points": 104,
            },
            "points",
        )

        self.assertEqual(home_value, 112)
        self.assertEqual(away_value, 104)

    def test_resolve_team_supports_common_aliases(self) -> None:
        service = object.__new__(DataService)
        service._teams_cache = [
            {
                "id": "1",
                "location": "Cleveland",
                "name": "Cavaliers",
                "abbreviation": "CLE",
            },
            {
                "id": "2",
                "location": "Boston",
                "name": "Celtics",
                "abbreviation": "BOS",
            },
        ]
        service._players_cache = None

        resolved = service.resolve_team("cavs")

        self.assertEqual(resolved["id"], "1")
        self.assertEqual(resolved["abbreviation"], "CLE")
        self.assertEqual(resolved["name"], "Cavaliers")


if __name__ == "__main__":
    unittest.main()
