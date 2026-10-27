import json
from datetime import datetime

from gh_lakehouse import transforms as t


def ev(id_, type_="PushEvent", actor=1, repo=10, created="2024-01-01T10:15:00Z", size=2):
    d = {
        "id": id_,
        "type": type_,
        "actor": {"id": actor, "login": f"user{actor}"} if actor is not None else {"login": "ghost"},
        "repo": {"id": repo, "name": f"org/repo{repo}"},
        "payload": {"action": "created"} if size is None else {"size": size, "action": "created"},
        "public": True,
        "created_at": created,
    }
    return json.dumps(d)


def bronze(spark, lines):
    rows = [(l, "f.json.gz", datetime(2024, 1, 1, 12, 0, i)) for i, l in enumerate(lines)]
    return spark.createDataFrame(rows, ["raw", "_source_file", "_ingested_at"])


def test_parse_events_typed(spark):
    r = t.parse_events(bronze(spark, [ev("1", size=3)])).collect()[0]
    assert r.event_id == "1" and r.event_type == "PushEvent"
    assert r.actor_id == 1 and r.repo_name == "org/repo10"
    assert r.push_commit_count == 3
    assert r.created_at is not None


def test_bad_rows_go_to_quarantine(spark):
    lines = [ev("1"), "{not json", ev("2", actor=None)]
    valid, invalid = t.split_valid_invalid(t.parse_events(bronze(spark, lines)))
    assert valid.count() == 1
    assert invalid.count() == 2
    assert all(r.dq_reason.startswith("missing:") for r in invalid.collect())


def test_dedupe_keeps_latest_ingest(spark):
    lines = [ev("1", size=1), ev("1", size=9), ev("2")]
    out = t.dedupe_events(t.parse_events(bronze(spark, lines)))
    assert out.count() == 2
    kept = out.filter("event_id = '1'").collect()[0]
    assert kept.push_commit_count == 9  # later _ingested_at wins


def test_gold_aggregates(spark):
    lines = [
        ev("1", actor=1, repo=10),
        ev("2", actor=2, repo=10),
        ev("3", actor=1, repo=11, type_="WatchEvent", size=None),
    ]
    silver = t.with_partition_columns(t.dedupe_events(t.parse_events(bronze(spark, lines))))

    hourly = {r.event_type: r.events for r in t.hourly_activity(silver).collect()}
    assert hourly == {"PushEvent": 2, "WatchEvent": 1}

    top = t.top_repos_daily(silver, top_n=1).collect()
    assert len(top) == 1 and top[0].repo_id == 10 and top[0].commits_pushed == 4

    dau = t.daily_active_actors(silver).collect()[0]
    assert dau.daily_active_actors == 2 and dau.events == 3
