-- Datasets for a Databricks AI/BI dashboard. Replace workspace.gh_lakehouse if you changed names.

-- 1) Events per hour by type
SELECT event_hour, event_type, events
FROM workspace.gh_lakehouse.gold_hourly_activity
ORDER BY event_hour;

-- 2) Top 20 repos for the latest day
SELECT repo_name, events, active_actors, commits_pushed
FROM workspace.gh_lakehouse.gold_top_repos_daily
WHERE event_date = (SELECT max(event_date) FROM workspace.gh_lakehouse.gold_top_repos_daily)
ORDER BY rank
LIMIT 20;

-- 3) Daily active contributors
SELECT event_date, daily_active_actors, events
FROM workspace.gh_lakehouse.gold_daily_active_actors
ORDER BY event_date;

-- 4) Pipeline health: rows in vs quarantined vs duplicates dropped
SELECT run_ts, rows_in, rows_quarantined, duplicates_dropped, rows_merged
FROM workspace.gh_lakehouse.dq_metrics
ORDER BY run_ts;
