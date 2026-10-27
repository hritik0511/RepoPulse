# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Quality gate
# MAGIC Fails the job (so the failure is visible and alertable) if the latest runs look unhealthy.

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "gh_lakehouse")
dbutils.widgets.text("max_quarantine_pct", "1.0")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
max_pct = float(dbutils.widgets.get("max_quarantine_pct"))

m = spark.table(f"{catalog}.{schema}.dq_metrics").orderBy("run_ts", ascending=False).limit(10)
row = m.selectExpr("sum(rows_in) as rows_in", "sum(rows_quarantined) as q").collect()[0]
rows_in, q = row.rows_in or 0, row.q or 0
pct = 100.0 * q / rows_in if rows_in else 0.0
print(f"last 10 batches: {rows_in} rows in, {q} quarantined ({pct:.3f}%)")

assert spark.table(f"{catalog}.{schema}.silver_events").limit(1).count() == 1, "silver is empty"
assert pct <= max_pct, f"quarantine rate {pct:.3f}% exceeds {max_pct}%"
