# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: analytics tables for the dashboard
# MAGIC Silver is incremental; gold is fully recomputed each run (cheap at this size).

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../src"))

from gh_lakehouse import transforms as t

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "gh_lakehouse")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

silver = spark.table(f"{catalog}.{schema}.silver_events")

outputs = {
    "gold_hourly_activity": t.hourly_activity(silver),
    "gold_top_repos_daily": t.top_repos_daily(silver, top_n=50),
    "gold_daily_active_actors": t.daily_active_actors(silver),
}
for name, df in outputs.items():
    (df.write.mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable(f"{catalog}.{schema}.{name}"))
    print(name, spark.table(f"{catalog}.{schema}.{name}").count())
