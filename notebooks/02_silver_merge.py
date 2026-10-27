# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: parse, validate, dedupe, MERGE
# MAGIC Streams new bronze rows (Delta as a streaming source) and, per micro-batch:
# MAGIC 1. parses JSON into typed columns
# MAGIC 2. sends rows missing required fields to a **quarantine** table
# MAGIC 3. de-duplicates by `event_id`
# MAGIC 4. **MERGEs** into silver (idempotent: re-running never creates duplicates)
# MAGIC 5. logs row counts to `dq_metrics`

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../src"))

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from gh_lakehouse import transforms as t

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "gh_lakehouse")
dbutils.widgets.text("volume", "raw")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

bronze = f"{catalog}.{schema}.bronze_events"
silver = f"{catalog}.{schema}.silver_events"
quarantine = f"{catalog}.{schema}.quarantine_events"
metrics = f"{catalog}.{schema}.dq_metrics"
checkpoint = f"/Volumes/{catalog}/{schema}/{volume}/_checkpoints/silver"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver} (
  event_id STRING, event_type STRING, actor_id BIGINT, actor_login STRING,
  repo_id BIGINT, repo_name STRING, org_login STRING, is_public BOOLEAN,
  created_at TIMESTAMP, payload_action STRING, push_commit_count INT,
  _source_file STRING, _ingested_at TIMESTAMP, event_date DATE, event_hour TIMESTAMP
)""")

# COMMAND ----------
def upsert_batch(batch_df, batch_id):
    parsed = t.parse_events(batch_df).cache()
    total = parsed.count()
    valid, invalid = t.split_valid_invalid(parsed)

    invalid_n = invalid.count()
    if invalid_n:
        (invalid.select("raw", "_source_file", "_ingested_at", "dq_reason")
         .withColumn("batch_id", F.lit(batch_id))
         .write.mode("append").saveAsTable(quarantine))

    clean = t.with_partition_columns(t.dedupe_events(valid)).drop("raw")
    clean_n = clean.count()

    (DeltaTable.forName(spark, silver).alias("t")
     .merge(clean.alias("s"), "t.event_id = s.event_id")
     .whenNotMatchedInsertAll()
     .execute())

    (spark.createDataFrame(
        [(batch_id, total, total - invalid_n, invalid_n, (total - invalid_n) - clean_n, clean_n)],
        "batch_id long, rows_in long, rows_valid long, rows_quarantined long, "
        "duplicates_dropped long, rows_merged long")
     .withColumn("run_ts", F.current_timestamp())
     .write.mode("append").saveAsTable(metrics))
    parsed.unpersist()


(spark.readStream.table(bronze)
 .writeStream.foreachBatch(upsert_batch)
 .option("checkpointLocation", checkpoint)
 .trigger(availableNow=True)
 .start()
 .awaitTermination())

# COMMAND ----------
print("silver rows:", spark.table(silver).count())
