# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: incremental ingestion with Auto Loader
# MAGIC Reads new `.json.gz` files from the landing volume and stores each raw JSON line
# MAGIC untouched, plus lineage columns. Re-runs only pick up files not seen before
# MAGIC (tracked in the checkpoint), so this is safe to schedule every hour.

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "gh_lakehouse")
dbutils.widgets.text("volume", "raw")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume = dbutils.widgets.get("volume")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

landing = f"/Volumes/{catalog}/{schema}/{volume}/landing"
checkpoint = f"/Volumes/{catalog}/{schema}/{volume}/_checkpoints/bronze"
bronze_table = f"{catalog}.{schema}.bronze_events"
dbutils.fs.mkdirs(landing)

# COMMAND ----------
stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "text")  # one JSON document per line
    .load(landing)
    .select(
        F.col("value").alias("raw"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )
)

query = (
    stream.writeStream.option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)  # process everything new, then stop
    .toTable(bronze_table)
)
query.awaitTermination()

# COMMAND ----------
print("bronze rows:", spark.table(bronze_table).count())
