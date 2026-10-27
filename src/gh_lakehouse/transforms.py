"""Pure PySpark transforms for the GH Archive lakehouse.

Kept free of I/O so they can be unit-tested locally and reused by the
Databricks notebooks.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Only the flat fields we need. `payload` is read separately with
# get_json_object because its shape differs for every event type.
EVENT_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType()),
        T.StructField("type", T.StringType()),
        T.StructField("public", T.BooleanType()),
        T.StructField("created_at", T.StringType()),
        T.StructField(
            "actor",
            T.StructType(
                [T.StructField("id", T.LongType()), T.StructField("login", T.StringType())]
            ),
        ),
        T.StructField(
            "repo",
            T.StructType(
                [T.StructField("id", T.LongType()), T.StructField("name", T.StringType())]
            ),
        ),
        T.StructField("org", T.StructType([T.StructField("login", T.StringType())])),
    ]
)

REQUIRED_COLUMNS = ["event_id", "event_type", "actor_id", "repo_id", "created_at"]


def parse_events(bronze: DataFrame) -> DataFrame:
    """Parse raw JSON lines (column `raw`) into typed columns."""
    parsed = bronze.withColumn("e", F.from_json("raw", EVENT_SCHEMA))
    return parsed.select(
        F.col("e.id").alias("event_id"),
        F.col("e.type").alias("event_type"),
        F.col("e.actor.id").alias("actor_id"),
        F.col("e.actor.login").alias("actor_login"),
        F.col("e.repo.id").alias("repo_id"),
        F.col("e.repo.name").alias("repo_name"),
        F.col("e.org.login").alias("org_login"),
        F.col("e.public").alias("is_public"),
        F.to_timestamp("e.created_at", "yyyy-MM-dd'T'HH:mm:ssX").alias("created_at"),
        F.get_json_object("raw", "$.payload.action").alias("payload_action"),
        F.get_json_object("raw", "$.payload.size").cast("int").alias("push_commit_count"),
        F.col("_source_file"),
        F.col("_ingested_at"),
        F.col("raw"),
    )


def split_valid_invalid(events: DataFrame):
    """Split into (valid, quarantine). Quarantine rows carry a reason."""
    missing = [F.when(F.col(c).isNull(), F.lit(c)) for c in REQUIRED_COLUMNS]
    flagged = events.withColumn("dq_missing", F.concat_ws(",", *missing))
    valid = flagged.filter(F.col("dq_missing") == "").drop("dq_missing")
    invalid = (
        flagged.filter(F.col("dq_missing") != "")
        .withColumn("dq_reason", F.concat(F.lit("missing: "), F.col("dq_missing")))
        .drop("dq_missing")
    )
    return valid, invalid


def dedupe_events(events: DataFrame) -> DataFrame:
    """Keep the most recently ingested copy of each event_id."""
    w = Window.partitionBy("event_id").orderBy(F.col("_ingested_at").desc())
    return (
        events.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def with_partition_columns(events: DataFrame) -> DataFrame:
    return events.withColumn("event_date", F.to_date("created_at")).withColumn(
        "event_hour", F.date_trunc("hour", F.col("created_at"))
    )


# ---------------------------------------------------------------- gold
def hourly_activity(silver: DataFrame) -> DataFrame:
    return silver.groupBy("event_hour", "event_type").agg(
        F.count("*").alias("events"),
        F.countDistinct("actor_id").alias("active_actors"),
    )


def top_repos_daily(silver: DataFrame, top_n: int = 50) -> DataFrame:
    daily = silver.groupBy("event_date", "repo_id", "repo_name").agg(
        F.count("*").alias("events"),
        F.countDistinct("actor_id").alias("active_actors"),
        F.sum(F.coalesce("push_commit_count", F.lit(0))).alias("commits_pushed"),
    )
    w = Window.partitionBy("event_date").orderBy(F.col("events").desc())
    return daily.withColumn("rank", F.row_number().over(w)).filter(
        F.col("rank") <= top_n
    )


def daily_active_actors(silver: DataFrame) -> DataFrame:
    return silver.groupBy("event_date").agg(
        F.countDistinct("actor_id").alias("daily_active_actors"),
        F.count("*").alias("events"),
    )
