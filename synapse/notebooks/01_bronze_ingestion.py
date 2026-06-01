"""
Notebook: 01_bronze_ingestion
Layer: Bronze (Raw → Validated Parquet with schema enforcement)
Spark Pool: sparkfifa2026

Purpose:
  Read raw Parquet/CSV files from the raw container, apply schema,
  deduplicate, add audit columns, and write as Delta tables to bronze/.
"""

# ── Parameters (injected by ADF / Synapse pipeline) ──────────────────────────
ingestion_date = "2026-01-01"    # overridden at runtime

# ── Imports ───────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType, BooleanType
)
from delta.tables import DeltaTable
import logging

logger = logging.getLogger("bronze_ingestion")

spark = SparkSession.builder \
    .appName("FIFA2026_Bronze_Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ── Config ────────────────────────────────────────────────────────────────────
STORAGE_ACCOUNT = "adlsfifa2026dev"
RAW_BASE  = f"abfss://raw@{STORAGE_ACCOUNT}.dfs.core.windows.net"
BRONZE_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# ── Schema definitions ────────────────────────────────────────────────────────
matches_schema = StructType([
    StructField("date",         DateType(),    True),
    StructField("home_team",    StringType(),  False),
    StructField("away_team",    StringType(),  False),
    StructField("home_score",   IntegerType(), True),
    StructField("away_score",   IntegerType(), True),
    StructField("tournament",   StringType(),  True),
    StructField("city",         StringType(),  True),
    StructField("country",      StringType(),  True),
    StructField("neutral",      BooleanType(), True),
])

rankings_schema = StructType([
    StructField("rank",           IntegerType(), True),
    StructField("country_full",   StringType(),  True),
    StructField("country_abrv",   StringType(),  True),
    StructField("total_points",   DoubleType(),  True),
    StructField("previous_points",DoubleType(),  True),
    StructField("rank_change",    IntegerType(), True),
    StructField("cur_year_avg",   DoubleType(),  True),
    StructField("cur_year_avg_weighted", DoubleType(), True),
    StructField("last_year_avg",  DoubleType(),  True),
    StructField("last_year_avg_weighted", DoubleType(), True),
    StructField("two_year_ago_avg", DoubleType(),True),
    StructField("three_year_ago_avg",DoubleType(),True),
    StructField("confederation",  StringType(),  True),
    StructField("rank_date",      DateType(),    True),
])

players_schema = StructType([
    StructField("sofifa_id",      IntegerType(), True),
    StructField("player_url",     StringType(),  True),
    StructField("short_name",     StringType(),  True),
    StructField("long_name",      StringType(),  True),
    StructField("player_positions",StringType(), True),
    StructField("overall",        IntegerType(), True),
    StructField("potential",      IntegerType(), True),
    StructField("value_eur",      DoubleType(),  True),
    StructField("wage_eur",       DoubleType(),  True),
    StructField("age",            IntegerType(), True),
    StructField("dob",            DateType(),    True),
    StructField("nationality_name",StringType(), True),
    StructField("nationality_id", IntegerType(), True),
    StructField("club_name",      StringType(),  True),
])


def add_audit_columns(df, source_layer: str = "raw"):
    """Add standard audit/lineage columns to every Bronze table."""
    return df.withColumn("_ingestion_date",  F.lit(ingestion_date).cast(DateType())) \
             .withColumn("_ingestion_ts",    F.current_timestamp()) \
             .withColumn("_source_layer",    F.lit(source_layer)) \
             .withColumn("_source_file",     F.input_file_name())


def upsert_to_delta(df, delta_path: str, merge_keys: list):
    """Merge-upsert into Delta table (idempotent re-runs)."""
    if DeltaTable.isDeltaTable(spark, delta_path):
        dt = DeltaTable.forPath(spark, delta_path)
        merge_condition = " AND ".join([f"existing.{k} = updates.{k}" for k in merge_keys])
        dt.alias("existing") \
          .merge(df.alias("updates"), merge_condition) \
          .whenMatchedUpdateAll() \
          .whenNotMatchedInsertAll() \
          .execute()
        logger.info(f"Upserted into existing Delta table: {delta_path}")
    else:
        df.write.format("delta").mode("overwrite").save(delta_path)
        logger.info(f"Created new Delta table: {delta_path}")


# ── 1. Ingest Match Results ───────────────────────────────────────────────────
print("=" * 60)
print("Step 1: Ingesting match results")
print("=" * 60)

raw_matches_path = f"{RAW_BASE}/fifa_matches/"

df_matches_raw = spark.read \
    .option("header", "true") \
    .option("dateFormat", "yyyy-MM-dd") \
    .schema(matches_schema) \
    .parquet(raw_matches_path)

df_matches_raw.printSchema()
print(f"Raw match records: {df_matches_raw.count():,}")

# Quality checks
null_scores = df_matches_raw.filter(F.col("home_score").isNull() | F.col("away_score").isNull())
print(f"Records with null scores: {null_scores.count():,}")

# Deduplicate on natural key
df_matches_clean = df_matches_raw \
    .dropDuplicates(["date", "home_team", "away_team"]) \
    .filter(F.col("home_team").isNotNull() & F.col("away_team").isNotNull())

# Add audit + derived columns
df_matches_bronze = add_audit_columns(df_matches_clean) \
    .withColumn("match_id",
        F.md5(F.concat_ws("_",
            F.col("date").cast(StringType()),
            F.col("home_team"),
            F.col("away_team")
        ))
    ) \
    .withColumn("total_goals", F.col("home_score") + F.col("away_score")) \
    .withColumn("result",
        F.when(F.col("home_score") > F.col("away_score"), "HOME_WIN")
         .when(F.col("home_score") < F.col("away_score"), "AWAY_WIN")
         .otherwise("DRAW")
    )

bronze_matches_path = f"{BRONZE_BASE}/matches"
upsert_to_delta(df_matches_bronze, bronze_matches_path, ["match_id"])
print(f"Bronze match records written: {df_matches_bronze.count():,}")


# ── 2. Ingest FIFA Rankings ───────────────────────────────────────────────────
print("\n" + "=" * 60)
print("Step 2: Ingesting FIFA rankings")
print("=" * 60)

df_rankings_raw = spark.read \
    .option("header", "true") \
    .schema(rankings_schema) \
    .parquet(f"{RAW_BASE}/fifa_rankings/")

df_rankings_bronze = add_audit_columns(df_rankings_raw) \
    .dropDuplicates(["country_abrv", "rank_date"]) \
    .filter(F.col("country_abrv").isNotNull())

bronze_rankings_path = f"{BRONZE_BASE}/rankings"
upsert_to_delta(df_rankings_bronze, bronze_rankings_path, ["country_abrv", "rank_date"])
print(f"Bronze ranking records: {df_rankings_bronze.count():,}")


# ── 3. Ingest Player Stats ────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("Step 3: Ingesting player stats")
print("=" * 60)

df_players_raw = spark.read \
    .option("header", "true") \
    .schema(players_schema) \
    .parquet(f"{RAW_BASE}/player_stats/")

df_players_bronze = add_audit_columns(df_players_raw) \
    .dropDuplicates(["sofifa_id"]) \
    .filter(F.col("sofifa_id").isNotNull() & F.col("nationality_name").isNotNull())

bronze_players_path = f"{BRONZE_BASE}/players"
upsert_to_delta(df_players_bronze, bronze_players_path, ["sofifa_id"])
print(f"Bronze player records: {df_players_bronze.count():,}")


# ── 4. Register Delta tables in Synapse Spark catalog ─────────────────────────
print("\n" + "=" * 60)
print("Step 4: Registering tables in Hive metastore")
print("=" * 60)

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze.matches
    USING DELTA
    LOCATION '{bronze_matches_path}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze.rankings
    USING DELTA
    LOCATION '{bronze_rankings_path}'
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze.players
    USING DELTA
    LOCATION '{bronze_players_path}'
""")

print("All Bronze tables registered.")
print("\nDelta table history for matches:")
spark.sql("DESCRIBE HISTORY bronze.matches LIMIT 5").show(truncate=False)

print("\nBronze ingestion complete!")
