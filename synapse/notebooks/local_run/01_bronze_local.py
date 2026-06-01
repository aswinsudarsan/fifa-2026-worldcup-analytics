"""
Local runner for Bronze ingestion — no Azure required.
Reads from data/sample/*.csv, writes Delta tables to local/bronze/
Run: python synapse/notebooks/local_run/01_bronze_local.py
"""

import os, sys

# Point to project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, BooleanType
)

# ── Start Spark with Delta Lake ───────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("FIFA2026_Bronze_Local") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark started:", spark.version)

DATA_DIR   = os.path.join(PROJECT_ROOT, "data", "sample")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "local", "bronze")
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ── 1. Matches ─────────────────────────────────────────────────────────────
print("\n=== Ingesting matches ===")

matches_schema = StructType([
    StructField("date",        StringType(),  True),
    StructField("home_team",   StringType(),  False),
    StructField("away_team",   StringType(),  False),
    StructField("home_score",  IntegerType(), True),
    StructField("away_score",  IntegerType(), True),
    StructField("tournament",  StringType(),  True),
    StructField("city",        StringType(),  True),
    StructField("country",     StringType(),  True),
    StructField("neutral",     StringType(),  True),
])

df_matches = spark.read \
    .option("header", "true") \
    .schema(matches_schema) \
    .csv(os.path.join(DATA_DIR, "matches.csv")) \
    .withColumn("date", F.to_date("date")) \
    .withColumn("neutral", F.col("neutral").cast(BooleanType())) \
    .dropDuplicates(["date", "home_team", "away_team"]) \
    .withColumn("match_id",
        F.md5(F.concat_ws("_", F.col("date").cast(StringType()),
                           F.col("home_team"), F.col("away_team")))
    ) \
    .withColumn("total_goals", F.col("home_score") + F.col("away_score")) \
    .withColumn("result",
        F.when(F.col("home_score") > F.col("away_score"), "HOME_WIN")
         .when(F.col("home_score") < F.col("away_score"), "AWAY_WIN")
         .otherwise("DRAW")
    ) \
    .withColumn("_ingestion_date", F.current_date()) \
    .withColumn("_ingestion_ts",   F.current_timestamp())

matches_out = os.path.join(OUTPUT_DIR, "matches")
df_matches.write.format("delta").mode("overwrite").save(matches_out)
print(f"  Matches written: {df_matches.count():,} rows → {matches_out}")

# Quick check
df_matches.groupBy("result").count().show()


# ── 2. Rankings ───────────────────────────────────────────────────────────
print("\n=== Ingesting rankings ===")

df_rankings = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(os.path.join(DATA_DIR, "rankings.csv")) \
    .dropDuplicates(["country_abrv", "rank_date"]) \
    .withColumn("_ingestion_date", F.current_date())

rankings_out = os.path.join(OUTPUT_DIR, "rankings")
df_rankings.write.format("delta").mode("overwrite").save(rankings_out)
print(f"  Rankings written: {df_rankings.count():,} rows → {rankings_out}")


# ── 3. Players ────────────────────────────────────────────────────────────
print("\n=== Ingesting players ===")

df_players = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(os.path.join(DATA_DIR, "players.csv")) \
    .dropDuplicates(["sofifa_id"]) \
    .withColumn("_ingestion_date", F.current_date())

players_out = os.path.join(OUTPUT_DIR, "players")
df_players.write.format("delta").mode("overwrite").save(players_out)
print(f"  Players written:  {df_players.count():,} rows → {players_out}")


print("\n✅ Bronze ingestion complete!")
print(f"   Output: {OUTPUT_DIR}")
print("   Next: run 02_silver_local.py")

spark.stop()
