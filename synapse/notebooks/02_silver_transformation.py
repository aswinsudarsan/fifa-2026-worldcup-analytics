"""
Notebook: 02_silver_transformation
Layer: Silver (Bronze → Cleaned, enriched, joined datasets)
Spark Pool: sparkfifa2026

Purpose:
  - Standardize country names across datasets
  - Enrich matches with FIFA rankings at match date
  - Compute team form metrics (rolling window aggregations)
  - Build head-to-head statistics
  - Write Silver Delta tables for ML feature engineering
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, DateType
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("FIFA2026_Silver_Transformation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

STORAGE_ACCOUNT = "adlsfifa2026dev"
BRONZE_BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
SILVER_BASE = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

# ── Load Bronze tables ────────────────────────────────────────────────────────
df_matches  = spark.read.format("delta").load(f"{BRONZE_BASE}/matches")
df_rankings = spark.read.format("delta").load(f"{BRONZE_BASE}/rankings")
df_players  = spark.read.format("delta").load(f"{BRONZE_BASE}/players")

print(f"Bronze matches:  {df_matches.count():,}")
print(f"Bronze rankings: {df_rankings.count():,}")
print(f"Bronze players:  {df_players.count():,}")


# ── 1. Standardize Country Names ──────────────────────────────────────────────
print("\n=== Step 1: Standardize country names ===")

# Common name variants that differ between datasets
country_mapping = {
    "USA":             "United States",
    "US":              "United States",
    "England":         "England",
    "Korea Republic":  "South Korea",
    "IR Iran":         "Iran",
    "Czech Republic":  "Czechia",
    "Ivory Coast":     "Côte d'Ivoire",
}

country_map_df = spark.createDataFrame(
    [(k, v) for k, v in country_mapping.items()],
    ["raw_name", "standard_name"]
)

def standardize_country(df, col_name: str) -> "DataFrame":
    return df \
        .join(country_map_df.withColumnRenamed("raw_name", col_name)
                             .withColumnRenamed("standard_name", f"{col_name}_std"),
              on=col_name, how="left") \
        .withColumn(col_name, F.coalesce(F.col(f"{col_name}_std"), F.col(col_name))) \
        .drop(f"{col_name}_std")

df_matches = standardize_country(df_matches, "home_team")
df_matches = standardize_country(df_matches, "away_team")
df_rankings = df_rankings.withColumn(
    "country_full",
    F.coalesce(
        country_map_df.select("raw_name", "standard_name")
            .withColumnRenamed("raw_name", "country_full")
            .withColumnRenamed("standard_name", "standard")
            .select("standard"),
        F.col("country_full")
    )
)


# ── 2. Enrich Matches with Rankings at Match Date ─────────────────────────────
print("\n=== Step 2: Enrich with FIFA rankings ===")

# Get the most recent ranking for each team before each match date
# using an as-of join via window function
df_rankings_prep = df_rankings.select(
    F.col("country_full").alias("team"),
    F.col("rank_date"),
    F.col("rank").alias("fifa_rank"),
    F.col("total_points").alias("fifa_points"),
    F.col("confederation")
)

# Home team ranking
df_matches = df_matches \
    .join(
        df_rankings_prep.withColumnRenamed("team", "home_team")
                        .withColumnRenamed("rank_date", "home_rank_date")
                        .withColumnRenamed("fifa_rank", "home_fifa_rank")
                        .withColumnRenamed("fifa_points", "home_fifa_points")
                        .withColumnRenamed("confederation", "home_confederation"),
        on=["home_team"],
        how="left"
    ) \
    .filter(F.col("home_rank_date") <= F.col("date")) \
    .withColumn("home_rank_row",
        F.row_number().over(
            Window.partitionBy("match_id")
                  .orderBy(F.col("home_rank_date").desc())
        )
    ) \
    .filter(F.col("home_rank_row") == 1) \
    .drop("home_rank_row", "home_rank_date")

# Away team ranking
df_matches = df_matches \
    .join(
        df_rankings_prep.withColumnRenamed("team", "away_team")
                        .withColumnRenamed("rank_date", "away_rank_date")
                        .withColumnRenamed("fifa_rank", "away_fifa_rank")
                        .withColumnRenamed("fifa_points", "away_fifa_points")
                        .withColumnRenamed("confederation", "away_confederation"),
        on=["away_team"],
        how="left"
    ) \
    .filter(F.col("away_rank_date") <= F.col("date")) \
    .withColumn("away_rank_row",
        F.row_number().over(
            Window.partitionBy("match_id")
                  .orderBy(F.col("away_rank_date").desc())
        )
    ) \
    .filter(F.col("away_rank_row") == 1) \
    .drop("away_rank_row", "away_rank_date")

# Derived ranking features
df_matches = df_matches \
    .withColumn("rank_diff", F.col("home_fifa_rank") - F.col("away_fifa_rank")) \
    .withColumn("points_diff", F.col("home_fifa_points") - F.col("away_fifa_points"))


# ── 3. Team Form (Rolling 5 and 10 match averages) ────────────────────────────
print("\n=== Step 3: Compute team form metrics ===")

# Explode to one row per team per match
df_team_match = df_matches.select(
    F.col("match_id"),
    F.col("date"),
    F.col("home_team").alias("team"),
    F.col("home_score").alias("goals_for"),
    F.col("away_score").alias("goals_against"),
    F.when(F.col("result") == "HOME_WIN", 3)
     .when(F.col("result") == "DRAW", 1)
     .otherwise(0).alias("points_earned")
).union(
    df_matches.select(
        F.col("match_id"),
        F.col("date"),
        F.col("away_team").alias("team"),
        F.col("away_score").alias("goals_for"),
        F.col("home_score").alias("goals_against"),
        F.when(F.col("result") == "AWAY_WIN", 3)
         .when(F.col("result") == "DRAW", 1)
         .otherwise(0).alias("points_earned")
    )
)

team_window_5 = Window.partitionBy("team").orderBy("date").rowsBetween(-5, -1)
team_window_10 = Window.partitionBy("team").orderBy("date").rowsBetween(-10, -1)

df_form = df_team_match \
    .withColumn("form_pts_last5",  F.avg("points_earned").over(team_window_5)) \
    .withColumn("avg_gf_last5",    F.avg("goals_for").over(team_window_5)) \
    .withColumn("avg_ga_last5",    F.avg("goals_against").over(team_window_5)) \
    .withColumn("form_pts_last10", F.avg("points_earned").over(team_window_10)) \
    .withColumn("avg_gf_last10",   F.avg("goals_for").over(team_window_10)) \
    .withColumn("avg_ga_last10",   F.avg("goals_against").over(team_window_10))

# Re-join form back to matches (home + away)
df_home_form = df_form.select(
    "match_id", "team",
    F.col("form_pts_last5").alias("home_form_pts_last5"),
    F.col("avg_gf_last5").alias("home_avg_gf_last5"),
    F.col("avg_ga_last5").alias("home_avg_ga_last5"),
    F.col("form_pts_last10").alias("home_form_pts_last10"),
).withColumnRenamed("team", "home_team")

df_away_form = df_form.select(
    "match_id", "team",
    F.col("form_pts_last5").alias("away_form_pts_last5"),
    F.col("avg_gf_last5").alias("away_avg_gf_last5"),
    F.col("avg_ga_last5").alias("away_avg_ga_last5"),
    F.col("form_pts_last10").alias("away_form_pts_last10"),
).withColumnRenamed("team", "away_team")

df_matches = df_matches \
    .join(df_home_form, on=["match_id", "home_team"], how="left") \
    .join(df_away_form, on=["match_id", "away_team"], how="left")


# ── 4. Head-to-Head Statistics ────────────────────────────────────────────────
print("\n=== Step 4: Head-to-head statistics ===")

# For each match, compute h2h win rates from all prior meetings
h2h_window = Window \
    .partitionBy("home_team", "away_team") \
    .orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, -1)

df_h2h_raw = df_matches.select(
    "match_id", "date", "home_team", "away_team", "result"
) \
.withColumn("home_wins_cumulative",
    F.sum(F.when(F.col("result") == "HOME_WIN", 1).otherwise(0)).over(h2h_window)
) \
.withColumn("draws_cumulative",
    F.sum(F.when(F.col("result") == "DRAW", 1).otherwise(0)).over(h2h_window)
) \
.withColumn("h2h_total",
    F.count("match_id").over(h2h_window)
) \
.withColumn("h2h_home_win_rate",
    F.when(F.col("h2h_total") > 0,
           F.col("home_wins_cumulative") / F.col("h2h_total"))
     .otherwise(0.5)
) \
.select("match_id", "h2h_home_win_rate", "h2h_total")

df_matches = df_matches.join(df_h2h_raw, on="match_id", how="left")


# ── 5. Squad Statistics per Nationality ──────────────────────────────────────
print("\n=== Step 5: Squad aggregate stats ===")

df_squad_agg = df_players \
    .groupBy("nationality_name") \
    .agg(
        F.avg("overall").alias("squad_avg_overall"),
        F.avg("potential").alias("squad_avg_potential"),
        F.avg("age").alias("squad_avg_age"),
        F.avg("value_eur").alias("squad_avg_value"),
        F.count("sofifa_id").alias("squad_size"),
        F.max("overall").alias("best_player_rating"),
    ) \
    .withColumnRenamed("nationality_name", "team")

df_matches = df_matches \
    .join(df_squad_agg.withColumnRenamed("team", "home_team")
                      .select([F.col(c).alias(f"home_{c}") if c != "home_team" else F.col(c)
                               for c in df_squad_agg.columns]),
          on="home_team", how="left") \
    .join(df_squad_agg.withColumnRenamed("team", "away_team")
                      .select([F.col(c).alias(f"away_{c}") if c != "away_team" else F.col(c)
                               for c in df_squad_agg.columns]),
          on="away_team", how="left")


# ── 6. Filter to World Cup matches only (2026 data) ───────────────────────────
df_wc_matches = df_matches.filter(
    F.col("tournament").isin([
        "FIFA World Cup", "FIFA World Cup qualification",
        "FIFA Confederations Cup", "AFC Asian Cup",
        "UEFA Euro", "Copa América"
    ])
)

print(f"World Cup / major tournament records: {df_wc_matches.count():,}")


# ── 7. Write Silver Delta tables ──────────────────────────────────────────────
print("\n=== Step 6: Writing Silver Delta tables ===")

df_matches.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("tournament") \
    .save(f"{SILVER_BASE}/matches_enriched")

df_form.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{SILVER_BASE}/team_form")

df_squad_agg.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{SILVER_BASE}/squad_stats")

df_wc_matches.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{SILVER_BASE}/wc_matches_only")

# Register in catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.matches_enriched
    USING DELTA LOCATION '{SILVER_BASE}/matches_enriched'
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.team_form
    USING DELTA LOCATION '{SILVER_BASE}/team_form'
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.squad_stats
    USING DELTA LOCATION '{SILVER_BASE}/squad_stats'
""")

print("\nSilver transformation complete!")
spark.sql("SELECT tournament, COUNT(*) as cnt FROM silver.matches_enriched GROUP BY tournament ORDER BY cnt DESC").show(20, truncate=False)
