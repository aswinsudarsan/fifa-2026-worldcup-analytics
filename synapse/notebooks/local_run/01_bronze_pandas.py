"""
Local Bronze runner using pandas + DuckDB (no Java / no Azure needed).
Simulates what the Synapse Spark notebook does, using the same logic.
Run: python synapse/notebooks/local_run/01_bronze_pandas.py
"""

import os
import hashlib
import pandas as pd
import duckdb

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
DATA_DIR     = os.path.join(PROJECT_ROOT, "data", "sample")
OUTPUT_DIR   = os.path.join(PROJECT_ROOT, "local", "bronze")
os.makedirs(OUTPUT_DIR, exist_ok=True)

con = duckdb.connect()       # in-memory DuckDB (our local "SQL Pool")
print("DuckDB version:", duckdb.__version__)


# â”€â”€ 1. Ingest Matches â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
print("\n=== Step 1: Ingest matches ===")

df_matches = pd.read_csv(
    os.path.join(DATA_DIR, "matches.csv"),
    parse_dates=["date"],
    dtype={"home_score": "Int64", "away_score": "Int64"}
)

print(f"  Raw rows: {len(df_matches):,}")

# Schema enforcement
df_matches = df_matches[["date","home_team","away_team","home_score",
                          "away_score","tournament","city","country","neutral"]]
df_matches["neutral"] = df_matches["neutral"].astype(str).str.lower().isin(["true","1"])

# Dedup
df_matches = df_matches.drop_duplicates(subset=["date","home_team","away_team"])

# Derived columns
def make_match_id(row):
    key = f"{row['date']}_{row['home_team']}_{row['away_team']}"
    return hashlib.md5(key.encode()).hexdigest()

df_matches["match_id"]     = df_matches.apply(make_match_id, axis=1)
df_matches["total_goals"]  = df_matches["home_score"] + df_matches["away_score"]
df_matches["result"]       = df_matches.apply(
    lambda r: "HOME_WIN" if r["home_score"] > r["away_score"]
    else ("AWAY_WIN" if r["home_score"] < r["away_score"] else "DRAW"), axis=1
)
df_matches["_ingestion_date"] = pd.Timestamp.now().date()

# Save as Parquet (local equivalent of Delta)
out_path = os.path.join(OUTPUT_DIR, "matches.parquet")
df_matches.to_parquet(out_path, index=False)
print(f"  Written: {len(df_matches):,} rows â†’ {out_path}")

# Register in DuckDB
con.execute(f"CREATE OR REPLACE VIEW bronze_matches AS SELECT * FROM read_parquet('{out_path}')")
print("\n  Result distribution:")
print(con.execute("SELECT result, COUNT(*) as cnt FROM bronze_matches GROUP BY result").df())


# â”€â”€ 2. Ingest Rankings â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
print("\n=== Step 2: Ingest rankings ===")

df_rankings = pd.read_csv(
    os.path.join(DATA_DIR, "rankings.csv"),
    parse_dates=["rank_date"],
)
df_rankings = df_rankings.drop_duplicates(subset=["country_abrv","rank_date"])
df_rankings["_ingestion_date"] = pd.Timestamp.now().date()

out_path_r = os.path.join(OUTPUT_DIR, "rankings.parquet")
df_rankings.to_parquet(out_path_r, index=False)
con.execute(f"CREATE OR REPLACE VIEW bronze_rankings AS SELECT * FROM read_parquet('{out_path_r}')")
print(f"  Written: {len(df_rankings):,} rows â†’ {out_path_r}")


# â”€â”€ 3. Ingest Players â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
print("\n=== Step 3: Ingest players ===")

df_players = pd.read_csv(os.path.join(DATA_DIR, "players.csv"))
df_players = df_players.drop_duplicates(subset=["sofifa_id"])
df_players["_ingestion_date"] = pd.Timestamp.now().date()

out_path_p = os.path.join(OUTPUT_DIR, "players.parquet")
df_players.to_parquet(out_path_p, index=False)
con.execute(f"CREATE OR REPLACE VIEW bronze_players AS SELECT * FROM read_parquet('{out_path_p}')")
print(f"  Written: {len(df_players):,} rows â†’ {out_path_p}")


# â”€â”€ 4. Quick data quality checks using DuckDB â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
print("\n=== Step 4: Data quality checks ===")

checks = [
    ("No null match_id",
     "SELECT COUNT(*) FROM bronze_matches WHERE match_id IS NULL"),
    ("No negative scores",
     "SELECT COUNT(*) FROM bronze_matches WHERE home_score < 0 OR away_score < 0"),
    ("Total matches",
     "SELECT COUNT(*) FROM bronze_matches"),
    ("Total players",
     "SELECT COUNT(*) FROM bronze_players"),
    ("Unique nationalities",
     "SELECT COUNT(DISTINCT nationality_name) FROM bronze_players"),
    ("Date range",
     "SELECT MIN(date), MAX(date) FROM bronze_matches"),
]

for name, sql in checks:
    result = con.execute(sql).fetchone()
    print(f"  {name}: {result}")


print("\nâœ… Bronze ingestion complete!")
print(f"   Parquet files saved in: {OUTPUT_DIR}")
print("   Next: run 02_silver_pandas.py")

