"""
Local Silver runner using pandas + DuckDB.
Enriches matches with FIFA rankings, form metrics, H2H stats, squad stats.
Run: python synapse/notebooks/local_run/02_silver_pandas.py
"""

import os
import pandas as pd
import numpy as np
import duckdb

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
BRONZE_DIR   = os.path.join(PROJECT_ROOT, "local", "bronze")
SILVER_DIR   = os.path.join(PROJECT_ROOT, "local", "silver")
os.makedirs(SILVER_DIR, exist_ok=True)

con = duckdb.connect()

# ── Load bronze Parquet files ─────────────────────────────────────────────────
df_matches  = pd.read_parquet(os.path.join(BRONZE_DIR, "matches.parquet"))
df_rankings = pd.read_parquet(os.path.join(BRONZE_DIR, "rankings.parquet"))
df_players  = pd.read_parquet(os.path.join(BRONZE_DIR, "players.parquet"))
df_matches["date"] = pd.to_datetime(df_matches["date"])
df_rankings["rank_date"] = pd.to_datetime(df_rankings["rank_date"])

print(f"Loaded bronze: matches={len(df_matches):,}, rankings={len(df_rankings):,}, players={len(df_players):,}")


# ── Step 1: Enrich with FIFA rankings (as-of join) ────────────────────────────
print("\n=== Step 1: Enrich with FIFA rankings ===")

def get_rank_at_date(team: str, match_date: pd.Timestamp) -> dict:
    """Get the most recent ranking row for a team before a given date."""
    rows = df_rankings[
        (df_rankings["country_full"] == team) &
        (df_rankings["rank_date"] <= match_date)
    ]
    if rows.empty:
        return {"rank": None, "total_points": None, "confederation": None}
    row = rows.sort_values("rank_date").iloc[-1]
    return {
        "rank": int(row["rank"]) if pd.notna(row["rank"]) else None,
        "total_points": float(row["total_points"]) if pd.notna(row["total_points"]) else None,
        "confederation": row.get("confederation", None)
    }

# Vectorize using DuckDB for speed
con.register("matches_df", df_matches)
con.register("rankings_df", df_rankings)

# DuckDB as-of join via window function
enriched_sql = """
WITH home_rank AS (
    SELECT
        m.match_id,
        r.rank        AS home_fifa_rank,
        r.total_points AS home_fifa_points,
        r.confederation AS home_confederation,
        ROW_NUMBER() OVER (PARTITION BY m.match_id ORDER BY r.rank_date DESC) AS rn
    FROM matches_df m
    JOIN rankings_df r
      ON r.country_full = m.home_team
     AND r.rank_date <= m.date
),
away_rank AS (
    SELECT
        m.match_id,
        r.rank        AS away_fifa_rank,
        r.total_points AS away_fifa_points,
        r.confederation AS away_confederation,
        ROW_NUMBER() OVER (PARTITION BY m.match_id ORDER BY r.rank_date DESC) AS rn
    FROM matches_df m
    JOIN rankings_df r
      ON r.country_full = m.away_team
     AND r.rank_date <= m.date
)
SELECT
    m.*,
    hr.home_fifa_rank,
    hr.home_fifa_points,
    hr.home_confederation,
    ar.away_fifa_rank,
    ar.away_fifa_points,
    (hr.home_fifa_rank  - ar.away_fifa_rank)    AS rank_diff,
    (hr.home_fifa_points - ar.away_fifa_points) AS points_diff
FROM matches_df m
LEFT JOIN home_rank hr ON hr.match_id = m.match_id AND hr.rn = 1
LEFT JOIN away_rank ar ON ar.match_id = m.match_id AND ar.rn = 1
"""

df_enriched = con.execute(enriched_sql).df()
print(f"  Enriched matches: {len(df_enriched):,}")
print(f"  Matches with rank data: {df_enriched['home_fifa_rank'].notna().sum():,}")


# ── Step 2: Team Form (rolling 5 & 10 match averages) ────────────────────────
print("\n=== Step 2: Team form metrics ===")

# Build one row per team per match
home_side = df_enriched[["match_id","date","home_team","home_score","away_score","result"]].copy()
home_side.rename(columns={"home_team":"team","home_score":"gf","away_score":"ga"}, inplace=True)
home_side["points"] = home_side["result"].map({"HOME_WIN":3,"DRAW":1,"AWAY_WIN":0})

away_side = df_enriched[["match_id","date","away_team","away_score","home_score","result"]].copy()
away_side.rename(columns={"away_team":"team","away_score":"gf","home_score":"ga"}, inplace=True)
away_side["points"] = away_side["result"].map({"AWAY_WIN":3,"DRAW":1,"HOME_WIN":0})

df_team_matches = pd.concat([home_side, away_side], ignore_index=True)
df_team_matches = df_team_matches.sort_values(["team","date"])

# Rolling averages (shift(1) so we don't include current match)
df_team_matches["form_pts_last5"]  = (
    df_team_matches.groupby("team")["points"]
                   .transform(lambda x: x.shift(1).rolling(5,  min_periods=1).mean())
)
df_team_matches["form_pts_last10"] = (
    df_team_matches.groupby("team")["points"]
                   .transform(lambda x: x.shift(1).rolling(10, min_periods=1).mean())
)
df_team_matches["avg_gf_last5"]    = (
    df_team_matches.groupby("team")["gf"]
                   .transform(lambda x: x.shift(1).rolling(5,  min_periods=1).mean())
)
df_team_matches["avg_ga_last5"]    = (
    df_team_matches.groupby("team")["ga"]
                   .transform(lambda x: x.shift(1).rolling(5,  min_periods=1).mean())
)

# Merge form back onto enriched matches (home side)
home_form = df_team_matches[["match_id","team","form_pts_last5","form_pts_last10",
                              "avg_gf_last5","avg_ga_last5"]].copy()
home_form.columns = ["match_id","home_team","home_form_pts_last5","home_form_pts_last10",
                     "home_avg_gf_last5","home_avg_ga_last5"]
away_form = df_team_matches[["match_id","team","form_pts_last5","form_pts_last10",
                              "avg_gf_last5","avg_ga_last5"]].copy()
away_form.columns = ["match_id","away_team","away_form_pts_last5","away_form_pts_last10",
                     "away_avg_gf_last5","away_avg_ga_last5"]

df_enriched = df_enriched.merge(home_form, on=["match_id","home_team"], how="left")
df_enriched = df_enriched.merge(away_form, on=["match_id","away_team"], how="left")
print(f"  Form features added.")


# ── Step 3: Head-to-head stats ────────────────────────────────────────────────
print("\n=== Step 3: Head-to-head stats ===")

def h2h_win_rate(df: pd.DataFrame) -> pd.Series:
    """For each row, compute home team's h2h win rate in all PRIOR meetings."""
    df = df.sort_values("date").copy()
    rates = []
    for idx, row in df.iterrows():
        prior = df[(df["date"] < row["date"]) &
                   (((df["home_team"]==row["home_team"]) & (df["away_team"]==row["away_team"])) |
                    ((df["home_team"]==row["away_team"]) & (df["away_team"]==row["home_team"])))]
        if prior.empty:
            rates.append(0.5)
        else:
            # Count wins for the current "home_team"
            wins = prior[
                ((prior["home_team"]==row["home_team"]) & (prior["result"]=="HOME_WIN")) |
                ((prior["away_team"]==row["home_team"]) & (prior["result"]=="AWAY_WIN"))
            ]
            rates.append(len(wins) / len(prior))
    return pd.Series(rates, index=df.index)

df_enriched["h2h_home_win_rate"] = h2h_win_rate(df_enriched)
df_enriched["h2h_total"]         = df_enriched.groupby(
    [df_enriched[["home_team","away_team"]].apply(
        lambda r: tuple(sorted([r["home_team"],r["away_team"]])), axis=1
    )]
)["match_id"].transform("count") - 1

print(f"  H2H features added.")


# ── Step 4: Squad stats per nationality ───────────────────────────────────────
print("\n=== Step 4: Squad stats ===")

df_squad = df_players.groupby("nationality_name").agg(
    squad_avg_overall   = ("overall",   "mean"),
    squad_avg_age       = ("age",       "mean"),
    squad_avg_value     = ("value_eur", "mean"),
    best_player_rating  = ("overall",   "max"),
    squad_size          = ("sofifa_id", "count"),
).reset_index()

df_enriched = df_enriched.merge(
    df_squad.rename(columns={"nationality_name":"home_team",
                             "squad_avg_overall":"home_squad_avg_overall",
                             "squad_avg_age":"home_squad_avg_age",
                             "squad_avg_value":"home_squad_avg_value",
                             "best_player_rating":"home_best_player_rating",
                             "squad_size":"home_squad_size"}),
    on="home_team", how="left"
)
df_enriched = df_enriched.merge(
    df_squad.rename(columns={"nationality_name":"away_team",
                             "squad_avg_overall":"away_squad_avg_overall",
                             "squad_avg_age":"away_squad_avg_age",
                             "squad_avg_value":"away_squad_avg_value",
                             "best_player_rating":"away_best_player_rating",
                             "squad_size":"away_squad_size"}),
    on="away_team", how="left"
)
print(f"  Squad features added.")


# ── Write Silver Parquet ──────────────────────────────────────────────────────
print("\n=== Writing Silver layer ===")

silver_path = os.path.join(SILVER_DIR, "matches_enriched.parquet")
df_enriched.to_parquet(silver_path, index=False)
print(f"  matches_enriched: {len(df_enriched):,} rows  {len(df_enriched.columns)} columns")

squad_path = os.path.join(SILVER_DIR, "squad_stats.parquet")
df_squad.to_parquet(squad_path, index=False)

form_path = os.path.join(SILVER_DIR, "team_form.parquet")
df_team_matches.to_parquet(form_path, index=False)

# Quick DuckDB summary
con.register("silver_matches", df_enriched)
print("\n  Tournament distribution:")
print(con.execute("""
    SELECT tournament, COUNT(*) as cnt
    FROM silver_matches
    GROUP BY tournament ORDER BY cnt DESC LIMIT 8
""").df().to_string(index=False))

print(f"\nSilver layer written to: {SILVER_DIR}")
print("Next: run 03_gold_pandas.py")
