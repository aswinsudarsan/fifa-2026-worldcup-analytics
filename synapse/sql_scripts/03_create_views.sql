-- ============================================================
-- Script: 03_create_views.sql
-- Pool:   Serverless SQL Pool (Built-in)
-- Purpose: Reporting views used by Power BI DirectQuery
-- ============================================================

-- ── View: Team Summary Dashboard ─────────────────────────────────────────────
CREATE OR ALTER VIEW reporting.v_team_summary AS
SELECT
    t.team_name,
    t.team_code,
    t.confederation,
    t.current_fifa_rank,
    t.current_fifa_points,
    t.squad_avg_overall,
    t.squad_avg_age,
    t.squad_avg_value             / 1000000.0  AS squad_avg_value_millions,
    t.best_player_rating,
    p.alltime_matches,
    p.alltime_wins,
    p.alltime_draws,
    p.alltime_losses,
    p.alltime_goals_for,
    p.alltime_goals_against,
    p.alltime_goals_for - p.alltime_goals_against AS alltime_goal_diff,
    ROUND(p.alltime_win_pct * 100, 1)             AS alltime_win_pct,
    p.wc_matches,
    p.wc_wins,
    ROUND(ISNULL(p.wc_win_pct, 0) * 100, 1)      AS wc_win_pct,
    p.wc_goal_diff
FROM gold.dim_team t
LEFT JOIN gold.agg_team_performance p ON t.team_name = p.team;
GO

-- ── View: Head-to-Head Matchup ───────────────────────────────────────────────
CREATE OR ALTER VIEW reporting.v_head_to_head AS
SELECT
    home_team,
    away_team,
    COUNT(*)                                                    AS total_meetings,
    SUM(CASE WHEN result = 'HOME_WIN' THEN 1 ELSE 0 END)       AS home_team_wins,
    SUM(CASE WHEN result = 'DRAW'     THEN 1 ELSE 0 END)       AS draws,
    SUM(CASE WHEN result = 'AWAY_WIN' THEN 1 ELSE 0 END)       AS away_team_wins,
    SUM(home_score)                                             AS home_team_goals,
    SUM(away_score)                                             AS away_team_goals,
    AVG(CAST(total_goals AS FLOAT))                             AS avg_goals_per_match,
    MIN([date])                                                 AS first_meeting,
    MAX([date])                                                 AS last_meeting
FROM gold.fact_matches
GROUP BY home_team, away_team;
GO

-- ── View: Tournament Statistics ──────────────────────────────────────────────
CREATE OR ALTER VIEW reporting.v_tournament_stats AS
SELECT
    tournament,
    match_year,
    COUNT(*)                                                   AS total_matches,
    SUM(total_goals)                                           AS total_goals,
    ROUND(AVG(CAST(total_goals AS FLOAT)), 2)                  AS avg_goals_per_match,
    SUM(CASE WHEN result = 'HOME_WIN' THEN 1 ELSE 0 END)      AS home_wins,
    SUM(CASE WHEN result = 'DRAW'     THEN 1 ELSE 0 END)      AS draws,
    SUM(CASE WHEN result = 'AWAY_WIN' THEN 1 ELSE 0 END)      AS away_wins,
    ROUND(AVG(CASE WHEN result = 'HOME_WIN' THEN 1.0 ELSE 0.0 END) * 100, 1) AS home_win_pct,
    ROUND(AVG(CASE WHEN result = 'DRAW'     THEN 1.0 ELSE 0.0 END) * 100, 1) AS draw_pct,
    ROUND(AVG(CASE WHEN result = 'AWAY_WIN' THEN 1.0 ELSE 0.0 END) * 100, 1) AS away_win_pct,
    AVG(CAST(ABS(rank_diff) AS FLOAT))                         AS avg_rank_diff
FROM gold.fact_matches
WHERE tournament IS NOT NULL
GROUP BY tournament, match_year;
GO

-- ── View: WC 2026 Predictions Summary ────────────────────────────────────────
CREATE OR ALTER VIEW reporting.v_wc2026_predictions AS
SELECT
    p.[group],
    p.home_team,
    p.away_team,
    p.predicted_outcome,
    p.home_win_prob,
    p.draw_prob,
    p.away_win_prob,
    h.current_fifa_rank   AS home_rank,
    a.current_fifa_rank   AS away_rank,
    h.squad_avg_overall   AS home_squad_rating,
    a.squad_avg_overall   AS away_squad_rating,
    h.confederation       AS home_confederation
FROM gold.wc2026_group_predictions p
LEFT JOIN gold.dim_team h ON p.home_team = h.team_name
LEFT JOIN gold.dim_team a ON p.away_team = a.team_name;
GO

-- ── View: Group Stage Points Table Simulation ────────────────────────────────
CREATE OR ALTER VIEW reporting.v_group_stage_table AS
WITH home_pts AS (
    SELECT
        [group],
        home_team                                                           AS team,
        SUM(CASE WHEN predicted_outcome = 'HOME_WIN' THEN 3
                 WHEN predicted_outcome = 'DRAW'     THEN 1 ELSE 0 END)   AS points,
        SUM(CASE WHEN predicted_outcome = 'HOME_WIN' THEN 1 ELSE 0 END)   AS wins,
        SUM(CASE WHEN predicted_outcome = 'DRAW'     THEN 1 ELSE 0 END)   AS draws,
        SUM(CASE WHEN predicted_outcome = 'AWAY_WIN' THEN 1 ELSE 0 END)   AS losses
    FROM gold.wc2026_group_predictions
    GROUP BY [group], home_team
),
away_pts AS (
    SELECT
        [group],
        away_team                                                           AS team,
        SUM(CASE WHEN predicted_outcome = 'AWAY_WIN' THEN 3
                 WHEN predicted_outcome = 'DRAW'     THEN 1 ELSE 0 END)   AS points,
        SUM(CASE WHEN predicted_outcome = 'AWAY_WIN' THEN 1 ELSE 0 END)   AS wins,
        SUM(CASE WHEN predicted_outcome = 'DRAW'     THEN 1 ELSE 0 END)   AS draws,
        SUM(CASE WHEN predicted_outcome = 'HOME_WIN' THEN 1 ELSE 0 END)   AS losses
    FROM gold.wc2026_group_predictions
    GROUP BY [group], away_team
),
combined AS (
    SELECT [group], team, points, wins, draws, losses FROM home_pts
    UNION ALL
    SELECT [group], team, points, wins, draws, losses FROM away_pts
)
SELECT
    [group],
    team,
    SUM(points)  AS total_points,
    SUM(wins)    AS total_wins,
    SUM(draws)   AS total_draws,
    SUM(losses)  AS total_losses,
    RANK() OVER (PARTITION BY [group] ORDER BY SUM(points) DESC) AS group_rank
FROM combined
GROUP BY [group], team;
GO

-- ── View: Top Scorers by Tournament (historical) ─────────────────────────────
CREATE OR ALTER VIEW reporting.v_match_trends AS
SELECT
    match_year,
    tournament,
    COUNT(*)                                AS matches,
    AVG(CAST(total_goals AS FLOAT))         AS avg_goals,
    AVG(CAST(ABS(rank_diff) AS FLOAT))      AS avg_rank_diff,
    ROUND(AVG(CASE WHEN result = 'HOME_WIN' THEN 1.0 ELSE 0.0 END) * 100, 1) AS home_win_pct
FROM gold.fact_matches
GROUP BY match_year, tournament;
GO

PRINT 'All reporting views created.';
