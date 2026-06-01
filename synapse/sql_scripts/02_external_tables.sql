-- ============================================================
-- Script: 02_external_tables.sql
-- Pool:   Serverless SQL Pool (Built-in)
-- Purpose: Create external tables over Delta Lake Gold layer
--          Power BI connects to these via DirectQuery
-- ============================================================

-- ── External data source ─────────────────────────────────────────────────────
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'adls_gold')
BEGIN
    CREATE EXTERNAL DATA SOURCE adls_gold
    WITH (
        LOCATION = 'https://adlsfifa2026dev.dfs.core.windows.net/gold'
    );
END
GO

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'delta_format')
BEGIN
    CREATE EXTERNAL FILE FORMAT delta_format
    WITH (FORMAT_TYPE = DELTA);
END
GO

-- ── Gold external tables ─────────────────────────────────────────────────────

-- dim_team
CREATE EXTERNAL TABLE gold.dim_team (
    team_name           NVARCHAR(100),
    team_code           NVARCHAR(10),
    confederation       NVARCHAR(50),
    current_fifa_rank   INT,
    current_fifa_points FLOAT,
    squad_avg_overall   FLOAT,
    squad_avg_age       FLOAT,
    squad_avg_value     FLOAT,
    best_player_rating  INT,
    squad_size          INT
)
WITH (
    LOCATION = '/dim_team',
    DATA_SOURCE = adls_gold,
    FILE_FORMAT = delta_format
);
GO

-- fact_matches
CREATE EXTERNAL TABLE gold.fact_matches (
    match_id                    NVARCHAR(50),
    [date]                      DATE,
    match_year                  INT,
    match_month                 INT,
    tournament                  NVARCHAR(100),
    home_team                   NVARCHAR(100),
    away_team                   NVARCHAR(100),
    home_score                  INT,
    away_score                  INT,
    result                      NVARCHAR(20),
    total_goals                 INT,
    neutral                     BIT,
    city                        NVARCHAR(100),
    country                     NVARCHAR(100),
    home_fifa_rank              INT,
    away_fifa_rank              INT,
    rank_diff                   INT,
    points_diff                 FLOAT,
    home_form_pts_last5         FLOAT,
    away_form_pts_last5         FLOAT,
    home_form_pts_last10        FLOAT,
    away_form_pts_last10        FLOAT,
    home_avg_gf_last5           FLOAT,
    away_avg_gf_last5           FLOAT,
    h2h_home_win_rate           FLOAT,
    h2h_total                   INT,
    home_squad_avg_overall      FLOAT,
    away_squad_avg_overall      FLOAT
)
WITH (
    LOCATION = '/fact_matches',
    DATA_SOURCE = adls_gold,
    FILE_FORMAT = delta_format
);
GO

-- agg_team_performance
CREATE EXTERNAL TABLE gold.agg_team_performance (
    team                    NVARCHAR(100),
    alltime_wins            INT,
    alltime_draws           INT,
    alltime_losses          INT,
    alltime_goals_for       INT,
    alltime_goals_against   INT,
    alltime_matches         INT,
    alltime_win_pct         FLOAT,
    alltime_goal_diff       INT,
    wc_wins                 INT,
    wc_draws                INT,
    wc_losses               INT,
    wc_goals_for            INT,
    wc_goals_against        INT,
    wc_matches              INT,
    wc_win_pct              FLOAT,
    wc_goal_diff            INT,
    current_fifa_rank       INT,
    squad_avg_overall       FLOAT,
    confederation           NVARCHAR(50)
)
WITH (
    LOCATION = '/agg_team_performance',
    DATA_SOURCE = adls_gold,
    FILE_FORMAT = delta_format
);
GO

-- wc2026_group_predictions (ML output)
CREATE EXTERNAL TABLE gold.wc2026_group_predictions (
    [group]             NVARCHAR(5),
    home_team           NVARCHAR(100),
    away_team           NVARCHAR(100),
    predicted_outcome   NVARCHAR(20),
    home_win_prob       FLOAT,
    draw_prob           FLOAT,
    away_win_prob       FLOAT
)
WITH (
    LOCATION = '/wc2026_group_predictions',
    DATA_SOURCE = adls_gold,
    FILE_FORMAT = delta_format
);
GO

-- wc2026_qualified_teams
CREATE EXTERNAL TABLE gold.wc2026_qualified_teams (
    team_name           NVARCHAR(100),
    team_code           NVARCHAR(10),
    confederation       NVARCHAR(50),
    current_rank        INT,
    alltime_wins        INT,
    alltime_matches     INT,
    alltime_win_pct     FLOAT,
    wc_wins             INT,
    wc_matches          INT,
    squad_avg_overall   FLOAT
)
WITH (
    LOCATION = '/wc2026_qualified_teams',
    DATA_SOURCE = adls_gold,
    FILE_FORMAT = delta_format
);
GO

PRINT 'External tables created successfully over Gold Delta Lake.';
