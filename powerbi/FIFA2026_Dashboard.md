# Power BI Dashboard — FIFA 2026 World Cup Analytics

## Connection Setup

**Data source:** Synapse Serverless SQL Pool (DirectQuery)

```
Server:  synapse-fifa2026-dev-ondemand.sql.azuresynapse.net
Database: master
Authentication: Azure Active Directory
Mode: DirectQuery
```

---

## Report Pages

### Page 1 — World Cup Overview
**Visuals:**
- KPI cards: 48 Teams | 16 Groups | 104 Matches | 3 Host Nations
- Map visual: All 16 host stadiums (USA, Canada, Mexico) with capacity bubbles
- Donut chart: Teams by Confederation (UEFA/CONMEBOL/CAF/AFC/CONCACAF/OFC)
- Bar chart: Top 10 teams by FIFA ranking
- Table: All 48 qualified teams with flag, rank, squad rating

**DAX Measures:**
```dax
Total Teams = COUNTROWS(gold_wc2026_qualified_teams)

Avg FIFA Rank = AVERAGE(gold_dim_team[current_fifa_rank])

Top Ranked Team =
    CALCULATE(
        SELECTEDVALUE(gold_dim_team[team_name]),
        TOPN(1, gold_dim_team, gold_dim_team[current_fifa_rank], ASC)
    )
```

---

### Page 2 — Team Deep Dive
**Visuals:**
- Team selector slicer (dropdown)
- Radar/spider chart: Squad Overall | Form | H2H WinRate | Goals For | Goals Against | Rank
- Line chart: Win % trend over years (1990–2026)
- Stacked bar: Results breakdown per tournament (Win/Draw/Loss)
- Card: WC wins, WC goals, WC appearances
- Scatter plot: Squad Rating vs Current FIFA Rank (all 48 teams)

**DAX Measures:**
```dax
WC Win Rate =
    DIVIDE(
        CALCULATE(COUNTROWS(gold_fact_matches),
            gold_fact_matches[result] = "HOME_WIN",
            gold_fact_matches[tournament] = "FIFA World Cup"),
        CALCULATE(COUNTROWS(gold_fact_matches),
            gold_fact_matches[tournament] = "FIFA World Cup")
    )

Goals Per Match =
    DIVIDE(
        SUM(gold_fact_matches[total_goals]),
        COUNTROWS(gold_fact_matches)
    )

Form Index =
    CALCULATE(
        AVERAGE(silver_team_form[form_pts_last5]),
        LASTDATE(silver_team_form[date])
    )
```

---

### Page 3 — Group Stage Predictions
**Visuals:**
- Group selector (A through L)
- Matrix: Group table (Team | P | W | D | L | Pts) from ML predictions
- Fixture card for each match with probability gauge charts
  - Home Win % | Draw % | Away Win %
- Conditional formatting: Green = predicted winner, amber = draw
- Stacked 100% bar: Win/Draw/Loss probabilities per fixture
- Highlighted: Top 2 teams advancing from each group

**DAX Measures:**
```dax
Predicted Points =
    SUMX(
        gold_wc2026_group_predictions,
        SWITCH(
            gold_wc2026_group_predictions[predicted_outcome],
            "HOME_WIN", 3,
            "DRAW", 1,
            0
        )
    )

Upset Index =
    -- Low-ranked team beats high-ranked team
    CALCULATE(
        AVERAGE(gold_wc2026_group_predictions[away_win_prob]),
        gold_fact_matches[rank_diff] > 20
    )
```

---

### Page 4 — Historical Match Analytics
**Visuals:**
- Trend line: Goals per match across World Cups (1930–2022)
- Heatmap calendar: Match density by month/year
- Bar chart: Home win % by tournament type
- Scatter: FIFA rank diff vs match outcome (does rank gap predict results?)
- Top 10 highest-scoring World Cup matches (all time)
- Win/Draw/Loss distribution pie chart

**DAX Measures:**
```dax
Home Advantage Factor =
    DIVIDE(
        CALCULATE(COUNTROWS(gold_fact_matches),
            gold_fact_matches[result] = "HOME_WIN",
            gold_fact_matches[neutral] = FALSE()),
        CALCULATE(COUNTROWS(gold_fact_matches),
            gold_fact_matches[neutral] = FALSE())
    ) -
    DIVIDE(
        CALCULATE(COUNTROWS(gold_fact_matches),
            gold_fact_matches[result] = "HOME_WIN",
            gold_fact_matches[neutral] = TRUE()),
        CALCULATE(COUNTROWS(gold_fact_matches),
            gold_fact_matches[neutral] = TRUE())
    )
```

---

### Page 5 — Live Scores (Synapse Link)
**Visuals:**
- Live match ticker (auto-refresh every 5 min via DirectQuery)
- Prediction accuracy tracker: "Model predicted X% of completed matches correctly"
- Goal timeline chart per match
- Upset tracker: Matches where actual result differs from prediction

**Connection:** `gold.live_match_results` (updated by notebook 05 via Synapse Link)

---

## Branding & Theme

```json
{
  "name": "FIFA2026Theme",
  "dataColors": ["#003087", "#CC0000", "#FFFFFF", "#FFD700", "#00843D", "#FF6B00"],
  "background": "#0A1628",
  "foreground": "#FFFFFF",
  "tableAccent": "#003087",
  "fontFamily": "Segoe UI"
}
```

---

## Row-Level Security (RLS)

```dax
-- Confederation filter role
[confederation] = USERPRINCIPALNAME()
-- Assign UEFA analysts to UEFA role, etc.
```

---

## Publish & Share

1. Publish to Power BI Service workspace: `FIFA2026-Analytics`
2. Enable scheduled refresh: Daily at 06:00 UTC
3. Create App for stakeholder distribution
4. Embed in Azure Static Web App (optional)
