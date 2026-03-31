# Cricket Player Performance Intelligence Platform
**Databricks Bootcamp Capstone Project — Zach Wilson 2026**

> *"ESPN Cricinfo tells you what happened. This platform tells you what it means — and flags when something has changed before anyone else notices."*

🔗 **Live App:** https://cricket-intelligence-platform-1352785079224954.aws.databricksapps.com

---

## What Is This?

A full-stack data engineering and AI platform that ingests **5,100+ T20 match files**, processes **~1.2 million ball-by-ball delivery records** through a medallion architecture (Bronze → Silver → Gold), and surfaces real-time player performance intelligence via an interactive Dash dashboard hosted on Databricks Apps.

The platform goes beyond traditional cricket statistics by applying **Z-score anomaly detection** and **Databricks AI-powered narrative generation** (Llama 3.3 70B) to identify statistically significant changes in player form — before they become obvious.

**Verified against ESPN Cricinfo:** Strike rates and batting averages match within 1–2% for all verified players (Rohit Sharma, Babar Azam, Virat Kohli). Minor differences are explained by the Cricsheet dataset coverage window (all T20 formats, up to June 2024).

---

## The 3 Pages — What Each One Does

### Page 1 — Player Scout
*"Who is this player and what does their form look like right now?"*

Open the app and **Rohit Sharma loads automatically**. Switch between any batter or bowler using the dropdown.

**What you see:**

| Section | What it shows |
|---|---|
| **Player Header** | Name, team, batting role (Opener / Top Order / Middle Order etc.), position number, form trajectory badge |
| **Key Stats Grid** | Matches, Runs, Career SR, Current Form SR (with delta vs career), Average, Impact Score, Highest Score, 100s/50s |
| **Phase Breakdown** | Powerplay / Middle / Death — separate SR, Average, Boundary%, Dot% for each phase |
| **AI Analyst Recommendation** | Llama 3.3 70B verdict on this player's current form with a specific tactical recommendation |
| **Strike Rate Chart** | Match-by-match SR trend vs career baseline (hover to see date, score, opponent) |

**Batter vs Bowler toggle:** Click BAT or BOWL at the top to switch modes. Bowler view shows Economy, Wickets, Bowling SR, Dot Ball% instead.

---

### Page 2 — Anomaly Feed
*"Is something statistically unusual happening right now?"*

Shows players whose **last 10 matches** have deviated significantly from their **career baseline**, with an AI-generated explanation for each.

**The Filters:**

| Filter | Options | What it does |
|---|---|---|
| **Signal Type** | All / Hot Streaks / Form Slumps | Hot = recent SR above career norm; Slump = below |
| **Player Type** | All / Batters / Bowlers | Filter by role |
| **Min Deviation** | 0.2σ – 1.2σ slider | Minimum Z-score to appear (see below) |

**What the Min Deviation (σ) slider means:**

The Z-score measures how many **standard deviations** a player's recent form is from their own career average — it's a personal benchmark, not a comparison to other players.

```
Z-score = (Recent SR − Career Average SR) ÷ Career Std Dev
```

| Slider Value | Meaning | Example |
|---|---|---|
| 0.2σ | Tiny shift — show almost everyone | Normal day-to-day variance |
| 0.5σ (default) | Noticeable form shift | Worth watching |
| 0.8σ | Meaningful deviation | Genuine concern or hot streak |
| 1.2σ | Strong outlier | Statistically rare — act now |

**Example:** If a batter's career SR std dev is 30 and they're scoring 20 points below average recently:
- Z-score = 20 ÷ 30 = **0.67σ** — appears at 0.5 threshold, not at 0.8

**Each anomaly card shows:**
- Player name + role + anomaly type (SR Drop / SR Spike / Dot Spike / Dot Drop)
- Career metric vs recent metric with % change
- Z-score badge (how many σ from normal)
- Full AI narrative explaining what's happening and what the team should do

---

### Page 3 — Matchup Intel
*"What happens specifically when this batter faces this bowler?"*

Select any batter + bowler combination. The page shows their full head-to-head history.

**What you see:**

| Section | What it shows |
|---|---|
| **Advantage Badge** | BATTER ADVANTAGE (SR > 120) or BOWLER ADVANTAGE (SR ≤ 120) |
| **H2H Stats** | Balls faced, Runs, SR, Batting Avg, Dot%, Boundary%, Dismissal Rate |
| **AI Tactical Brief** | Live Llama 3.3 70B analysis: when to use this bowler, what phase, key threat |
| **Dangermen Table** | Top 6 bowlers who are most dangerous vs the selected batter (by dismissal rate, min 12 balls) |

**Minimum data requirement:** 6 balls faced between the pair. Below this, the H2H stats aren't statistically meaningful.

---

## Understanding the Metrics

### Batting Metrics

| Metric | Formula | What it means |
|---|---|---|
| **Strike Rate (SR)** | `(Runs ÷ Legal Balls) × 100` | Runs scored per 100 balls. 120+ is good in T20. |
| **Batting Average** | `Runs ÷ Dismissals` | Divided by dismissals (not innings) — not-outs don't deflate it |
| **Career SR** | True overall SR across all career balls | The gold standard baseline |
| **Current Form SR** | Average SR across last 10 matches | Short-term form indicator |
| **Form Delta** | Current Form SR − Career SR | +ve = hotter than usual, -ve = cooler |
| **Dot Ball %** | `Dot Balls ÷ Legal Balls × 100` | Lower is better — dots waste deliveries |
| **Boundary %** | `Boundaries (4s+6s) ÷ Legal Balls × 100` | Scoring efficiency |
| **Highest Score** | Maximum runs in a single innings | Career best innings |

### Bowling Metrics

| Metric | Formula | What it means |
|---|---|---|
| **Economy** | `(Runs Conceded ÷ Legal Balls) × 6` | Runs per over. Excludes byes/legbyes (ICC standard) |
| **Bowling Average** | `Runs Conceded ÷ Wickets` | Runs per wicket. Lower = more deadly |
| **Bowling SR** | `Legal Balls ÷ Wickets` | Balls per wicket. Lower = strikes more often |
| **Dot Ball %** | `Dot Balls ÷ Legal Balls × 100` | Higher is better — building pressure |
| **Career Economy** | True average economy across career | Career baseline |
| **Current Form Economy** | Average economy across last 10 matches | Short-term form |

### Composite Metrics

| Metric | Formula | What it means |
|---|---|---|
| **Impact Score (0–99)** | SR (35%) + Avg (30%) + Boundary% (20%) + Dot Avoidance (15%) | Single ranking number. 70+ = elite |
| **Consistency Score** | `1 − (Std Dev SR ÷ Avg SR)` | Higher = more predictable. "Very Consistent" = 0.85+, "Boom or Bust" = below 0.55 |
| **Z-Score** | `(Recent Metric − Career Avg) ÷ Career Std Dev` | Standard deviations from own norm. ±0.5+ = worth watching |

### Phase Definitions (T20 Cricket)

| Phase | Overs | Context |
|---|---|---|
| **Powerplay** | 1–6 | Fielding restrictions. Openers attack or build base |
| **Middle** | 7–15 | Consolidation and acceleration. Rotate strike + big hits |
| **Death** | 16–20 | Maximum aggression. Boundary hitting and finishing |

---

## End-to-End Architecture

```
Cricsheet.org                        Cricsheet.org
T20 JSON files (~5,100 matches)      people.csv (17,834 players)
        │                                    │
        ▼                                    ▼
        Unity Catalog Volume
        /Volumes/tabular/dataexpert/prateek_capstone_project/
        │
        ▼ 01_bronze_ingestion.py
        binaryFile reader → parse_match_json() → mapInPandas
        │
        ├── bronze_deliveries      (1 row per ball, ~1.2M rows)
        └── bronze_quality_log
        │
        ▼ 02_silver_enrichment.py
        Filter male T20 → Cast types → Phase labels → Ball flags
        → Batting position (Window fn) → Legal ball flags
        │
        ├── silver_deliveries      (enriched, Z-ORDERED by player)
        └── silver_quality_log
        │
        ▼ 03_gold_layer.py
        9 aggregated Gold tables + player registry join
        │
        ├── gold_batter_by_match   gold_bowler_by_match
        ├── gold_batter_career     gold_bowler_career
        ├── gold_batter_by_phase   gold_bowler_by_phase
        ├── gold_phase_momentum    gold_matchup
        └── gold_anomaly_feed
        │
        ▼ 04_agentic_ai.py
        gold_anomaly_feed → ai_query(Llama 3.3 70B) → OBSERVE → REASON → DECIDE → ACT
        │
        ├── anomaly_narratives     (AI text per player anomaly)
        ├── agent_alerts           (HIGH severity — immediate action)
        ├── agent_watchlist        (MEDIUM severity — monitor)
        └── agent_actions          (full audit log)
        │
        ▼ Databricks SQL Warehouse (b15d3d6f837ba428)
        databricks-sql-connector
        │
        ▼ Databricks Apps
        Dash application (app/app.py)
        ├── Page 1: Player Scout
        ├── Page 2: Anomaly Feed
        └── Page 3: Matchup Intel
```

---

## Capstone Requirements Checklist

| Requirement | Implementation | Status |
|---|---|---|
| **Pipeline** | 4-notebook medallion architecture (Bronze → Silver → Gold → Agentic AI) | ✅ |
| **Quality Controls** | DQ log tables at every layer (bronze/silver/gold), PASS/WARN/FAIL checks, append-mode history | ✅ |
| **Cloud Deployment** | Databricks Apps (live URL), Unity Catalog, Delta Lake, Serverless SQL Warehouse | ✅ |
| **Agentic Action** | `ai_query()` calls Llama 3.3 70B, generates narrative per anomaly, routes HIGH/MEDIUM/LOG, writes back to catalog | ✅ |
| **Multiple Data Sources** | Cricsheet JSON (ball-by-ball) + Cricsheet people.csv (player registry) joined in Gold layer | ✅ |
| **1M+ rows (batch)** | ~1.2M delivery rows in Bronze (728,461 after male T20 filter in Silver) | ✅ |

---

## Data Sources

### Source 1 — Cricsheet T20 Ball-by-Ball Data
- **Format:** JSON (1 file per match)
- **Volume:** ~5,100 files, ~1.2 million deliveries
- **Coverage:** All T20 matches (international + domestic) up to June 2024
- **URL:** https://cricsheet.org/downloads/

### Source 2 — Cricsheet Player Registry
- **Format:** CSV (17,834 players)
- **Content:** Cricsheet UUID → ESPN Cricinfo ID, Cricket Archive ID, Cricinfo URL
- **Used for:** Enriching career tables with external profile links
- **URL:** https://cricsheet.org/register/people.csv

---

## Key Technical Decisions

### Why binaryFile instead of spark.read.json()?
Cricsheet JSON files use player names as object keys in `info.registry.people`. Spark's case-insensitive schema inference creates a `COLUMN_ALREADY_EXISTS` error when names like `"N D'Souza"` and `"N D'souza"` appear across files. Reading as binary and parsing via `mapInPandas` sidesteps this entirely.

### Why mapInPandas instead of RDDs?
Databricks serverless clusters do not support RDDs. `mapInPandas` provides equivalent distributed Python execution with the full Pandas API and is the serverless-recommended approach.

### Stat Logic — Why These Formulas?
| Metric | Formula | Why This Way |
|---|---|---|
| Strike Rate | `(runs / legal_balls) × 100` | Wides don't count as balls faced — using total balls would deflate the rate unfairly |
| Batting Average | `runs / dismissals` | Divides by dismissals not innings — not-outs would deflate averages if innings was the denominator |
| Dot Ball % | `dot_balls / legal_balls × 100` | Excludes wides from denominator — a wide isn't a dot from the batter's perspective |
| Economy | `(runs_for_bowler / legal_balls) × 6` | Excludes byes/legbyes — ICC official formula, these are the keeper's fault not the bowler's |
| Z-Score | `(recent_metric − career_avg) / career_stddev` | Real stddev from match-level data gives a personal benchmark, not an arbitrary threshold |
| Dismissals | Only counts innings where `player_out = batter` | Excludes non-striker run-outs (which appear on another batter's delivery in raw data) |

### 3-Tier Auth Pattern (Databricks Apps)
The app supports three authentication methods in priority order:
1. `DATABRICKS_TOKEN` environment variable (CI/CD, local dev)
2. Databricks CLI profile (`dbc-7b106152-caf3`)
3. `WorkspaceClient().config.authenticate()` — M2M OAuth when running inside Databricks Apps

---

## Repository Structure

```
capstone_project/
├── notebooks/
│   ├── 01_bronze_ingestion.py      # Raw JSON → bronze_deliveries (1 row per ball)
│   ├── 02_silver_enrichment.py     # Phase labels, batting position, legal ball flags
│   ├── 03_gold_layer.py            # 9 Gold tables + registry join + bug-fixed metrics
│   ├── 04_agentic_ai.py            # Llama 3.3 70B narrative generation + agent routing
│   ├── 05_aibi_dashboard.py        # Databricks AI/BI dashboard definition
│   └── 06_sql_alerts.py            # SQL alert thresholds
├── app/
│   ├── app.py                      # Dash app — 3 pages, 3-tier auth, SQL-safe queries
│   ├── app.yaml                    # Databricks Apps deployment config
│   ├── requirements.txt            # Pinned Python dependencies
│   └── assets/
│       └── style.css               # Clean Inter font theme
├── people.csv                      # Cricsheet player registry (Source 2)
└── README.md
```

---

## Gold Tables Reference

| Table | Grain | Rows (approx) | Used By |
|---|---|---|---|
| `gold_batter_by_match` | batter × match | ~85,000 | Player Scout trend chart, highest score, 100s/50s |
| `gold_batter_career` | batter | ~1,500 | Player Scout career stats, impact score |
| `gold_bowler_by_match` | bowler × match | ~70,000 | Player Scout economy trend |
| `gold_bowler_career` | bowler | ~1,200 | Player Scout bowling stats |
| `gold_batter_by_phase` | batter × phase | ~4,000 | Player Scout phase breakdown |
| `gold_bowler_by_phase` | bowler × phase | ~3,500 | Player Scout bowling phase breakdown |
| `gold_phase_momentum` | batter × phase × season | ~25,000 | Seasonal phase trend |
| `gold_matchup` | batter × bowler | ~45,000 | Matchup Intel H2H, dangermen table |
| `gold_anomaly_feed` | player | ~518 | Anomaly Feed (pre-AI) |
| `anomaly_narratives` | player | ~518 | Anomaly Feed (with AI text) |

All tables prefixed: `tabular.dataexpert.prateek_capstone_project_`

---

## What This Platform Has That ESPN Cricinfo Doesn't (For Free)

| Feature | ESPN Cricinfo | This Platform |
|---|---|---|
| Phase-level SR breakdown | Buried in scorecards | Instant visual per player per phase |
| Form anomaly detection | None | Z-score vs personal career baseline |
| Form trajectory label | None | Rising ↑ / Stable → / Declining ↓ with numeric delta |
| AI-generated narratives | None | Per-player LLM analysis with tactical recommendation |
| Composite impact score | None | Weighted 0–99 score (SR + Avg + Boundary + Dot) |
| Consistency score | None | Coefficient of variation — "Boom or Bust" vs "Very Consistent" |
| Batter vs bowler H2H | Basic | Statistical significance filter (min 6 balls), live AI tactical brief |
| Dangermen alerts | None | Top bowlers ranked by dismissal rate vs selected batter |

---

## Setup — Run It Yourself

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Serverless compute access
- Volume created at `/Volumes/tabular/dataexpert/prateek_capstone_project/`

### 1. Upload Data
```
Upload all Cricsheet T20 JSON files + people.csv to the Volume above
```

### 2. Run Notebooks in Order
```
notebooks/01_bronze_ingestion.py    # ~10 min
notebooks/02_silver_enrichment.py   # ~5 min
notebooks/03_gold_layer.py          # ~8 min
notebooks/04_agentic_ai.py          # ~5 min (calls LLM — needs Model Serving access)
```

### 3. Deploy the App
```bash
# Upload app folder to Databricks workspace
databricks workspace import-dir app/ /Workspace/Users/<you>/capstone_app

# Deploy to Databricks Apps
databricks apps deploy cricket-intelligence-platform \
  --source-code-path /Workspace/Users/<you>/capstone_app
```

### 4. App Dependencies
```
dash==2.18.1
dash-bootstrap-components==1.6.0
plotly==5.24.1
pandas==2.2.3
databricks-sql-connector==3.4.0
databricks-sdk==0.37.0
```

---

## Data Notes

- **Coverage:** All T20 formats (T20I, IPL, BBL, PSL, CPL, etc.) via Cricsheet
- **Cutoff:** June 2024 (ICC T20 World Cup)
- **Verification:** Stats cross-checked against ESPN Cricinfo T20I profiles
  - Rohit Sharma SR: App 140.4 vs ESPN 140.85 ✅
  - Babar Azam SR: App 128.36 vs ESPN 128.02 ✅
  - Virat Kohli Avg: App 48.57 vs ESPN 48.70 ✅
- **Known gap:** Kohli's 122* vs Afghanistan (Sep 2022) is missing from Cricsheet extract — explains 0 centuries in our data vs 1 on ESPN
