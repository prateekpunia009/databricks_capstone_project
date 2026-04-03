# System Design: Cricket Intelligence Platform

A T20 cricket analytics platform built on Databricks. Ingests 5,100 raw match files, processes them through a medallion architecture, runs anomaly detection and LLM-powered analysis, and serves results via a 3-page interactive dashboard.

Live app: https://cricket-intelligence-platform-1352785079224954.aws.databricksapps.com

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│  Cricsheet T20I JSONs (~5,100 files)   people.csv (player registry) │
│  Unity Catalog Volume: /Volumes/tabular/dataexpert/prateek_.../     │
└────────────────────────┬────────────────────────────────────────────┘
                         │ 01_bronze_ingestion.py
                         │ Spark binaryFile + mapInPandas
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER   (Delta Lake, Unity Catalog)                         │
│  bronze_deliveries      1.2M rows  1 row per delivery               │
│  bronze_quality_log     DQ checks appended each run                 │
│  Partition: gender, season                                          │
└────────────────────────┬────────────────────────────────────────────┘
                         │ 02_silver_enrichment.py
                         │ Filter male T20 + 15 derived columns
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER   (Delta Lake, Unity Catalog)                         │
│  silver_deliveries      728K rows  male T20 only                    │
│  Z-ORDER by (batter_id, bowler_id, match_date)                      │
│  Phase labels  Ball flags  Batting position (Window fn)             │
└────────────────────────┬────────────────────────────────────────────┘
                         │ 03_gold_layer.py
                         │ 9 aggregation tables  Z-score computation
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER   (Delta Lake, Unity Catalog)                           │
│                                                                     │
│  gold_batter_career      ~1.5K rows  Career SR, avg, impact score   │
│  gold_batter_by_match    ~85K rows   Match-level batter stats       │
│  gold_batter_by_phase     ~4K rows   Powerplay / Middle / Death     │
│  gold_bowler_career      ~1.2K rows  Economy, avg, bowling SR       │
│  gold_bowler_by_match    ~70K rows   Match-level bowler stats       │
│  gold_bowler_by_phase     ~3.5K rows Phase-specific economy         │
│  gold_matchup            ~45K rows   H2H (batter vs bowler, min 6b) │
│  gold_anomaly_feed         ~500 rows  Z-score flagged anomalies     │
│  gold_phase_momentum     ~25K rows   Seasonal form by phase         │
└────────────────────────┬────────────────────────────────────────────┘
                         │ 04_agentic_ai.py
                         │ Llama 3.3 70B via ai_query()
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  AI / AGENTIC LAYER   (Databricks AI Functions)                     │
│                                                                     │
│  OBSERVE  →  REASON  →  DECIDE  →  ACT  →  LOG                    │
│                                                                     │
│  anomaly_narratives      518 rows  LLM insight + recommendation     │
│  agent_alerts            HIGH severity  ( Z-score >= 2.5 )         │
│  agent_watchlist         MEDIUM severity ( Z-score >= 1.5 )        │
│  agent_actions           Full audit log of every LLM decision       │
└────────────────────────┬────────────────────────────────────────────┘
                         │ databricks-sql-connector
                         │ 3-tier auth (Token → CLI → OAuth)
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│  DASH APPLICATION   (Python, Databricks Apps)                       │
│                                                                     │
│  /           Player Scout    individual stats + AI verdict          │
│  /anomaly    Hot Signals     anomaly feed with LLM narratives       │
│  /matchup    Matchup Intel   H2H analysis + AI tactical brief       │
│                                                                     │
│  Dual-mode: live SQL Warehouse  OR  static CSV fallback             │
└────────────────────────┬────────────────────────────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          ▼                             ▼
  Databricks Apps                 Render.com (free tier)
  workspace auth                  static CSV mode
  live SQL warehouse              no Databricks token needed
```

---

## Component Breakdown

| Component | Technology | Purpose | Scale |
|---|---|---|---|
| Raw Storage | Unity Catalog Volume | Store 5.1K Cricsheet JSON files | ~2 GB |
| Ingestion | Spark `binaryFile` + `mapInPandas` | Parse nested JSON, avoid key-casing collision | 1.2M deliveries |
| Bronze Store | Delta Lake (Unity Catalog) | Immutable raw facts + append-mode DQ log | 1.2M rows |
| Enrichment | Spark SQL + Window functions | Phase labels, batting position, 15 derived columns | 728K rows |
| Silver Store | Delta Lake (Z-ordered) | Fast player + date range lookups | 728K rows |
| Gold Analytics | Spark SQL aggregations | 9 purpose-built analytics tables | ~230K total rows |
| Anomaly Detection | Z-score (Spark/Python) | Flag players deviating from their personal baseline | 518 anomalies |
| AI Narrative | Llama 3.3 70B via `ai_query()` | Plain-English insight + recommended action per anomaly | 518 narratives |
| Agentic Loop | Custom Python notebook | Observe → Reason → Decide → Act → Log per anomaly | Fully audited |
| Query Engine | Databricks SQL Warehouse | Sub-second queries from the frontend app | Serverless |
| Frontend | Dash (Python) + Plotly + Bootstrap | 3-page interactive dashboard | ~20 queries/session |
| Primary Hosting | Databricks Apps | Auto-scaling cloud deployment | Serverless |
| Fallback Hosting | Render.com (free tier) | Public access without Databricks login | Static CSV mode |
| Auth | 3-tier Token → CLI → OAuth | Works in CI/CD, local dev, and production | No single point of failure |
| SQL Safety | `_safe()` quote-escaping | Prevent SQL injection from user dropdown inputs | All user inputs |

---

## Data Pipeline

### Bronze: Raw Ingestion (`01_bronze_ingestion.py`)

**Input:** 5,100 Cricsheet T20 JSON files from Unity Catalog Volume.

**Key choices:**
- Uses `binaryFile` reader instead of `spark.read.json()` to avoid schema inference failures caused by key-casing collisions in Cricsheet files
- Custom `parse_match_json()` function denormalizes nested JSON (match metadata + all deliveries) into flat rows
- `mapInPandas` for distributed parsing — serverless-compatible alternative to RDDs

**Output:** `bronze_deliveries` (1.2M rows, 1 per delivery), partitioned by gender + season

**Columns:** match_id, match_date, season, venue, team_home, team_away, batter, bowler, runs_batter, runs_total, extras (wides/noballs/byes/legbyes), is_wicket, wicket_kind, over, ball

**Data quality checks:** null batters/bowlers, invalid over numbers, runs > 36, innings completeness

---

### Silver: Enrichment (`02_silver_enrichment.py`)

**Input:** `bronze_deliveries`

**Filtering:** Male T20 matches only, excludes super overs (~728K rows remain)

**Derived columns added:**

| Column | Logic |
|---|---|
| `phase` | Powerplay (overs 0-5), Middle (6-14), Death (15-19) |
| `batting_position` | `dense_rank()` over (match_id, innings, team) ordered by ball |
| `is_legal` | Not wide and not noball |
| `is_dot_ball` | `is_legal AND runs_total == 0` |
| `is_four / is_six` | `runs_batter IN (4, 6)` |
| `runs_for_bowler` | `runs_total - extras_byes - extras_legbyes` (ICC standard) |
| `is_bowler_wicket` | `is_wicket AND wicket_kind NOT IN ('run out', 'obstructing')` |

**Optimization:** Z-ORDER by (batter_id, bowler_id, match_date) minimizes files scanned for the most common query pattern (one player's history).

---

### Gold: Aggregations (`03_gold_layer.py`)

Nine tables built in dependency order. All metrics follow ICC standards:

| Metric | Formula |
|---|---|
| Strike Rate | `(runs / legal_balls) * 100` |
| Batting Average | `runs / dismissals` (not innings — preserves not-outs) |
| Economy | `(runs_for_bowler / legal_balls) * 6` (excludes byes + legbyes) |
| Bowling SR | `legal_balls / wickets` |
| Dot Ball % | `dot_balls / legal_balls * 100` |
| Impact Score | `SR * 0.35 + Avg * 0.30 + Boundary% * 0.20 + DotAvoidance * 0.15` (normalized 0-99) |
| Z-Score | `(recent_metric - career_avg) / career_stddev` using last 10 matches as "recent" |

**Thresholds:** min 10 innings for batter career inclusion, min 60 balls for bowler career inclusion, min 6 balls for H2H matchup.

---

## AI and Agentic Layer (`04_agentic_ai.py`)

### Anomaly Detection

Z-scores are computed against each player's own career baseline, not a league-wide average. This matters because a Strike Rate of 90 is normal for a lower-order finisher but alarming for an opener — personal baseline makes the signal meaningful.

Anomaly types flagged:
- SR Spike / SR Drop (recent SR deviates by 1.5+ standard deviations)
- Dot Spike / Dot Drop (dot ball % deviates by 1.5+ standard deviations)

### Agentic Loop

The agent processes all 518 flagged anomalies through a 5-step loop:

```
1. OBSERVE  — reads gold_anomaly_feed (player, role, Z-score, severity, metric delta)
2. REASON   — calls Llama 3.3 70B with structured cricket context prompt
3. DECIDE   — routes by Z-score severity:
               |Z| >= 2.5 or HIGH   → ALERT (agent_alerts table)
               |Z| >= 1.5 or MEDIUM → WATCHLIST (agent_watchlist table)
               else                  → LOG only
4. ACT      — writes parsed LLM output to anomaly_narratives
5. LOG      — records every decision with reasoning in agent_actions
```

LLM prompt format produces two structured fields:
```
INSIGHT: [2-3 sentences explaining what is happening in cricket terms]
RECOMMENDED ACTION: [One specific, prescriptive tactical recommendation]
```

Each narrative is stored in `anomaly_narratives` and surfaced on the Hot Signals page and Player Scout verdict box.

---

## Frontend Application (`app/app.py`)

### Three Pages

**Player Scout (`/`)** — Look up any T20 player. Loads Rohit Sharma by default.
- 8-tile career stats grid with hover tooltips explaining each metric
- Phase Breakdown: Powerplay / Middle / Death performance cards
- AI Analyst verdict box (color-coded: green/amber/red based on Z-score)
- Match-by-match Strike Rate chart with career baseline + form bands
- Supports both batters and bowlers via toggle

**Hot Signals (`/anomaly`)** — Players whose form has diverged from their baseline.
- Filter by: signal type (hot streak / form slump / all), player type, deviation threshold
- Each card shows career vs recent metric, Z-score, and the full LLM recommendation
- Hot streak cards pulse green; form slump cards have red border

**Matchup Intel (`/matchup`)** — Head-to-head battle between a specific batter and bowler.
- Requires minimum 6 balls faced in history
- Fight-poster header: batter name (blue) | VS | bowler name (gold)
- 8 H2H stats tiles + AI tactical brief + dangermen table (top 6 bowlers vs that batter)

### Dual-Mode Data Loading

```
DATABRICKS_TOKEN present  →  Live SQL Warehouse (real-time queries)
No token + app/data/ CSVs →  Static CSV mode via SQLite in-memory (data_loader.py)
USE_STATIC_DATA=1 env var →  Force static mode
```

Static mode enables free Render.com deployment with no Databricks dependency at runtime.

### Callbacks

All pages use Dash callbacks. Key patterns:

| Callback | Trigger | Output |
|---|---|---|
| `load_scout_options` | URL change to `/` | Player dropdown options, default = Rohit Sharma |
| `render_scout` | Player dropdown value | Full player card (6 parallel SQL queries) |
| `render_anomaly` | Signal/etype/threshold filters | Filtered anomaly cards feed |
| `render_matchup` | Both batter + bowler selected | H2H stats + AI brief |

---

## Authentication and Security

### 3-Tier Auth

The app tries three auth methods in order, using the first that succeeds:

```
1. DATABRICKS_TOKEN env var   → Used in CI/CD, local dev, GitHub Actions
2. Databricks CLI             → databricks auth token --profile <profile>
3. WorkspaceClient OAuth      → WorkspaceClient().config.authenticate()
                                Used automatically inside Databricks Apps
```

This means the same code works without modification in all three environments.

### SQL Injection Prevention

All player name inputs from dropdowns are passed through `_safe()` before embedding in SQL:

```python
def _safe(name: str) -> str:
    return str(name).replace("'", "''")   # SQL standard escaping, not backslash
```

Used in every WHERE clause:
```sql
WHERE LOWER(batter) = LOWER('{safe_name}')
```

Column and table names are never constructed from user input — only hard-coded mappings are used.

### Connection Management

A single thread-safe connection is reused across all queries with automatic reconnection on failure:

```python
_lock = threading.Lock()
_conn = None   # singleton, initialized once per process
```

---

## Deployment

### Primary: Databricks Apps

```
Source: /Workspace/Users/prateekpunia009@gmail.com/capstone_project/app/
Config: app/app.yaml
Start:  python app.py
Auth:   WorkspaceClient OAuth (automatic inside Databricks Apps)
```

Deploy command:
```bash
databricks apps deploy cricket-intelligence-platform \
  --source-code-path /Workspace/Users/.../capstone_project/app
```

### Fallback: Render.com (Free, Token-Free)

For public access without a Databricks workspace login:

1. Run `notebooks/07_export_gold_to_csv.py` in Databricks to export all 8 gold tables to CSV
2. Place CSVs in `app/data/`
3. Deploy to Render.com with env var `USE_STATIC_DATA=1`
4. No token, no Databricks connection — app reads from bundled CSV files via SQLite

---

## Key Design Decisions

| Decision | Why |
|---|---|
| `binaryFile` reader instead of `spark.read.json()` | Cricsheet JSONs have key-casing collisions on player name fields; binaryFile avoids schema inference failures entirely |
| `mapInPandas` for delivery extraction | Serverless clusters do not support RDDs; mapInPandas gives full Python flexibility with Spark distribution |
| Dismissal-based batting average | Excludes not-outs, matching ICC standard; innings-based average unfairly deflates averages for not-out innings |
| `runs_for_bowler` excludes byes + legbyes | ICC standard: fielding errors, not the bowler's fault; including them would inflate economy rates |
| Z-score vs personal baseline, not league-wide | A 90 SR is normal for a finisher but alarming for an opener; personal baseline makes anomalies contextually meaningful |
| Agentic routing instead of batch LLM | Produces three different output tables based on severity; creates a full observable audit trail of every LLM decision |
| 3-tier auth fallback chain | App works identically in CI/CD, local dev, and Databricks Apps production without any code changes |
| Static CSV fallback with SQLite | Decouples the frontend from live data infrastructure; enables free public hosting with zero Databricks dependency |
| Z-ORDER on (batter_id, bowler_id, match_date) | The overwhelming majority of queries filter by player + date; Z-ordering collocates that data to minimize file scans |

---

## Repository Structure

```
capstone_project/
├── app/
│   ├── app.py                  Dash application (3 pages, all callbacks)
│   ├── data_loader.py          Static CSV query engine (SQLite fallback)
│   ├── app.yaml                Databricks Apps config
│   ├── requirements.txt        Python dependencies
│   ├── assets/
│   │   └── style.css           Dark theme, animations, responsive layout
│   └── data/
│       └── *.csv               Gold table exports (gitignored, for Render hosting)
├── notebooks/
│   ├── 01_bronze_ingestion.py  Raw JSON parsing
│   ├── 02_silver_enrichment.py T20 filtering + feature engineering
│   ├── 03_gold_layer.py        9 aggregation tables + Z-score anomaly detection
│   ├── 04_agentic_ai.py        LLM agentic loop + narrative generation
│   └── 07_export_gold_to_csv.py Export gold tables for static hosting
├── README.md
└── SYSTEM_DESIGN.md            This document
```

---

## Data Source

All match data is sourced from [Cricsheet](https://cricsheet.org) (T20I format, male matches only). Coverage includes most international matches but may have gaps of 5-15 matches per player compared to official records. Rate statistics (SR, economy, averages) match ESPN Cricinfo within 1-2%.
