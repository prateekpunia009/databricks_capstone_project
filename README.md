# Cricket Player Performance Intelligence Platform
**Databricks Bootcamp Capstone Project — Zach Wilson 2026**

> *"ESPN Cricinfo tells you what happened. This platform tells you what it means — and flags when something has changed before anyone else notices."*

---

## Project Overview

A full-stack data engineering and AI platform that ingests 5,100+ T20 international cricket match files, processes ~1.2 million ball-by-ball delivery records through a medallion architecture, and surfaces real-time player performance intelligence via an interactive Streamlit dashboard.

The platform goes beyond traditional cricket statistics by applying Z-score anomaly detection and Databricks AI-powered narrative generation to identify statistically significant changes in player form before they become obvious.

---

## End-to-End Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│                                                                     │
│  Cricsheet.org                      Cricsheet.org                   │
│  T20 JSON files                     people.csv                      │
│  (~5,100 matches)                   (17,834 players)                │
│  [Ball-by-ball data]                [Player registry]               │
└──────────────┬──────────────────────────────┬───────────────────────┘
               │                              │
               ▼                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       STORAGE LAYER                                 │
│                                                                     │
│              Unity Catalog Volume (Databricks)                      │
│       /Volumes/tabular/dataexpert/prateek_capstone_project/         │
│                                                                     │
│       ├── *.json  (5,100 match files)                               │
│       └── people.csv                                                │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              COMPUTE LAYER  (Databricks Serverless)                 │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │  INGESTION — 01_bronze_ingestion.py                          │  │
│  │  binaryFile reader → mapInPandas → 1 row per delivery        │  │
│  │  Output: bronze_deliveries + bronze_player_registry          │  │
│  │  DQ log: bronze_quality_log                                  │  │
│  └───────────────────────────┬──────────────────────────────────┘  │
│                              │                                      │
│  ┌───────────────────────────▼──────────────────────────────────┐  │
│  │  ENRICHMENT — 02_silver_enrichment.py                        │  │
│  │  Filter → Cast → Phase labels → Ball flags → Window fns      │  │
│  │  Output: silver_deliveries (Z-ORDERED)                       │  │
│  │  DQ log: silver_quality_log                                  │  │
│  └───────────────────────────┬──────────────────────────────────┘  │
│                              │                                      │
│  ┌───────────────────────────▼──────────────────────────────────┐  │
│  │  AGGREGATION — 03_gold_layer.py                              │  │
│  │  9 Gold tables + people.csv registry join                    │  │
│  │  career / by_match / by_phase / matchup / anomaly_feed       │  │
│  │  DQ log: gold_quality_log                                    │  │
│  └───────────────────────────┬──────────────────────────────────┘  │
│                              │                                      │
│  ┌───────────────────────────▼──────────────────────────────────┐  │
│  │  AGENTIC AI — 04_agentic_ai.py                               │  │
│  │  gold_anomaly_feed → ai_query(Llama 3.3 70B)                 │  │
│  │  → generates analyst narratives → gold_anomaly_narratives    │  │
│  └──────────────────────────────────────────────────────────────┘  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SERVING LAYER  (Unity Catalog)                   │
│                                                                     │
│  Delta Lake Tables — tabular.dataexpert.prateek_capstone_project_*  │
│                                                                     │
│  bronze_deliveries          silver_deliveries                       │
│  bronze_player_registry     gold_batter_career                      │
│  gold_batter_by_match       gold_bowler_career                      │
│  gold_batter_by_phase       gold_bowler_by_match                    │
│  gold_bowler_by_phase       gold_phase_momentum                     │
│  gold_matchup               gold_anomaly_feed                       │
│  gold_anomaly_narratives                                            │
│                                                                     │
│  bronze_quality_log   silver_quality_log   gold_quality_log         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    CONNECTION LAYER                                 │
│                                                                     │
│            Databricks SQL Warehouse                                 │
│            databricks-sql-connector (Python)                        │
│            HTTP Path + Personal Access Token                        │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   PRESENTATION LAYER                                │
│                                                                     │
│            Streamlit Dashboard  (app/streamlit_app.py)              │
│                                                                     │
│  Tab 1: Player Story          Tab 2: Phase Momentum                 │
│  • Scan card + photo          • SR by phase bar chart               │
│  • Impact badge (0–99)        • Phase radar chart                   │
│  • Career stats grid          • Best / worst phase badge            │
│  • Form trend line            • Season arc (phase_momentum)         │
│  • Trajectory badge                                                 │
│                                                                     │
│  Tab 3: Matchup Intel         Tab 4: Anomaly Feed                   │
│  • Head-to-head table         • Z-score flagged players             │
│  • Danger bowlers             • AI narrative per anomaly            │
│  • Favourite targets          • Severity: HIGH / MEDIUM / LOW       │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
                         END USER
                   (Coach / Analyst / Fan)
```

---

## Capstone Requirements Checklist

| Requirement | Implementation | Status |
|---|---|---|
| **Pipeline** | 4-notebook medallion architecture (Bronze → Silver → Gold → Agentic) | ✅ |
| **Quality Controls** | DQ log tables at every layer with PASS/WARN/FAIL checks, append-mode history | ✅ |
| **Cloud Deployment** | Databricks serverless compute, Unity Catalog, Delta Lake | ✅ |
| **Agentic Action** | `ai_query()` calls Llama 3.3 70B to generate analyst narratives per anomaly, writes back to catalog | ✅ |
| **Multiple Data Sources** | Cricsheet JSON (ball-by-ball) + Cricsheet people.csv (player registry) joined in Gold | ✅ |
| **1M+ rows (batch)** | ~1.2M delivery rows in Bronze | ✅ |

---

## Data Sources

### Source 1 — Cricsheet T20 Ball-by-Ball Data
- **Format:** JSON (1 file per match)
- **Volume:** ~5,100 files, ~1.2 million deliveries
- **Coverage:** All T20 international matches
- **Location:** `/Volumes/tabular/dataexpert/prateek_capstone_project/`
- **URL:** https://cricsheet.org/downloads/

### Source 2 — Cricsheet Player Registry
- **Format:** CSV
- **Volume:** 17,834 players
- **Content:** Player identity cross-reference (Cricsheet UUID → ESPN Cricinfo ID, BCCI ID, Cricket Archive ID)
- **Location:** `/Volumes/tabular/dataexpert/prateek_capstone_project/people.csv`
- **URL:** https://cricsheet.org/register/people.csv
- **Used for:** Joining `key_cricinfo` and `cricinfo_url` onto career tables

---

## Key Technical Decisions

### Why binaryFile instead of spark.read.json()?
Cricsheet JSON files use player names as object keys in `info.registry.people`. Spark's case-insensitive schema inference creates a `COLUMN_ALREADY_EXISTS` error when names like `"N D'Souza"` and `"N D'souza"` appear across files. Reading as binary and parsing in Python with `json.loads()` sidesteps this entirely.

### Why mapInPandas instead of RDDs?
Databricks serverless clusters do not support RDDs. `mapInPandas` provides equivalent distributed Python execution with full Pandas API support and is the serverless-recommended approach.

### Why no SCD (Slowly Changing Dimensions)?
All analytical tables are fact tables (one row per delivery/match/player-phase). Player identity is tracked via immutable Cricsheet UUIDs. All derived attributes (career SR, phase performance) are computed dynamically from the Silver fact table rather than stored as slowly-changing dimension attributes.

### Stat Logic
| Metric | Formula | Why |
|---|---|---|
| Strike Rate | `(runs / legal_balls) * 100` | Wides don't count as balls faced |
| Batting Average | `runs / dismissals` | Divides by dismissals not innings — not-outs inflate averages otherwise |
| Dot % | `dot_balls / legal_balls * 100` | Excludes wides from denominator |
| Economy | `(runs_for_bowler / legal_balls) * 6` | Excludes byes/legbyes — ICC official formula |
| Z-score | `(recent_metric - career_avg) / career_stddev` | Real stddev from match-level data |
| Impact Score | Weighted composite: SR (35%) + Avg (30%) + Boundary% (20%) + Dot avoidance (15%) | Single number for player ranking |

---

## Repository Structure

```
capstone_project/
├── notebooks/
│   ├── 01_bronze_ingestion.py       # Raw JSON → bronze_deliveries + bronze_player_registry
│   ├── 02_silver_enrichment.py      # Enrichment, flags, phase labels
│   ├── 03_gold_layer.py             # 9 aggregated Gold tables + registry join
│   └── 04_agentic_ai.py             # ai_query() narrative generation
├── app/
│   ├── streamlit_app.py             # Production dashboard (reads Gold tables)
│   ├── preview_app.py               # Local preview with mock data
│   ├── requirements.txt
│   └── assets/                      # Player photos
├── people.csv                       # Cricsheet player registry (Source 2)
└── README.md
```

---

## Gold Tables Reference

| Table | Grain | Rows (approx) | Used By |
|---|---|---|---|
| `gold_batter_by_match` | batter × match | ~85,000 | Player Story form trend |
| `gold_batter_career` | batter | ~3,000 | Player Story scan card, impact badge |
| `gold_bowler_by_match` | bowler × match | ~70,000 | Bowler form trend |
| `gold_bowler_career` | bowler | ~2,500 | Bowler profile card |
| `gold_batter_by_phase` | batter × phase | ~7,000 | Phase Momentum tab |
| `gold_bowler_by_phase` | bowler × phase | ~6,000 | Phase Momentum (bowling) |
| `gold_phase_momentum` | batter × phase × season | ~25,000 | Career arc chart |
| `gold_matchup` | batter × bowler | ~180,000 | Matchup Intel tab |
| `gold_anomaly_feed` | player (batter + bowler) | ~5,500 | Anomaly Feed tab |

All tables prefixed: `tabular.dataexpert.prateek_capstone_project_`

---

## Dashboard Tabs

### Tab 1 — Player Story
Answers: *Who is this player and what does their form look like right now?*
- Sci-fi scan card with player photo
- Hexagon impact badge (composite score 0–99)
- Career stat grid: matches, runs, SR, average, highest score
- Rolling 10-match form trend vs career baseline
- Trajectory badge: Rising ↑ / Stable → / Declining ↓

### Tab 2 — Phase Momentum
Answers: *Where in the innings does this player win or lose the game?*
- Strike rate by phase (Powerplay / Middle / Death)
- Phase radar chart: SR + Boundary% + Dot%
- Best and worst phase badges

### Tab 3 — Matchup Intel
Answers: *Who does this player dominate and who owns them?*
- Batter vs bowler head-to-head stats
- Danger bowlers (lowest batter SR against)
- Favourite targets (bowler dominance)

### Tab 4 — Anomaly Feed
Answers: *Is something unusual happening right now?*
- Z-score flagged anomalies for batters and bowlers
- AI-generated narrative per anomaly (Llama 3.3 70B via Databricks ai_query)
- Severity classification: HIGH / MEDIUM / LOW

---

## What This Platform Has That ESPN Cricinfo Doesn't

| Feature | ESPN Cricinfo | This Platform |
|---|---|---|
| Phase-level SR breakdown | Buried in scorecards | Interactive visual per player |
| Anomaly detection | None | Z-score vs career baseline |
| Form trajectory label | None | Rising ↑ / Stable → / Declining ↓ with delta |
| AI-generated narratives | None | Per-anomaly LLM analysis |
| Composite impact score | None | Weighted 0–99 score |
| Consistency score | None | Coefficient of variation metric |
| Matchup with min-ball filter | Basic | Statistical significance filter |

---

## Setup

### Prerequisites
- Databricks workspace with Unity Catalog
- Serverless compute cluster
- Volume: `/Volumes/tabular/dataexpert/prateek_capstone_project/`

### Run Order
```
1. Upload Cricsheet JSON files + people.csv to Volume
2. Run 01_bronze_ingestion.py
3. Run 02_silver_enrichment.py
4. Run 03_gold_layer.py
5. Run 04_agentic_ai.py
6. Launch app/streamlit_app.py
```

### Dashboard Dependencies
```
pip install streamlit plotly pandas databricks-sql-connector pillow requests
```
