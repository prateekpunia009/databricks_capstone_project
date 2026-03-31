# Databricks notebook source
# MAGIC %md
# MAGIC # Cricket Intelligence Platform — AI/BI Dashboard Deployment
# MAGIC
# MAGIC Deploys a native Databricks AI/BI Dashboard with 4 pages:
# MAGIC - **Overview**       : KPIs + season filter
# MAGIC - **Batters**        : Career table + phase strike-rate chart + top-10 bar
# MAGIC - **Bowlers**        : Career table + economy chart + wickets bar
# MAGIC - **AI Insights**    : Anomaly feed + agent alerts + recommended actions

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard

w = WorkspaceClient()

PREFIX   = "tabular.dataexpert.prateek_capstone_project"
USER     = w.current_user.me().user_name          # e.g. prateek@example.com
DASH_DIR = f"/Workspace/Users/{USER}/dashboards"

print(f"Deploying dashboard to: {DASH_DIR}")

# COMMAND ----------

# MAGIC %md ## Dashboard Definition

# COMMAND ----------

dashboard = {
    "datasets": [

        # ── 1. Summary KPIs ──────────────────────────────────────────────────
        {
            "name": "ds_summary",
            "displayName": "Summary KPIs",
            "queryLines": [
                f"SELECT COUNT(DISTINCT batter_id) AS total_batters, ",
                f"       SUM(total_matches)         AS total_match_appearances, ",
                f"       ROUND(AVG(strike_rate), 1) AS avg_strike_rate, ",
                f"       ROUND(AVG(average), 1)     AS avg_batting_avg ",
                f"FROM {PREFIX}_batter_career "
            ]
        },

        # ── 2. Batter Career Table ───────────────────────────────────────────
        {
            "name": "ds_batter_career",
            "displayName": "Batter Career Stats",
            "queryLines": [
                f"SELECT batter_name, total_matches, total_innings, runs, ",
                f"       ROUND(strike_rate, 1)     AS strike_rate, ",
                f"       ROUND(average, 1)         AS average, ",
                f"       boundaries, sixes, ",
                f"       ROUND(impact_score, 1)    AS impact_score, ",
                f"       consistency_label, ",
                f"       form_trajectory ",
                f"FROM {PREFIX}_batter_career ",
                f"ORDER BY impact_score DESC ",
                f"LIMIT 50 "
            ]
        },

        # ── 3. Batter Phase Strike Rate ──────────────────────────────────────
        {
            "name": "ds_batter_phase",
            "displayName": "Batter Phase Stats",
            "queryLines": [
                f"SELECT phase, ",
                f"       ROUND(AVG(phase_strike_rate), 1) AS avg_phase_sr, ",
                f"       ROUND(AVG(phase_runs), 1)        AS avg_phase_runs ",
                f"FROM {PREFIX}_batter_by_phase ",
                f"WHERE phase IN ('Powerplay', 'Middle', 'Death') ",
                f"GROUP BY phase ",
                f"ORDER BY ",
                f"  CASE phase WHEN 'Powerplay' THEN 1 WHEN 'Middle' THEN 2 ELSE 3 END "
            ]
        },

        # ── 4. Top 10 Batters by Runs ────────────────────────────────────────
        {
            "name": "ds_top_batters",
            "displayName": "Top 10 Batters",
            "queryLines": [
                f"SELECT batter_name, runs, ",
                f"       ROUND(strike_rate, 1) AS strike_rate ",
                f"FROM {PREFIX}_batter_career ",
                f"ORDER BY runs DESC ",
                f"LIMIT 10 "
            ]
        },

        # ── 5. Bowler Summary KPIs ───────────────────────────────────────────
        {
            "name": "ds_bowler_summary",
            "displayName": "Bowler Summary",
            "queryLines": [
                f"SELECT COUNT(DISTINCT bowler_id) AS total_bowlers, ",
                f"       SUM(wickets)              AS total_wickets, ",
                f"       ROUND(AVG(economy), 2)    AS avg_economy, ",
                f"       ROUND(AVG(bowling_avg), 1) AS avg_bowling_avg ",
                f"FROM {PREFIX}_bowler_career "
            ]
        },

        # ── 6. Bowler Career Table ───────────────────────────────────────────
        {
            "name": "ds_bowler_career",
            "displayName": "Bowler Career Stats",
            "queryLines": [
                f"SELECT bowler_name, total_matches, wickets, ",
                f"       ROUND(economy, 2)      AS economy, ",
                f"       ROUND(bowling_avg, 1)  AS bowling_avg, ",
                f"       ROUND(bowling_sr, 1)   AS bowling_sr, ",
                f"       ROUND(dot_pct * 100, 1) AS dot_ball_pct ",
                f"FROM {PREFIX}_bowler_career ",
                f"ORDER BY wickets DESC ",
                f"LIMIT 50 "
            ]
        },

        # ── 7. Top 10 Bowlers by Wickets ─────────────────────────────────────
        {
            "name": "ds_top_bowlers",
            "displayName": "Top 10 Bowlers",
            "queryLines": [
                f"SELECT bowler_name, wickets, ",
                f"       ROUND(economy, 2) AS economy ",
                f"FROM {PREFIX}_bowler_career ",
                f"ORDER BY wickets DESC ",
                f"LIMIT 10 "
            ]
        },

        # ── 8. Bowler Phase Economy ───────────────────────────────────────────
        {
            "name": "ds_bowler_phase",
            "displayName": "Bowler Phase Economy",
            "queryLines": [
                f"SELECT phase, ",
                f"       ROUND(AVG(phase_economy), 2)  AS avg_economy, ",
                f"       ROUND(AVG(phase_wickets), 1)  AS avg_wickets ",
                f"FROM {PREFIX}_bowler_by_phase ",
                f"WHERE phase IN ('Powerplay', 'Middle', 'Death') ",
                f"GROUP BY phase ",
                f"ORDER BY ",
                f"  CASE phase WHEN 'Powerplay' THEN 1 WHEN 'Middle' THEN 2 ELSE 3 END "
            ]
        },

        # ── 9. AI Anomaly Narratives ─────────────────────────────────────────
        {
            "name": "ds_anomaly",
            "displayName": "AI Anomaly Feed",
            "queryLines": [
                f"SELECT player, entity_type, anomaly_type, severity, ",
                f"       ROUND(career_metric, 2) AS career_metric, ",
                f"       ROUND(recent_metric, 2) AS recent_metric, ",
                f"       ROUND(sr_zscore, 2)     AS z_score, ",
                f"       ai_narrative, ",
                f"       recommended_action ",
                f"FROM {PREFIX}_anomaly_narratives ",
                f"ORDER BY ABS(sr_zscore) DESC ",
                f"LIMIT 50 "
            ]
        },

        # ── 10. Agent Alerts (HIGH severity) ─────────────────────────────────
        {
            "name": "ds_alerts",
            "displayName": "Agent Alerts",
            "queryLines": [
                f"SELECT player, anomaly_type, ",
                f"       ROUND(sr_zscore, 2) AS z_score, ",
                f"       recommended_action, ",
                f"       CAST(created_at AS STRING) AS alert_time ",
                f"FROM {PREFIX}_agent_alerts ",
                f"ORDER BY created_at DESC ",
                f"LIMIT 20 "
            ]
        },

        # ── 11. Anomaly Severity Mix ─────────────────────────────────────────
        {
            "name": "ds_severity_mix",
            "displayName": "Anomaly Severity Mix",
            "queryLines": [
                f"SELECT severity, COUNT(*) AS anomaly_count ",
                f"FROM {PREFIX}_anomaly_narratives ",
                f"GROUP BY severity "
            ]
        },

    ],

    # ══════════════════════════════════════════════════════════════════════════
    "pages": [

        # ─────────────────────────────────────────────────────────────────────
        # PAGE 1 — OVERVIEW
        # ─────────────────────────────────────────────────────────────────────
        {
            "name":        "overview",
            "displayName": "Overview",
            "pageType":    "PAGE_TYPE_CANVAS",
            "layout": [

                # Title
                {
                    "widget": {
                        "name": "title",
                        "multilineTextboxSpec": {
                            "lines": ["## Cricket Player Performance Intelligence Platform"]
                        }
                    },
                    "position": {"x": 0, "y": 0, "width": 6, "height": 1}
                },
                # Subtitle
                {
                    "widget": {
                        "name": "subtitle",
                        "multilineTextboxSpec": {
                            "lines": ["Male T20 International — Medallion Pipeline + Agentic AI  |  Data: Cricsheet + ESPNcricinfo"]
                        }
                    },
                    "position": {"x": 0, "y": 1, "width": 6, "height": 1}
                },

                # KPI 1 — Total Batters
                {
                    "widget": {
                        "name": "kpi-total-batters",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_summary",
                            "fields": [{"name": "total_batters", "expression": "`total_batters`"}],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 2, "widgetType": "counter",
                            "encodings": {"value": {"fieldName": "total_batters", "displayName": "Total Batters Tracked"}},
                            "frame": {"showTitle": True, "title": "Batters Tracked"}
                        }
                    },
                    "position": {"x": 0, "y": 2, "width": 2, "height": 3}
                },

                # KPI 2 — Avg Strike Rate
                {
                    "widget": {
                        "name": "kpi-avg-sr",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_summary",
                            "fields": [{"name": "avg_strike_rate", "expression": "`avg_strike_rate`"}],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 2, "widgetType": "counter",
                            "encodings": {"value": {"fieldName": "avg_strike_rate", "displayName": "Avg Strike Rate"}},
                            "frame": {"showTitle": True, "title": "Avg Strike Rate (T20I)"}
                        }
                    },
                    "position": {"x": 2, "y": 2, "width": 2, "height": 3}
                },

                # KPI 3 — Avg Batting Average
                {
                    "widget": {
                        "name": "kpi-avg-batting-avg",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_summary",
                            "fields": [{"name": "avg_batting_avg", "expression": "`avg_batting_avg`"}],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 2, "widgetType": "counter",
                            "encodings": {"value": {"fieldName": "avg_batting_avg", "displayName": "Avg Batting Average"}},
                            "frame": {"showTitle": True, "title": "Avg Batting Average"}
                        }
                    },
                    "position": {"x": 4, "y": 2, "width": 2, "height": 3}
                },

                # Section header
                {
                    "widget": {
                        "name": "section-phase",
                        "multilineTextboxSpec": {"lines": ["### Strike Rate by Phase (All Batters)"]}
                    },
                    "position": {"x": 0, "y": 5, "width": 6, "height": 1}
                },

                # Bar: SR by Phase
                {
                    "widget": {
                        "name": "bar-phase-sr",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_batter_phase",
                            "fields": [
                                {"name": "phase",         "expression": "`phase`"},
                                {"name": "avg_phase_sr",  "expression": "`avg_phase_sr`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "phase",        "scale": {"type": "categorical"}, "displayName": "Phase"},
                                "y": {"fieldName": "avg_phase_sr", "scale": {"type": "quantitative"}, "displayName": "Avg Strike Rate"}
                            },
                            "frame": {"showTitle": True, "title": "Average Strike Rate by Phase"}
                        }
                    },
                    "position": {"x": 0, "y": 6, "width": 3, "height": 5}
                },

                # Bar: Economy by Phase
                {
                    "widget": {
                        "name": "bar-phase-economy",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_bowler_phase",
                            "fields": [
                                {"name": "phase",       "expression": "`phase`"},
                                {"name": "avg_economy", "expression": "`avg_economy`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "phase",       "scale": {"type": "categorical"}, "displayName": "Phase"},
                                "y": {"fieldName": "avg_economy", "scale": {"type": "quantitative"}, "displayName": "Avg Economy"}
                            },
                            "frame": {"showTitle": True, "title": "Average Economy Rate by Phase"}
                        }
                    },
                    "position": {"x": 3, "y": 6, "width": 3, "height": 5}
                },
            ]
        },

        # ─────────────────────────────────────────────────────────────────────
        # PAGE 2 — BATTERS
        # ─────────────────────────────────────────────────────────────────────
        {
            "name":        "batters",
            "displayName": "Batters",
            "pageType":    "PAGE_TYPE_CANVAS",
            "layout": [

                {"widget": {"name": "b-title", "multilineTextboxSpec": {"lines": ["## Batter Performance"]}},
                 "position": {"x": 0, "y": 0, "width": 6, "height": 1}},

                {"widget": {"name": "b-sub", "multilineTextboxSpec": {"lines": ["Top 50 batters ranked by Impact Score (weighted composite of runs, SR, average, consistency)"]}},
                 "position": {"x": 0, "y": 1, "width": 6, "height": 1}},

                # Bowler summary KPIs
                {
                    "widget": {
                        "name": "kpi-total-bowlers",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_bowler_summary",
                            "fields": [{"name": "total_bowlers", "expression": "`total_bowlers`"}],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 2, "widgetType": "counter",
                            "encodings": {"value": {"fieldName": "total_bowlers", "displayName": "Bowlers Tracked"}},
                            "frame": {"showTitle": True, "title": "Bowlers Tracked"}
                        }
                    },
                    "position": {"x": 0, "y": 2, "width": 2, "height": 3}
                },
                {
                    "widget": {
                        "name": "kpi-total-wickets",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_bowler_summary",
                            "fields": [{"name": "total_wickets", "expression": "`total_wickets`"}],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 2, "widgetType": "counter",
                            "encodings": {"value": {"fieldName": "total_wickets", "displayName": "Total Wickets"}},
                            "frame": {"showTitle": True, "title": "Total Wickets (Bowlers)"}
                        }
                    },
                    "position": {"x": 2, "y": 2, "width": 2, "height": 3}
                },
                {
                    "widget": {
                        "name": "kpi-avg-economy",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_bowler_summary",
                            "fields": [{"name": "avg_economy", "expression": "`avg_economy`"}],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 2, "widgetType": "counter",
                            "encodings": {"value": {"fieldName": "avg_economy", "displayName": "Avg Economy"}},
                            "frame": {"showTitle": True, "title": "Avg Economy Rate"}
                        }
                    },
                    "position": {"x": 4, "y": 2, "width": 2, "height": 3}
                },

                # Section header
                {"widget": {"name": "b-sec1", "multilineTextboxSpec": {"lines": ["### Top 10 Batters by Career Runs"]}},
                 "position": {"x": 0, "y": 5, "width": 6, "height": 1}},

                # Bar: Top 10 by runs
                {
                    "widget": {
                        "name": "bar-top-batters",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_top_batters",
                            "fields": [
                                {"name": "batter_name", "expression": "`batter_name`"},
                                {"name": "runs",        "expression": "`runs`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "batter_name", "scale": {"type": "categorical"}, "displayName": "Batter"},
                                "y": {"fieldName": "runs",        "scale": {"type": "quantitative"}, "displayName": "Career Runs"}
                            },
                            "frame": {"showTitle": True, "title": "Top 10 Batters — Career Runs"}
                        }
                    },
                    "position": {"x": 0, "y": 6, "width": 6, "height": 5}
                },

                # Section header
                {"widget": {"name": "b-sec2", "multilineTextboxSpec": {"lines": ["### Career Stats — All Players"]}},
                 "position": {"x": 0, "y": 11, "width": 6, "height": 1}},

                # Table: Batter career
                {
                    "widget": {
                        "name": "table-batter-career",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_batter_career",
                            "fields": [
                                {"name": "batter_name",      "expression": "`batter_name`"},
                                {"name": "total_matches",    "expression": "`total_matches`"},
                                {"name": "runs",             "expression": "`runs`"},
                                {"name": "strike_rate",      "expression": "`strike_rate`"},
                                {"name": "average",          "expression": "`average`"},
                                {"name": "sixes",            "expression": "`sixes`"},
                                {"name": "impact_score",     "expression": "`impact_score`"},
                                {"name": "consistency_label","expression": "`consistency_label`"},
                                {"name": "form_trajectory",  "expression": "`form_trajectory`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 2, "widgetType": "table",
                            "encodings": {"columns": [
                                {"fieldName": "batter_name",       "displayName": "Player"},
                                {"fieldName": "total_matches",     "displayName": "Matches"},
                                {"fieldName": "runs",              "displayName": "Runs"},
                                {"fieldName": "strike_rate",       "displayName": "Strike Rate"},
                                {"fieldName": "average",           "displayName": "Average"},
                                {"fieldName": "sixes",             "displayName": "Sixes"},
                                {"fieldName": "impact_score",      "displayName": "Impact Score"},
                                {"fieldName": "consistency_label", "displayName": "Consistency"},
                                {"fieldName": "form_trajectory",   "displayName": "Form"}
                            ]},
                            "frame": {"showTitle": True, "title": "Batter Career Stats (Top 50 by Impact Score)"}
                        }
                    },
                    "position": {"x": 0, "y": 12, "width": 6, "height": 7}
                },
            ]
        },

        # ─────────────────────────────────────────────────────────────────────
        # PAGE 3 — BOWLERS
        # ─────────────────────────────────────────────────────────────────────
        {
            "name":        "bowlers",
            "displayName": "Bowlers",
            "pageType":    "PAGE_TYPE_CANVAS",
            "layout": [

                {"widget": {"name": "w-title", "multilineTextboxSpec": {"lines": ["## Bowler Performance"]}},
                 "position": {"x": 0, "y": 0, "width": 6, "height": 1}},
                {"widget": {"name": "w-sub",   "multilineTextboxSpec": {"lines": ["Top 50 bowlers ranked by wickets. Economy uses ICC formula (excludes byes/leg-byes)."]}},
                 "position": {"x": 0, "y": 1, "width": 6, "height": 1}},

                # Section header
                {"widget": {"name": "w-sec1", "multilineTextboxSpec": {"lines": ["### Top 10 Bowlers by Wickets"]}},
                 "position": {"x": 0, "y": 2, "width": 6, "height": 1}},

                # Bar: Top 10 bowlers
                {
                    "widget": {
                        "name": "bar-top-bowlers",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_top_bowlers",
                            "fields": [
                                {"name": "bowler_name", "expression": "`bowler_name`"},
                                {"name": "wickets",     "expression": "`wickets`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "bowler_name", "scale": {"type": "categorical"}, "displayName": "Bowler"},
                                "y": {"fieldName": "wickets",     "scale": {"type": "quantitative"}, "displayName": "Wickets"}
                            },
                            "frame": {"showTitle": True, "title": "Top 10 Bowlers — Career Wickets"}
                        }
                    },
                    "position": {"x": 0, "y": 3, "width": 6, "height": 5}
                },

                # Section header
                {"widget": {"name": "w-sec2", "multilineTextboxSpec": {"lines": ["### Economy Rate by Phase"]}},
                 "position": {"x": 0, "y": 8, "width": 6, "height": 1}},

                # Bar: Economy by phase
                {
                    "widget": {
                        "name": "bar-bowler-phase",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_bowler_phase",
                            "fields": [
                                {"name": "phase",       "expression": "`phase`"},
                                {"name": "avg_economy", "expression": "`avg_economy`"},
                                {"name": "avg_wickets", "expression": "`avg_wickets`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "phase",       "scale": {"type": "categorical"}, "displayName": "Phase"},
                                "y": {
                                    "scale": {"type": "quantitative"},
                                    "fields": [
                                        {"fieldName": "avg_economy", "displayName": "Avg Economy"},
                                        {"fieldName": "avg_wickets", "displayName": "Avg Wickets/Match"}
                                    ]
                                }
                            },
                            "frame": {"showTitle": True, "title": "Economy & Wickets by Phase"}
                        }
                    },
                    "position": {"x": 0, "y": 9, "width": 6, "height": 5}
                },

                # Section header
                {"widget": {"name": "w-sec3", "multilineTextboxSpec": {"lines": ["### Career Stats — All Bowlers"]}},
                 "position": {"x": 0, "y": 14, "width": 6, "height": 1}},

                # Table: Bowler career
                {
                    "widget": {
                        "name": "table-bowler-career",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_bowler_career",
                            "fields": [
                                {"name": "bowler_name",  "expression": "`bowler_name`"},
                                {"name": "total_matches","expression": "`total_matches`"},
                                {"name": "wickets",      "expression": "`wickets`"},
                                {"name": "economy",      "expression": "`economy`"},
                                {"name": "bowling_avg",  "expression": "`bowling_avg`"},
                                {"name": "bowling_sr",   "expression": "`bowling_sr`"},
                                {"name": "dot_ball_pct", "expression": "`dot_ball_pct`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 2, "widgetType": "table",
                            "encodings": {"columns": [
                                {"fieldName": "bowler_name",  "displayName": "Bowler"},
                                {"fieldName": "total_matches","displayName": "Matches"},
                                {"fieldName": "wickets",      "displayName": "Wickets"},
                                {"fieldName": "economy",      "displayName": "Economy"},
                                {"fieldName": "bowling_avg",  "displayName": "Bowling Avg"},
                                {"fieldName": "bowling_sr",   "displayName": "Bowling SR"},
                                {"fieldName": "dot_ball_pct", "displayName": "Dot Ball %"}
                            ]},
                            "frame": {"showTitle": True, "title": "Bowler Career Stats (Top 50 by Wickets)"}
                        }
                    },
                    "position": {"x": 0, "y": 15, "width": 6, "height": 7}
                },
            ]
        },

        # ─────────────────────────────────────────────────────────────────────
        # PAGE 4 — AI INSIGHTS
        # ─────────────────────────────────────────────────────────────────────
        {
            "name":        "ai-insights",
            "displayName": "AI Insights",
            "pageType":    "PAGE_TYPE_CANVAS",
            "layout": [

                {"widget": {"name": "ai-title", "multilineTextboxSpec": {"lines": ["## AI-Powered Anomaly Intelligence"]}},
                 "position": {"x": 0, "y": 0, "width": 6, "height": 1}},
                {"widget": {"name": "ai-sub", "multilineTextboxSpec": {"lines": ["Agentic loop: OBSERVE → REASON → DECIDE → ACT. Powered by databricks-meta-llama-3-3-70b-instruct"]}},
                 "position": {"x": 0, "y": 1, "width": 6, "height": 1}},

                # Pie: Severity mix
                {
                    "widget": {
                        "name": "pie-severity",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_severity_mix",
                            "fields": [
                                {"name": "severity",      "expression": "`severity`"},
                                {"name": "anomaly_count", "expression": "`anomaly_count`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "pie",
                            "encodings": {
                                "angle": {"fieldName": "anomaly_count", "scale": {"type": "quantitative"}, "displayName": "Count"},
                                "color": {"fieldName": "severity",      "scale": {"type": "categorical"}, "displayName": "Severity"}
                            },
                            "frame": {"showTitle": True, "title": "Anomalies by Severity"}
                        }
                    },
                    "position": {"x": 0, "y": 2, "width": 3, "height": 5}
                },

                # Bar: Top anomalies by Z-score
                {
                    "widget": {
                        "name": "bar-anomaly-zscore",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_anomaly",
                            "fields": [
                                {"name": "player",  "expression": "`player`"},
                                {"name": "z_score", "expression": "`z_score`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 3, "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "player",  "scale": {"type": "categorical"},  "displayName": "Player"},
                                "y": {"fieldName": "z_score", "scale": {"type": "quantitative"}, "displayName": "Z-Score"}
                            },
                            "frame": {"showTitle": True, "title": "Top Anomalies by Z-Score"}
                        }
                    },
                    "position": {"x": 3, "y": 2, "width": 3, "height": 5}
                },

                # Section: Alerts
                {"widget": {"name": "ai-sec1", "multilineTextboxSpec": {"lines": ["### 🚨 HIGH Severity Alerts — Recommended Actions"]}},
                 "position": {"x": 0, "y": 7, "width": 6, "height": 1}},

                {
                    "widget": {
                        "name": "table-alerts",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_alerts",
                            "fields": [
                                {"name": "player",             "expression": "`player`"},
                                {"name": "anomaly_type",       "expression": "`anomaly_type`"},
                                {"name": "z_score",            "expression": "`z_score`"},
                                {"name": "recommended_action", "expression": "`recommended_action`"},
                                {"name": "alert_time",         "expression": "`alert_time`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 2, "widgetType": "table",
                            "encodings": {"columns": [
                                {"fieldName": "player",             "displayName": "Player"},
                                {"fieldName": "anomaly_type",       "displayName": "Anomaly Type"},
                                {"fieldName": "z_score",            "displayName": "Z-Score"},
                                {"fieldName": "recommended_action", "displayName": "Recommended Action"},
                                {"fieldName": "alert_time",         "displayName": "Detected At"}
                            ]},
                            "frame": {"showTitle": True, "title": "Agent Alerts (HIGH Severity)"}
                        }
                    },
                    "position": {"x": 0, "y": 8, "width": 6, "height": 5}
                },

                # Section: Full anomaly feed
                {"widget": {"name": "ai-sec2", "multilineTextboxSpec": {"lines": ["### Full AI Anomaly Feed — Narratives + Actions"]}},
                 "position": {"x": 0, "y": 13, "width": 6, "height": 1}},

                {
                    "widget": {
                        "name": "table-anomaly-feed",
                        "queries": [{"name": "main_query", "query": {
                            "datasetName": "ds_anomaly",
                            "fields": [
                                {"name": "player",             "expression": "`player`"},
                                {"name": "entity_type",        "expression": "`entity_type`"},
                                {"name": "anomaly_type",       "expression": "`anomaly_type`"},
                                {"name": "severity",           "expression": "`severity`"},
                                {"name": "career_metric",      "expression": "`career_metric`"},
                                {"name": "recent_metric",      "expression": "`recent_metric`"},
                                {"name": "z_score",            "expression": "`z_score`"},
                                {"name": "ai_narrative",       "expression": "`ai_narrative`"},
                                {"name": "recommended_action", "expression": "`recommended_action`"}
                            ],
                            "disaggregated": True
                        }}],
                        "spec": {
                            "version": 2, "widgetType": "table",
                            "encodings": {"columns": [
                                {"fieldName": "player",             "displayName": "Player"},
                                {"fieldName": "entity_type",        "displayName": "Type"},
                                {"fieldName": "anomaly_type",       "displayName": "Anomaly"},
                                {"fieldName": "severity",           "displayName": "Severity"},
                                {"fieldName": "career_metric",      "displayName": "Career Baseline"},
                                {"fieldName": "recent_metric",      "displayName": "Recent Value"},
                                {"fieldName": "z_score",            "displayName": "Z-Score"},
                                {"fieldName": "ai_narrative",       "displayName": "AI Analysis"},
                                {"fieldName": "recommended_action", "displayName": "Recommended Action"}
                            ]},
                            "frame": {"showTitle": True, "title": "Anomaly Feed with AI Narratives"}
                        }
                    },
                    "position": {"x": 0, "y": 14, "width": 6, "height": 8}
                },
            ]
        },
    ]
}

# COMMAND ----------

# MAGIC %md ## Deploy Dashboard

# COMMAND ----------

result = w.lakeview.create(
    display_name      = "Cricket Intelligence Platform",
    parent_path       = DASH_DIR,
    serialized_dashboard = json.dumps(dashboard),
)

dashboard_id  = result.dashboard_id
dashboard_url = f"https://{w.config.host}/dashboardsv3/{dashboard_id}"

print(f"✅ Dashboard created!")
print(f"   ID  : {dashboard_id}")
print(f"   URL : {dashboard_url}")

# COMMAND ----------

# MAGIC %md ## Publish Dashboard (makes it shareable inside the workspace)

# COMMAND ----------

w.lakeview.publish(dashboard_id=dashboard_id)
print(f"✅ Dashboard published: {dashboard_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Done
# MAGIC
# MAGIC Open the link above in your Databricks workspace.
# MAGIC
# MAGIC **Pages:**
# MAGIC | Page | Content |
# MAGIC |------|---------|
# MAGIC | Overview | Platform KPIs, Phase Strike Rate & Economy bars |
# MAGIC | Batters  | Top-10 bar, full career table with Impact Score & Form |
# MAGIC | Bowlers  | Top-10 bar, phase economy+wickets chart, career table |
# MAGIC | AI Insights | Severity pie, Z-score bar, HIGH alerts + full AI feed |
# MAGIC
# MAGIC **To update the dashboard after re-running notebooks:**
# MAGIC ```python
# MAGIC w.lakeview.update(dashboard_id=dashboard_id, serialized_dashboard=json.dumps(dashboard))
# MAGIC ```
