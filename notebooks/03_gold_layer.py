# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold Layer: Aggregated Analytics Tables
# MAGIC
# MAGIC **Reads from:** `tabular.dataexpert.prateek_capstone_project_silver_deliveries`
# MAGIC
# MAGIC **Tables built (in dependency order):**
# MAGIC 1. `gold_batter_by_match`   — match-by-match batter performance (needed for stddev)
# MAGIC 2. `gold_batter_career`     — career summary + consistency score + form trajectory
# MAGIC 3. `gold_bowler_by_match`   — match-by-match bowler performance
# MAGIC 4. `gold_bowler_career`     — career summary + form trajectory
# MAGIC 5. `gold_batter_by_phase`   — powerplay / middle / death breakdown (batting)
# MAGIC 6. `gold_bowler_by_phase`   — powerplay / middle / death breakdown (bowling)
# MAGIC 7. `gold_phase_momentum`    — batter phase SR by season (trend over career)
# MAGIC 8. `gold_matchup`           — batter vs bowler head-to-head
# MAGIC 9. `gold_anomaly_feed`      — recent form vs career baseline using real Z-scores
# MAGIC
# MAGIC **Stat logic (consistent across all tables):**
# MAGIC - Strike rate       = (runs / legal balls faced) * 100
# MAGIC - Batting avg       = runs / dismissals  (not innings — not-outs inflate otherwise)
# MAGIC - Dot %             = dot balls / legal balls * 100
# MAGIC - Economy           = (runs_for_bowler / legal balls bowled) * 6
# MAGIC - Bowling SR        = legal balls / wickets
# MAGIC - Consistency score = 1 - (stddev_sr / avg_sr)  — higher = more consistent
# MAGIC - Z-score           = (recent_sr - career_avg_sr) / career_stddev_sr  — real stddev
# MAGIC - Impact score      = weighted composite (SR + avg + boundary% + dot avoidance)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import datetime

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG = "tabular"
SCHEMA  = "dataexpert"
PREFIX  = f"{CATALOG}.{SCHEMA}.prateek_capstone_project"

SILVER_TABLE   = f"{PREFIX}_silver_deliveries"
REGISTRY_TABLE = f"{PREFIX}_bronze_player_registry"
QUALITY_TABLE  = f"{PREFIX}_gold_quality_log"

TABLES = {
    "batter_by_match"  : f"{PREFIX}_gold_batter_by_match",
    "batter_career"    : f"{PREFIX}_gold_batter_career",
    "bowler_by_match"  : f"{PREFIX}_gold_bowler_by_match",
    "bowler_career"    : f"{PREFIX}_gold_bowler_career",
    "batter_by_phase"  : f"{PREFIX}_gold_batter_by_phase",
    "bowler_by_phase"  : f"{PREFIX}_gold_bowler_by_phase",
    "phase_momentum"   : f"{PREFIX}_gold_phase_momentum",
    "matchup"          : f"{PREFIX}_gold_matchup",
    "anomaly_feed"     : f"{PREFIX}_gold_anomaly_feed",
}

RECENT_WINDOW     = 10   # matches to define "recent form"
ZSCORE_THRESHOLD  = 1.5  # flag if |z-score| >= this
MIN_INNINGS       = 10   # min innings for batter career table
MIN_BALLS_BOWLED  = 60   # min balls (10 overs) for bowler career table
MIN_PHASE_BALLS   = 30   # min balls per phase for phase tables
MIN_MATCHUP_BALLS = 6    # min balls for a meaningful matchup

print("✅ Config ready")
for k, v in TABLES.items():
    print(f"   {k:<20} → {v}")

# COMMAND ----------
# MAGIC %md ## Step 1 — Read Silver

# COMMAND ----------

sv    = spark.table(SILVER_TABLE)
total = sv.count()
print(f"✅ Silver rows loaded  : {total:,}")
print(f"   Distinct batters   : {sv.select('batter').distinct().count():,}")
print(f"   Distinct bowlers   : {sv.select('bowler').distinct().count():,}")
print(f"   Distinct matches   : {sv.select('match_id').distinct().count():,}")
print(f"   Season range       : {sv.agg(F.min('season'), F.max('season')).collect()[0]}")

# Legal deliveries — used in all batter-facing metrics
legal = sv.filter(F.col("is_legal") == True)

# COMMAND ----------
# MAGIC %md ## Table 1 — gold_batter_by_match

# COMMAND ----------

batter_by_match = (
    legal
    .groupBy("batter", "batter_id", "match_id", "match_date", "season",
             "innings_number", "team_batting", "team_home", "team_away",
             "match_winner", "venue")
    .agg(
        F.sum("runs_batter")                            .alias("runs"),
        F.count("*")                                    .alias("balls_faced"),
        F.sum(F.col("is_dot_ball").cast("int"))         .alias("dot_balls"),
        F.sum(F.col("is_boundary").cast("int"))         .alias("boundaries"),
        F.sum(F.col("is_four").cast("int"))             .alias("fours"),
        F.sum(F.col("is_six").cast("int"))              .alias("sixes"),
        # FIX Bug7: only count the facing batter's OWN dismissal (not non-striker run-outs)
        F.max(F.when(
            (F.col("is_wicket") == True) & (F.col("player_out") == F.col("batter")), 1
        ).otherwise(0))                                 .alias("is_out"),
        F.first(F.when(
            (F.col("is_wicket") == True) & (F.col("player_out") == F.col("batter")),
            F.col("wicket_kind")
        ))                                              .alias("dismissal_kind"),
        F.min("batting_position")                       .alias("batting_position"),
    )
    .withColumn("strike_rate",
        F.round(F.when(F.col("balls_faced") > 0, F.col("runs") / F.col("balls_faced") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("dot_pct",
        F.round(F.when(F.col("balls_faced") > 0, F.col("dot_balls") / F.col("balls_faced") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("boundary_pct",
        F.round(F.when(F.col("balls_faced") > 0, F.col("boundaries") / F.col("balls_faced") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("opponent",
        F.when(F.col("team_batting") == F.col("team_home"), F.col("team_away"))
         .otherwise(F.col("team_home")))
    .orderBy("batter", "match_date")
)

batter_by_match.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(TABLES["batter_by_match"])
print(f"✅ gold_batter_by_match — {spark.table(TABLES['batter_by_match']).count():,} rows")

# COMMAND ----------
# MAGIC %md ## Table 2 — gold_batter_career

# COMMAND ----------

bbm = spark.table(TABLES["batter_by_match"])

# FIX Bugs 1 & 2: highest_score / hundreds / fifties MUST come from match-level runs,
# not delivery-level runs_batter (max per ball = 6, so was always showing 4 or 6)
innings_stats = (
    bbm
    .groupBy("batter", "batter_id")
    .agg(
        F.max("runs")                                                   .alias("highest_score"),
        F.sum(F.when(F.col("runs") >= 100, 1).otherwise(0))            .alias("hundreds"),
        F.sum(F.when(F.col("runs").between(50, 99), 1).otherwise(0))   .alias("fifties"),
    )
)

# Career totals from silver
career_totals = (
    legal
    .groupBy("batter", "batter_id")
    .agg(
        F.countDistinct("match_id")                                          .alias("matches"),
        F.countDistinct("match_id", "innings_number")                        .alias("innings"),
        F.sum("runs_batter")                                                 .alias("runs"),
        F.count("*")                                                         .alias("balls_faced"),
        # FIX Bug3: only count dismissals where THIS batter was the one out (not non-striker run-outs)
        F.sum(F.when(
            (F.col("is_wicket") == True) & (F.col("player_out") == F.col("batter")), 1
        ).otherwise(0))                                                      .alias("dismissals"),
        F.sum(F.col("is_dot_ball").cast("int"))                              .alias("dot_balls"),
        F.sum(F.col("is_boundary").cast("int"))                              .alias("boundaries"),
        F.sum(F.col("is_four").cast("int"))                                  .alias("fours"),
        F.sum(F.col("is_six").cast("int"))                                   .alias("sixes"),
        # NOTE: highest_score / hundreds / fifties are per-INNINGS stats,
        # NOT per-ball. They are joined from innings_stats below (from gold_batter_by_match).
        F.min("match_date")                                                  .alias("debut_date"),
        F.max("match_date")                                                  .alias("last_match_date"),
    )
)

# Career avg SR + stddev SR from match-level data
career_sr_stats = (
    bbm
    .groupBy("batter", "batter_id")
    .agg(
        F.avg("strike_rate")    .alias("career_avg_sr"),
        F.stddev("strike_rate") .alias("career_stddev_sr"),
        F.avg("boundary_pct")   .alias("career_avg_boundary_pct"),
    )
)

# Rolling 10-match form — latest value per player
rolling_window = (
    Window.partitionBy("batter_id")
          .orderBy("match_date")
          .rowsBetween(-9, 0)
)
latest_form = (
    bbm
    .withColumn("rolling_avg_sr",
        F.round(F.avg("strike_rate").over(rolling_window), 2))
    .withColumn("rolling_avg_boundary_pct",
        F.round(F.avg("boundary_pct").over(rolling_window), 2))
    .withColumn("match_rank",
        F.row_number().over(Window.partitionBy("batter_id").orderBy(F.desc("match_date"))))
    .filter(F.col("match_rank") == 1)
    .select("batter_id", "rolling_avg_sr", "rolling_avg_boundary_pct")
)

batter_career = (
    career_totals
    .join(career_sr_stats, on=["batter", "batter_id"], how="left")
    .join(latest_form,     on="batter_id",             how="left")
    # FIX Bugs 1 & 2: brings in correct highest_score, hundreds, fifties from match-level data
    .join(innings_stats,   on=["batter", "batter_id"], how="left")
    .withColumn("strike_rate",
        F.round(F.when(F.col("balls_faced") > 0, F.col("runs") / F.col("balls_faced") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("batting_avg",
        F.round(F.when(F.col("dismissals") > 0, F.col("runs") / F.col("dismissals"))
                 .otherwise(F.col("runs").cast("double")), 2))
    .withColumn("dot_pct",
        F.round(F.when(F.col("balls_faced") > 0, F.col("dot_balls") / F.col("balls_faced") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("boundary_pct",
        F.round(F.when(F.col("balls_faced") > 0, F.col("boundaries") / F.col("balls_faced") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("not_outs",
        F.col("innings") - F.col("dismissals"))
    # Null-safe consistency score: null stddev (single match) → null score
    .withColumn("consistency_score",
        F.round(
            F.when(F.col("career_stddev_sr").isNotNull() & (F.col("career_avg_sr") > 0),
                F.greatest(F.lit(0.0),
                    F.lit(1.0) - (F.col("career_stddev_sr") / F.col("career_avg_sr")))
            ).otherwise(F.lit(None)), 3))
    .withColumn("consistency_label",
        F.when(F.col("consistency_score") >= 0.85, "Very Consistent")
         .when(F.col("consistency_score") >= 0.70, "Consistent")
         .when(F.col("consistency_score") >= 0.55, "Moderate")
         .when(F.col("consistency_score").isNotNull(), "Boom or Bust")
         .otherwise(F.lit(None)))
    .withColumn("form_vs_career_delta",
        F.round(F.col("rolling_avg_sr") - F.col("career_avg_sr"), 2))
    .withColumn("form_trajectory",
        F.when(F.col("form_vs_career_delta") >  10, "Rising ↑")
         .when(F.col("form_vs_career_delta") < -10, "Declining ↓")
         .otherwise("Stable →"))
    .withColumn("impact_score",
        F.round(
            F.least(F.lit(99.0),
                F.when(F.col("strike_rate").isNotNull() & F.col("batting_avg").isNotNull(),
                    (F.col("strike_rate")  / F.lit(150.0)) * 35 +
                    (F.col("batting_avg")  / F.lit(35.0))  * 30 +
                    (F.col("boundary_pct") / F.lit(40.0))  * 20 +
                    (F.lit(1) - F.col("dot_pct") / F.lit(40.0)) * 15
                ).otherwise(F.lit(0))), 1))
    .filter(F.col("innings") >= MIN_INNINGS)
    .orderBy(F.desc("runs"))
)

batter_career.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(TABLES["batter_career"])
print(f"✅ gold_batter_career — {spark.table(TABLES['batter_career']).count():,} rows")
display(spark.table(TABLES["batter_career"]).orderBy(F.desc("runs")).limit(10))

# COMMAND ----------
# MAGIC %md ## Table 3 — gold_bowler_by_match

# COMMAND ----------

bowler_by_match = (
    sv
    .groupBy("bowler", "bowler_id", "match_id", "match_date", "season",
             "innings_number", "team_home", "team_away", "venue")
    .agg(
        F.sum(F.col("is_legal").cast("int"))            .alias("balls_bowled"),
        F.sum("runs_for_bowler")                        .alias("runs_conceded"),
        F.sum(F.col("is_bowler_wicket").cast("int"))    .alias("wickets"),
        F.sum(F.col("is_dot_ball").cast("int"))         .alias("dot_balls"),
        F.sum(F.col("is_boundary").cast("int"))         .alias("boundaries_conceded"),
        F.sum(F.col("is_wide").cast("int"))             .alias("wides"),
        F.sum(F.col("is_noball").cast("int"))           .alias("noballs"),
    )
    .withColumn("overs",
        F.round(F.col("balls_bowled") / 6, 1))
    .withColumn("economy",
        F.round(F.when(F.col("balls_bowled") > 0,
                       F.col("runs_conceded") / F.col("balls_bowled") * 6)
                 .otherwise(F.lit(None)), 2))
    .withColumn("dot_pct",
        F.round(F.when(F.col("balls_bowled") > 0,
                       F.col("dot_balls") / F.col("balls_bowled") * 100)
                 .otherwise(F.lit(None)), 2))
    .orderBy("bowler", "match_date")
)

bowler_by_match.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(TABLES["bowler_by_match"])
print(f"✅ gold_bowler_by_match — {spark.table(TABLES['bowler_by_match']).count():,} rows")

# COMMAND ----------
# MAGIC %md ## Table 4 — gold_bowler_career

# COMMAND ----------

bobm = spark.table(TABLES["bowler_by_match"])

bowler_totals = (
    sv
    .groupBy("bowler", "bowler_id")
    .agg(
        F.countDistinct("match_id")                         .alias("matches"),
        F.countDistinct("match_id", "innings_number")       .alias("innings_bowled"),
        F.sum(F.col("is_legal").cast("int"))                .alias("balls_bowled"),
        F.sum("runs_for_bowler")                            .alias("runs_conceded"),
        F.sum(F.col("is_bowler_wicket").cast("int"))        .alias("wickets"),
        F.sum(F.col("is_dot_ball").cast("int"))             .alias("dot_balls"),
        F.sum(F.col("is_boundary").cast("int"))             .alias("boundaries_conceded"),
        F.sum(F.col("is_wide").cast("int"))                 .alias("wides"),
        F.sum(F.col("is_noball").cast("int"))               .alias("noballs"),
        F.min("match_date")                                 .alias("debut_date"),
        F.max("match_date")                                 .alias("last_match_date"),
    )
)

bowler_sr_stats = (
    bobm
    .groupBy("bowler", "bowler_id")
    .agg(
        F.avg("economy")    .alias("career_avg_economy"),
        F.stddev("economy") .alias("career_stddev_economy"),
    )
)

bowler_rolling_window = (
    Window.partitionBy("bowler_id")
          .orderBy("match_date")
          .rowsBetween(-9, 0)
)
bowler_latest_form = (
    bobm
    .withColumn("rolling_avg_economy",
        F.round(F.avg("economy").over(bowler_rolling_window), 2))
    .withColumn("match_rank",
        F.row_number().over(Window.partitionBy("bowler_id").orderBy(F.desc("match_date"))))
    .filter(F.col("match_rank") == 1)
    .select("bowler_id", "rolling_avg_economy")
)

bowler_career = (
    bowler_totals
    .join(bowler_sr_stats,    on=["bowler", "bowler_id"], how="left")
    .join(bowler_latest_form, on="bowler_id",             how="left")
    .withColumn("overs_bowled",
        F.round(F.col("balls_bowled") / 6, 1))
    .withColumn("economy",
        F.round(F.when(F.col("balls_bowled") > 0,
                       F.col("runs_conceded") / F.col("balls_bowled") * 6)
                 .otherwise(F.lit(None)), 2))
    .withColumn("bowling_avg",
        F.round(F.when(F.col("wickets") > 0, F.col("runs_conceded") / F.col("wickets"))
                 .otherwise(F.lit(None)), 2))
    .withColumn("bowling_sr",
        F.round(F.when(F.col("wickets") > 0, F.col("balls_bowled") / F.col("wickets"))
                 .otherwise(F.lit(None)), 2))
    .withColumn("dot_pct",
        F.round(F.when(F.col("balls_bowled") > 0,
                       F.col("dot_balls") / F.col("balls_bowled") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("boundary_pct_conceded",
        F.round(F.when(F.col("balls_bowled") > 0,
                       F.col("boundaries_conceded") / F.col("balls_bowled") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("economy_vs_career_delta",
        F.round(F.col("rolling_avg_economy") - F.col("career_avg_economy"), 2))
    .withColumn("form_trajectory",
        F.when(F.col("economy_vs_career_delta") < -0.5, "Improving ↑")
         .when(F.col("economy_vs_career_delta") >  0.5, "Declining ↓")
         .otherwise("Stable →"))
    .withColumn("impact_score",
        F.round(
            F.least(F.lit(99.0),
                F.when(F.col("economy").isNotNull() & (F.col("overs_bowled") > 0),
                    F.greatest(F.lit(0.0),
                        (F.lit(1) - F.col("economy") / F.lit(9.0))               * 35 +
                        (F.col("wickets") / F.col("overs_bowled"))                * 30 +
                        (F.col("dot_pct") / F.lit(50.0))                          * 20 +
                        (F.lit(1) - F.col("boundary_pct_conceded") / F.lit(20.0)) * 15
                    )
                ).otherwise(F.lit(0))), 1))
    .filter(F.col("balls_bowled") >= MIN_BALLS_BOWLED)
    .orderBy(F.desc("wickets"))
)

bowler_career.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(TABLES["bowler_career"])
print(f"✅ gold_bowler_career — {spark.table(TABLES['bowler_career']).count():,} rows")
display(spark.table(TABLES["bowler_career"]).orderBy(F.desc("wickets")).limit(10))

# COMMAND ----------
# MAGIC %md ## Table 5 — gold_batter_by_phase

# COMMAND ----------

batter_by_phase = (
    legal
    .groupBy("batter", "batter_id", "phase")
    .agg(
        F.countDistinct("match_id")                     .alias("matches"),
        F.sum("runs_batter")                            .alias("runs"),
        F.count("*")                                    .alias("balls_faced"),
        F.sum(F.col("is_wicket").cast("int"))           .alias("dismissals"),
        F.sum(F.col("is_dot_ball").cast("int"))         .alias("dot_balls"),
        F.sum(F.col("is_boundary").cast("int"))         .alias("boundaries"),
        F.sum(F.col("is_four").cast("int"))             .alias("fours"),
        F.sum(F.col("is_six").cast("int"))              .alias("sixes"),
    )
    .withColumn("strike_rate",
        F.round(F.when(F.col("balls_faced") > 0, F.col("runs") / F.col("balls_faced") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("batting_avg",
        F.round(F.when(F.col("dismissals") > 0, F.col("runs") / F.col("dismissals"))
                 .otherwise(F.col("runs").cast("double")), 2))
    .withColumn("dot_pct",
        F.round(F.when(F.col("balls_faced") > 0, F.col("dot_balls") / F.col("balls_faced") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("boundary_pct",
        F.round(F.when(F.col("balls_faced") > 0, F.col("boundaries") / F.col("balls_faced") * 100)
                 .otherwise(F.lit(None)), 2))
    .filter(F.col("balls_faced") >= MIN_PHASE_BALLS)
    .orderBy("batter", "phase")
)

batter_by_phase.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(TABLES["batter_by_phase"])
print(f"✅ gold_batter_by_phase — {spark.table(TABLES['batter_by_phase']).count():,} rows")

# COMMAND ----------
# MAGIC %md ## Table 6 — gold_bowler_by_phase

# COMMAND ----------

bowler_by_phase = (
    sv
    .groupBy("bowler", "bowler_id", "phase")
    .agg(
        F.countDistinct("match_id")                         .alias("matches"),
        F.sum(F.col("is_legal").cast("int"))                .alias("balls_bowled"),
        F.sum("runs_for_bowler")                            .alias("runs_conceded"),
        F.sum(F.col("is_bowler_wicket").cast("int"))        .alias("wickets"),
        F.sum(F.col("is_dot_ball").cast("int"))             .alias("dot_balls"),
        F.sum(F.col("is_boundary").cast("int"))             .alias("boundaries_conceded"),
    )
    .withColumn("economy",
        F.round(F.when(F.col("balls_bowled") > 0,
                       F.col("runs_conceded") / F.col("balls_bowled") * 6)
                 .otherwise(F.lit(None)), 2))
    .withColumn("bowling_sr",
        F.round(F.when(F.col("wickets") > 0, F.col("balls_bowled") / F.col("wickets"))
                 .otherwise(F.lit(None)), 2))
    .withColumn("dot_pct",
        F.round(F.when(F.col("balls_bowled") > 0,
                       F.col("dot_balls") / F.col("balls_bowled") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("boundary_pct_conceded",
        F.round(F.when(F.col("balls_bowled") > 0,
                       F.col("boundaries_conceded") / F.col("balls_bowled") * 100)
                 .otherwise(F.lit(None)), 2))
    .filter(F.col("balls_bowled") >= MIN_PHASE_BALLS)
    .orderBy("bowler", "phase")
)

bowler_by_phase.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(TABLES["bowler_by_phase"])
print(f"✅ gold_bowler_by_phase — {spark.table(TABLES['bowler_by_phase']).count():,} rows")

# COMMAND ----------
# MAGIC %md ## Table 7 — gold_phase_momentum

# COMMAND ----------

season_window = (
    Window.partitionBy("batter_id", "phase")
          .orderBy("season")
          .rowsBetween(-2, 0)
)

phase_momentum = (
    legal
    .groupBy("batter", "batter_id", "phase", "season")
    .agg(
        F.countDistinct("match_id")                 .alias("matches"),
        F.sum("runs_batter")                        .alias("runs"),
        F.count("*")                                .alias("balls_faced"),
        F.sum(F.col("is_boundary").cast("int"))     .alias("boundaries"),
        F.sum(F.col("is_six").cast("int"))          .alias("sixes"),
        F.sum(F.col("is_dot_ball").cast("int"))     .alias("dot_balls"),
    )
    .filter(F.col("balls_faced") >= 12)
    .withColumn("strike_rate",
        F.round(F.when(F.col("balls_faced") > 0, F.col("runs") / F.col("balls_faced") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("boundary_pct",
        F.round(F.when(F.col("balls_faced") > 0, F.col("boundaries") / F.col("balls_faced") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("dot_pct",
        F.round(F.when(F.col("balls_faced") > 0, F.col("dot_balls") / F.col("balls_faced") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("rolling_3season_sr",
        F.round(F.avg("strike_rate").over(season_window), 2))
    .orderBy("batter", "phase", "season")
)

phase_momentum.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(TABLES["phase_momentum"])
print(f"✅ gold_phase_momentum — {spark.table(TABLES['phase_momentum']).count():,} rows")

# COMMAND ----------
# MAGIC %md ## Table 8 — gold_matchup

# COMMAND ----------

matchup = (
    legal
    .groupBy("batter", "batter_id", "bowler", "bowler_id")
    .agg(
        F.count("*")                                    .alias("balls"),
        F.sum("runs_batter")                            .alias("runs"),
        F.sum(F.col("is_bowler_wicket").cast("int"))    .alias("dismissals"),
        F.sum(F.col("is_dot_ball").cast("int"))         .alias("dot_balls"),
        F.sum(F.col("is_boundary").cast("int"))         .alias("boundaries"),
        F.sum(F.col("is_six").cast("int"))              .alias("sixes"),
        F.countDistinct("match_id")                     .alias("matches_faced"),
    )
    .withColumn("strike_rate",
        F.round(F.when(F.col("balls") > 0, F.col("runs") / F.col("balls") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("batting_avg",
        F.round(F.when(F.col("dismissals") > 0, F.col("runs") / F.col("dismissals"))
                 .otherwise(F.col("runs").cast("double")), 2))
    .withColumn("dot_pct",
        F.round(F.when(F.col("balls") > 0, F.col("dot_balls") / F.col("balls") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("boundary_pct",
        F.round(F.when(F.col("balls") > 0, F.col("boundaries") / F.col("balls") * 100)
                 .otherwise(F.lit(None)), 2))
    .withColumn("dismissal_rate",
        F.round(F.when(F.col("balls") > 0, F.col("dismissals") / F.col("balls"))
                 .otherwise(F.lit(None)), 4))
    .filter(F.col("balls") >= MIN_MATCHUP_BALLS)
    .orderBy(F.desc("balls"))
)

matchup.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(TABLES["matchup"])
print(f"✅ gold_matchup — {spark.table(TABLES['matchup']).count():,} rows")

# COMMAND ----------
# MAGIC %md ## Table 9 — gold_anomaly_feed

# COMMAND ----------

# ── 9a. Batter anomalies ──────────────────────────────────────────────────────
batter_baseline = spark.table(TABLES["batter_career"]).select(
    "batter", "batter_id",
    "career_avg_sr", "career_stddev_sr",
    F.col("strike_rate").alias("career_sr"),
    F.col("batting_avg").alias("career_avg"),
    F.col("dot_pct").alias("career_dot_pct"),
    F.col("boundary_pct").alias("career_boundary_pct"),
    F.col("matches").alias("career_matches"),
    "consistency_score", "consistency_label",
)

batter_rank_w = Window.partitionBy("batter").orderBy(F.desc("match_date"))
recent_batter = (
    spark.table(TABLES["batter_by_match"])
    .withColumn("match_rank", F.dense_rank().over(batter_rank_w))
    .filter(F.col("match_rank") <= RECENT_WINDOW)
    .groupBy("batter", "batter_id")
    .agg(
        F.sum("runs")           .alias("recent_runs"),
        F.sum("balls_faced")    .alias("recent_balls"),
        F.sum("dot_balls")      .alias("recent_dots"),
        F.sum("boundaries")     .alias("recent_boundaries"),
        F.sum("is_out")         .alias("recent_dismissals"),
        F.count("*")            .alias("recent_matches"),
        F.avg("strike_rate")    .alias("recent_sr"),
        F.avg("dot_pct")        .alias("recent_dot_pct"),
    )
    .withColumn("recent_avg",
        F.round(F.when(F.col("recent_dismissals") > 0,
                       F.col("recent_runs") / F.col("recent_dismissals"))
                 .otherwise(F.col("recent_runs").cast("double")), 2))
)

batter_anomaly = (
    recent_batter.join(batter_baseline, on=["batter", "batter_id"], how="inner")
    .withColumn("sr_delta",
        F.round(F.col("recent_sr") - F.col("career_avg_sr"), 1))
    .withColumn("dot_delta",
        F.round(F.col("recent_dot_pct") - F.col("career_dot_pct"), 1))
    .withColumn("sr_zscore",
        F.round(
            F.when(F.col("career_stddev_sr") > 1.0,
                   F.col("sr_delta") / F.col("career_stddev_sr"))
             .when(F.col("career_avg_sr") > 0,
                   F.col("sr_delta") / (F.col("career_avg_sr") * 0.20))
             .otherwise(F.lit(0)), 2))
    .withColumn("anomaly_type",
        F.when(F.col("sr_zscore") >= ZSCORE_THRESHOLD,  "SR_SPIKE")
         .when(F.col("sr_zscore") <= -ZSCORE_THRESHOLD, "SR_DROP")
         .when(F.col("dot_delta") >= 10,                "DOT_SPIKE")
         .when(F.col("dot_delta") <= -10,               "DOT_DROP")
         .otherwise("NORMAL"))
    .withColumn("anomaly_flag",
        (F.abs(F.col("sr_zscore")) >= ZSCORE_THRESHOLD) |
        (F.abs(F.col("dot_delta")) >= 10))
    .withColumn("severity",
        F.when(F.abs(F.col("sr_zscore")) >= 3.0, "HIGH")
         .when(F.abs(F.col("sr_zscore")) >= 2.0, "MEDIUM")
         .when(F.abs(F.col("sr_zscore")) >= ZSCORE_THRESHOLD, "LOW")
         .otherwise("NORMAL"))
    .withColumn("entity_type", F.lit("batter"))
    .withColumn("checked_at",  F.lit(datetime.utcnow().isoformat()))
    .select(
        F.col("batter").alias("player"),
        F.col("batter_id").alias("player_id"),
        "entity_type", "career_matches", "recent_matches",
        F.col("career_avg_sr").alias("career_metric"),
        F.col("recent_sr").alias("recent_metric"),
        "sr_delta", "sr_zscore",
        "career_dot_pct", "recent_dot_pct", "dot_delta",
        "consistency_score", "consistency_label",
        "anomaly_flag", "anomaly_type", "severity", "checked_at",
    )
)

# ── 9b. Bowler anomalies ──────────────────────────────────────────────────────
bowler_baseline = spark.table(TABLES["bowler_career"]).select(
    "bowler", "bowler_id",
    "career_avg_economy", "career_stddev_economy",
    F.col("economy").alias("career_economy"),
    F.col("dot_pct").alias("career_dot_pct"),
    F.col("matches").alias("career_matches"),
)

bowler_rank_w = Window.partitionBy("bowler").orderBy(F.desc("match_date"))
recent_bowler = (
    spark.table(TABLES["bowler_by_match"])
    .withColumn("match_rank", F.dense_rank().over(bowler_rank_w))
    .filter(F.col("match_rank") <= RECENT_WINDOW)
    .groupBy("bowler", "bowler_id")
    .agg(
        F.count("*")         .alias("recent_matches"),
        F.avg("economy")     .alias("recent_economy"),
        F.avg("dot_pct")     .alias("recent_dot_pct"),
        F.sum("wickets")     .alias("recent_wickets"),
    )
)

bowler_anomaly = (
    recent_bowler.join(bowler_baseline, on=["bowler", "bowler_id"], how="inner")
    .withColumn("economy_delta",
        F.round(F.col("recent_economy") - F.col("career_avg_economy"), 2))
    .withColumn("dot_delta",
        F.round(F.col("recent_dot_pct") - F.col("career_dot_pct"), 1))
    .withColumn("economy_zscore",
        F.round(
            F.when(F.col("career_stddev_economy") > 0.1,
                   F.col("economy_delta") / F.col("career_stddev_economy"))
             .when(F.col("career_avg_economy") > 0,
                   F.col("economy_delta") / (F.col("career_avg_economy") * 0.15))
             .otherwise(F.lit(0)), 2))
    .withColumn("anomaly_type",
        F.when(F.col("economy_zscore") >= ZSCORE_THRESHOLD,  "ECONOMY_SPIKE")
         .when(F.col("economy_zscore") <= -ZSCORE_THRESHOLD, "ECONOMY_DROP")
         .when(F.col("dot_delta") <= -10,                    "DOT_DROP")
         .when(F.col("dot_delta") >= 10,                     "DOT_SPIKE")
         .otherwise("NORMAL"))
    .withColumn("anomaly_flag",
        (F.abs(F.col("economy_zscore")) >= ZSCORE_THRESHOLD) |
        (F.abs(F.col("dot_delta")) >= 10))
    .withColumn("severity",
        F.when(F.abs(F.col("economy_zscore")) >= 3.0, "HIGH")
         .when(F.abs(F.col("economy_zscore")) >= 2.0, "MEDIUM")
         .when(F.abs(F.col("economy_zscore")) >= ZSCORE_THRESHOLD, "LOW")
         .otherwise("NORMAL"))
    .withColumn("entity_type", F.lit("bowler"))
    .withColumn("checked_at",  F.lit(datetime.utcnow().isoformat()))
    .select(
        F.col("bowler").alias("player"),
        F.col("bowler_id").alias("player_id"),
        "entity_type", "career_matches", "recent_matches",
        F.col("career_avg_economy").alias("career_metric"),
        F.col("recent_economy").alias("recent_metric"),
        F.col("economy_delta").alias("sr_delta"),
        F.col("economy_zscore").alias("sr_zscore"),
        "career_dot_pct", "recent_dot_pct", "dot_delta",
        F.lit(None).cast("double").alias("consistency_score"),
        F.lit(None).cast("string").alias("consistency_label"),
        "anomaly_flag", "anomaly_type", "severity", "checked_at",
    )
)

anomaly_feed = batter_anomaly.unionByName(bowler_anomaly)
anomaly_feed.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(TABLES["anomaly_feed"])

flagged = spark.table(TABLES["anomaly_feed"]).filter(F.col("anomaly_flag") == True).count()
print(f"✅ gold_anomaly_feed — {spark.table(TABLES['anomaly_feed']).count():,} rows | 🚨 {flagged:,} anomalies flagged")
display(
    spark.table(TABLES["anomaly_feed"])
    .filter(F.col("anomaly_flag") == True)
    .orderBy(F.desc(F.abs(F.col("sr_zscore"))))
    .limit(20)
)

# COMMAND ----------
# MAGIC %md ## Step 10 — Enrich career tables with player registry (second data source join)

# COMMAND ----------

registry = spark.table(REGISTRY_TABLE).select(
    "player_id", "unique_name", "key_cricinfo", "key_cricketarchive", "cricinfo_url"
)

# Enrich batter career — assign to variable first to avoid DataFrame column resolution error
batter_career_df = spark.table(TABLES["batter_career"])
batter_enriched = (
    batter_career_df
    .join(registry, batter_career_df["batter_id"] == registry["player_id"], how="left")
    .drop("player_id")
)
batter_enriched.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(TABLES["batter_career"])
print(f"✅ gold_batter_career enriched — cricinfo_url added")
print(f"   Players with cricinfo URL: {batter_enriched.filter(F.col('cricinfo_url').isNotNull()).count():,}")

# Enrich bowler career
bowler_career_df = spark.table(TABLES["bowler_career"])
bowler_enriched = (
    bowler_career_df
    .join(registry, bowler_career_df["bowler_id"] == registry["player_id"], how="left")
    .drop("player_id")
)
bowler_enriched.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(TABLES["bowler_career"])
print(f"✅ gold_bowler_career enriched — cricinfo_url added")
print(f"   Bowlers with cricinfo URL: {bowler_enriched.filter(F.col('cricinfo_url').isNotNull()).count():,}")

# COMMAND ----------
# MAGIC %md ## DQ Checks + Quality Log

# COMMAND ----------

run_ts   = datetime.utcnow().isoformat()
log_rows = []

def chk(key, name, value, expect_positive=False):
    status = ("PASS" if value > 0 else "FAIL") if expect_positive else ("PASS" if value == 0 else "WARN")
    icon   = "✅" if status == "PASS" else ("⚠️ " if status == "WARN" else "❌")
    print(f"{icon} [{key}] {name}: {value:,}  →  {status}")
    log_rows.append({"run_ts": run_ts, "table_name": TABLES[key],
                     "check_name": name, "value": int(value), "status": status})

print(f"\n📊 Gold DQ Checks — {run_ts}\n")

bc  = spark.table(TABLES["batter_career"])
boc = spark.table(TABLES["bowler_career"])
chk("batter_career",   "row_count",             bc.count(),  expect_positive=True)
chk("batter_career",   "null_impact_score",      bc.filter(F.col("impact_score").isNull()).count())
chk("batter_career",   "has_cricinfo_url",       0 if bc.filter(F.col("cricinfo_url").isNotNull()).count() > 0 else 1)
chk("bowler_career",   "row_count",             boc.count(), expect_positive=True)
chk("bowler_career",   "null_economy",           boc.filter(F.col("economy").isNull()).count())
chk("bowler_career",   "has_cricinfo_url",       0 if boc.filter(F.col("cricinfo_url").isNotNull()).count() > 0 else 1)

bbm  = spark.table(TABLES["batter_by_match"])
bobm = spark.table(TABLES["bowler_by_match"])
chk("batter_by_match", "row_count",             bbm.count(),  expect_positive=True)
chk("batter_by_match", "null_strike_rate",       bbm.filter(F.col("strike_rate").isNull()).count())
chk("bowler_by_match", "row_count",             bobm.count(), expect_positive=True)

bbp    = spark.table(TABLES["batter_by_phase"])
phases = {r[0] for r in bbp.select("phase").distinct().collect()}
chk("batter_by_phase", "row_count",             bbp.count(),  expect_positive=True)
chk("batter_by_phase", "all_phases_present",     0 if {"Powerplay","Middle","Death"} == phases else 1)

pm = spark.table(TABLES["phase_momentum"])
chk("phase_momentum",  "row_count",             pm.count(),   expect_positive=True)

mu = spark.table(TABLES["matchup"])
chk("matchup",         "row_count",             mu.count(),   expect_positive=True)
chk("matchup",         "null_strike_rate",       mu.filter(F.col("strike_rate").isNull()).count())

af = spark.table(TABLES["anomaly_feed"])
chk("anomaly_feed",    "row_count",             af.count(),   expect_positive=True)
chk("anomaly_feed",    "flagged_anomalies",      af.filter(F.col("anomaly_flag") == True).count(), expect_positive=True)
chk("anomaly_feed",    "both_entity_types",      0 if {"batter","bowler"} ==
                                                     {r[0] for r in af.select("entity_type").distinct().collect()}
                                                 else 1)

spark.createDataFrame(log_rows).write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(QUALITY_TABLE)
print(f"\n✅ DQ log written → {QUALITY_TABLE}")

# COMMAND ----------
# MAGIC %md ## Final Summary

# COMMAND ----------

print("=" * 65)
print("GOLD LAYER COMPLETE")
print("=" * 65)
for key, table in TABLES.items():
    count = spark.table(table).count()
    print(f"  {key:<20} {table.split('.')[-1]:<45} {count:>8,} rows")
print("=" * 65)

# COMMAND ----------
# MAGIC %md ## Sanity Previews

# COMMAND ----------

print("=== Top 10 Batters by Career Runs ===")
display(
    spark.table(TABLES["batter_career"])
    .select("batter", "matches", "runs", "strike_rate", "batting_avg",
            "consistency_label", "form_trajectory", "impact_score", "cricinfo_url")
    .orderBy(F.desc("runs"))
    .limit(10)
)

# COMMAND ----------

print("=== Top Anomalies (HOT FORM) ===")
display(
    spark.table(TABLES["anomaly_feed"])
    .filter((F.col("anomaly_flag") == True) & (F.col("sr_zscore") > 0))
    .select("player", "entity_type", "career_metric", "recent_metric",
            "sr_delta", "sr_zscore", "severity")
    .orderBy(F.desc("sr_zscore"))
    .limit(10)
)

# COMMAND ----------

print("=== Death Over Specialists (min 50 balls) ===")
display(
    spark.table(TABLES["batter_by_phase"])
    .filter((F.col("phase") == "Death") & (F.col("balls_faced") >= 50))
    .select("batter", "matches", "balls_faced", "runs", "strike_rate",
            "boundary_pct", "dot_pct")
    .orderBy(F.desc("strike_rate"))
    .limit(10)
)
