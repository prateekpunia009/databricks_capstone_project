# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver Layer: Enrichment & Cleaning
# MAGIC
# MAGIC **Reads from:** `tabular.dataexpert.prateek_capstone_project_bronze_deliveries`
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Filters to male T20 matches, excludes super overs
# MAGIC - Casts match_date to DateType
# MAGIC - Adds derived columns: phase, ball-type flags, batting/bowling metrics
# MAGIC - Computes batting position per innings using Window functions
# MAGIC - Applies correct cricket stat logic: wide/bye exclusions, runs_for_bowler
# MAGIC - Writes enriched table to Silver, Z-ORDERED for fast player lookups
# MAGIC - Runs DQ checks and appends to quality log
# MAGIC
# MAGIC **Business rules applied here:**
# MAGIC - `is_wide`          → extras_wides IS NOT NULL
# MAGIC - `is_noball`        → extras_noballs IS NOT NULL
# MAGIC - `is_legal`         → NOT wide AND NOT no-ball (counts as bowler's ball)
# MAGIC - `is_boundary`      → runs_batter IN (4,6)
# MAGIC - `is_dot_ball`      → runs_batter = 0 AND NOT wide AND NOT no-ball
# MAGIC - `runs_for_bowler`  → runs_total - byes - legbyes (byes not bowler's fault)
# MAGIC - `is_bowler_wicket` → is_wicket AND wicket_kind NOT IN ('run out')
# MAGIC - `phase`            → powerplay (0–5) / middle (6–14) / death (15–19)
# MAGIC
# MAGIC **Output table:** `tabular.dataexpert.prateek_capstone_project_silver_deliveries`
# MAGIC **Quality log:**  `tabular.dataexpert.prateek_capstone_project_silver_quality_log`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DateType
from datetime import datetime

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG       = "tabular"
SCHEMA        = "dataexpert"
BRONZE_TABLE  = f"{CATALOG}.{SCHEMA}.prateek_capstone_project_bronze_deliveries"
SILVER_TABLE  = f"{CATALOG}.{SCHEMA}.prateek_capstone_project_silver_deliveries"
QUALITY_TABLE = f"{CATALOG}.{SCHEMA}.prateek_capstone_project_silver_quality_log"

print(f"✅ Config ready")
print(f"   Reading  : {BRONZE_TABLE}")
print(f"   Writing  : {SILVER_TABLE}")

# COMMAND ----------
# MAGIC %md ## Step 1 — Read Bronze table

# COMMAND ----------

bronze = spark.table(BRONZE_TABLE)
bronze_male_count = bronze.filter(F.col("gender") == "male").count()

print(f"✅ Total bronze rows    : {bronze.count():,}")
print(f"   Male bronze rows    : {bronze_male_count:,}")
print(f"   Genders present     : {[r[0] for r in bronze.select('gender').distinct().collect()]}")
print(f"   Seasons present     : {sorted([r[0] for r in bronze.select('season').distinct().collect()])}")

# COMMAND ----------
# MAGIC %md ## Step 2 — Filter: male T20 only, exclude super overs

# COMMAND ----------

silver = (
    bronze
    .filter(F.col("gender")        == "male")
    .filter(F.col("match_type")    == "T20")
    .filter(F.col("is_super_over") == False)
)

print(f"✅ After filter (male T20, no super overs): {silver.count():,} rows")

# COMMAND ----------
# MAGIC %md ## Step 3 — Type casting

# COMMAND ----------

silver = (
    silver
    .withColumn("match_date", F.col("match_date").cast(DateType()))
    .withColumn("season",     F.col("season").cast("string"))
)

print("✅ Types cast: match_date → DateType")

# COMMAND ----------
# MAGIC %md ## Step 4 — Add phase column

# COMMAND ----------

# T20 phase definitions (standard cricket analytics):
#   Powerplay : overs 1–6   (index 0–5)  — fielding restrictions
#   Middle    : overs 7–15  (index 6–14) — consolidation / acceleration
#   Death     : overs 16–20 (index 15–19)— big hitting
silver = silver.withColumn(
    "phase",
    F.when(F.col("over_number") <= 5,  F.lit("Powerplay"))
     .when(F.col("over_number") <= 14, F.lit("Middle"))
     .otherwise(F.lit("Death"))
)

# COMMAND ----------
# MAGIC %md ## Step 5 — Add ball-type flags

# COMMAND ----------

silver = silver.withColumn(
    "is_wide",   F.col("extras_wides").isNotNull()
).withColumn(
    "is_noball", F.col("extras_noballs").isNotNull()
).withColumn(
    "is_bye",    F.col("extras_byes").isNotNull()
).withColumn(
    "is_legbye", F.col("extras_legbyes").isNotNull()
).withColumn(
    # Legal delivery = counts as one of bowler's 6 balls in the over
    "is_legal",
    (~F.col("extras_wides").isNotNull()) & (~F.col("extras_noballs").isNotNull())
)

# COMMAND ----------
# MAGIC %md ## Step 6 — Add batting metric flags

# COMMAND ----------

silver = silver.withColumn(
    "is_four",        F.col("runs_batter") == 4
).withColumn(
    "is_six",         F.col("runs_batter") == 6
).withColumn(
    "is_boundary",    F.col("runs_batter").isin(4, 6)
).withColumn(
    # Dot ball: 0 runs to batter on a legal delivery
    "is_dot_ball",
    (F.col("runs_batter") == 0) &
    (~F.col("extras_wides").isNotNull()) &
    (~F.col("extras_noballs").isNotNull())
).withColumn(
    "is_scoring_shot", F.col("runs_batter") > 0
)

# COMMAND ----------
# MAGIC %md ## Step 7 — Add bowling metric columns

# COMMAND ----------

silver = silver.withColumn(
    # Byes and leg-byes go on the scoreboard but are NOT the bowler's fault.
    # ICC and Cricinfo both use this formula for economy rate.
    "runs_for_bowler",
    F.col("runs_total") -
    F.coalesce(F.col("extras_byes"),    F.lit(0)) -
    F.coalesce(F.col("extras_legbyes"), F.lit(0))
).withColumn(
    # Run outs are NOT credited to the bowler — all other dismissal types are
    "is_bowler_wicket",
    F.col("is_wicket") & (~F.col("wicket_kind").isin(["run out"]))
).withColumn(
    # Did the batting team win the toss? (useful for toss impact analysis)
    "batting_team_won_toss",
    F.col("team_batting") == F.col("toss_winner")
)

# COMMAND ----------
# MAGIC %md ## Step 8 — Add batting position per innings

# COMMAND ----------

# Batting position = order in which each batter first appeared in the innings.
# Uses Window: find earliest over+ball faced, then rank across all batters in that innings.
first_ball_window = (
    Window.partitionBy("match_id", "innings_number", "batter")
          .orderBy("over_number", "ball_index")
)
position_window = (
    Window.partitionBy("match_id", "innings_number")
          .orderBy("_first_over", "_first_ball")
)

silver = (
    silver
    .withColumn("_first_over",  F.min("over_number").over(first_ball_window))
    .withColumn("_first_ball",  F.min("ball_index").over(first_ball_window))
    .withColumn("batting_position", F.dense_rank().over(position_window))
    .drop("_first_over", "_first_ball")
)

# COMMAND ----------
# MAGIC %md ## Step 9 — Add helper columns

# COMMAND ----------

silver = (
    silver
    .withColumn("match_year",   F.year(F.col("match_date")))
    .withColumn("over_1indexed", F.col("over_number") + 1)   # 1–20 for display
)

# COMMAND ----------
# MAGIC %md ## Step 10 — Final column selection

# COMMAND ----------

silver = silver.select(
    # Identifiers
    "match_id", "match_date", "match_year", "season",
    "gender", "match_type", "venue", "city", "event_name",
    "team_home", "team_away",
    # Toss & outcome
    "toss_winner", "toss_decision", "match_winner", "outcome_result",
    "is_dl_match", "data_missing_flag", "batting_team_won_toss",
    # Innings context
    "innings_number", "team_batting", "is_super_over",
    # Delivery position
    "over_number", "over_1indexed", "ball_index", "phase",
    # Players
    "batter", "batter_id", "batting_position",
    "bowler", "bowler_id", "non_striker",
    # Runs
    "runs_batter", "runs_extras", "runs_total", "runs_for_bowler",
    # Extras breakdown
    "extras_wides", "extras_noballs", "extras_byes", "extras_legbyes",
    # Ball-type flags
    "is_wide", "is_noball", "is_bye", "is_legbye", "is_legal",
    # Batting flags
    "is_four", "is_six", "is_boundary", "is_dot_ball", "is_scoring_shot",
    # Wicket
    "is_wicket", "is_bowler_wicket", "wicket_kind", "player_out",
)

print(f"✅ Final silver schema: {len(silver.columns)} columns")
silver.printSchema()

# COMMAND ----------
# MAGIC %md ## Step 11 — Write to Delta silver table

# COMMAND ----------

(
    silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("season")
    .saveAsTable(SILVER_TABLE)
)

# Z-ORDER for the most common Gold query pattern: filter by player
spark.sql(f"OPTIMIZE {SILVER_TABLE} ZORDER BY (batter_id, bowler_id, match_date)")
print(f"✅ Silver table written and optimized: {SILVER_TABLE}")

# COMMAND ----------
# MAGIC %md ## Step 12 — Data quality checks + append to log

# COMMAND ----------

sv     = spark.table(SILVER_TABLE)
run_ts = datetime.utcnow().isoformat()
total  = sv.count()

checks = [
    ("total_rows",              total),
    ("distinct_matches",        sv.select("match_id").distinct().count()),
    ("distinct_players",        sv.select("batter").distinct().count()),
    # Row count must be <= bronze male rows (we only filter, never add rows)
    ("silver_lte_bronze_male",  0 if total <= bronze_male_count else 1),
    ("null_batter",             sv.filter(F.col("batter").isNull()).count()),
    ("null_bowler",             sv.filter(F.col("bowler").isNull()).count()),
    ("null_match_date",         sv.filter(F.col("match_date").isNull()).count()),
    ("null_phase",              sv.filter(F.col("phase").isNull()).count()),
    ("null_batting_position",   sv.filter(F.col("batting_position").isNull()).count()),
    ("female_rows_leaked",      sv.filter(F.col("gender") != "male").count()),
    ("super_over_rows_leaked",  sv.filter(F.col("is_super_over") == True).count()),
    ("negative_runs_for_bowler", sv.filter(F.col("runs_for_bowler") < 0).count()),
    ("all_phases_present",      0 if {"Powerplay","Middle","Death"} ==
                                    {r[0] for r in sv.select("phase").distinct().collect()}
                                else 1),
]

def get_status(name, value):
    if name in ("total_rows", "distinct_matches", "distinct_players"):
        return "PASS" if value > 0 else "FAIL"
    return "PASS" if value == 0 else "WARN"

log_rows = [
    {
        "run_ts":     run_ts,
        "table_name": SILVER_TABLE,
        "check_name": name,
        "value":      int(value),
        "status":     get_status(name, value),
    }
    for name, value in checks
]

print(f"\n📊 DQ Check Results — {run_ts}")
print(f"{'Check':<30} {'Value':>12}  Status")
print("─" * 55)
for row in log_rows:
    icon = "✅" if row["status"] == "PASS" else "⚠️ " if row["status"] == "WARN" else "❌"
    print(f"{icon} {row['check_name']:<28} {row['value']:>12,}  {row['status']}")

log_df = spark.createDataFrame(log_rows)
(
    log_df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(QUALITY_TABLE)
)

print(f"\n✅ DQ log appended → {QUALITY_TABLE}")
print(f"   Total log entries so far: {spark.table(QUALITY_TABLE).count():,}")

# COMMAND ----------
# MAGIC %md ## Step 13 — Sanity checks

# COMMAND ----------

print("Phase distribution:")
sv.groupBy("phase").agg(
    F.count("*").alias("deliveries"),
    F.sum(F.col("is_wicket").cast("int")).alias("wickets"),
    F.sum(F.col("is_boundary").cast("int")).alias("boundaries"),
    F.sum(F.col("is_dot_ball").cast("int")).alias("dot_balls"),
    F.round(F.avg("runs_batter"), 3).alias("avg_runs_per_ball")
).orderBy(
    F.when(F.col("phase") == "Powerplay", 1)
     .when(F.col("phase") == "Middle", 2)
     .otherwise(3)
).show()

print("\nTop 10 batters by deliveries faced:")
sv.groupBy("batter").count().orderBy(F.desc("count")).limit(10).show()
