# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 03 — Gold Layer: Player Aggregates & Anomaly Detection
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC Builds 5 Gold tables from Silver — all exclude super overs from career stats.
# MAGIC
# MAGIC | Table | Description |
# MAGIC |---|---|
# MAGIC | `gold_batting_by_match`  | Per-batter per-match stats (runs, SR, boundaries, dots) |
# MAGIC | `gold_bowling_by_match`  | Per-bowler per-match stats (economy, wickets) |
# MAGIC | `gold_phase_stats`       | Per-batter phase breakdown (powerplay / middle / death) |
# MAGIC | `gold_player_career`     | Career baselines + rolling 10-match form (min 5 matches) |
# MAGIC | `gold_anomaly_flags`     | Players where |z-score| > 2σ (significant form shift) |
# MAGIC
# MAGIC **Anomaly logic:**
# MAGIC `z_score = (rolling_avg_sr - career_avg_sr) / career_stddev_sr`
# MAGIC Positive z → player in hot form. Negative z → form slump.

# COMMAND ----------

CATALOG       = "capstone"
SCHEMA        = "cricket"
SILVER_TABLE  = f"{CATALOG}.{SCHEMA}.silver_deliveries"

# Output tables
T_BAT  = f"{CATALOG}.{SCHEMA}.gold_batting_by_match"
T_BOWL = f"{CATALOG}.{SCHEMA}.gold_bowling_by_match"
T_PH   = f"{CATALOG}.{SCHEMA}.gold_phase_stats"
T_CAR  = f"{CATALOG}.{SCHEMA}.gold_player_career"
T_ANOM = f"{CATALOG}.{SCHEMA}.gold_anomaly_flags"

MIN_MATCHES_FOR_ANOMALY = 5   # Players with fewer matches excluded from anomaly

# COMMAND ----------
# MAGIC %md ## 1. Batting stats per player per match

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

silver = spark.table(SILVER_TABLE).filter(~F.col("is_super_over"))

batting_df = (
    silver
    .groupBy("batter_id", "batter", "match_id", "date", "season", "event_name", "source_dataset")
    .agg(
        # Runs
        F.sum("runs_batter").alias("runs"),

        # Balls faced = deliveries where batter actually faced (wides don't count)
        F.count(
            F.when(~F.col("is_wide"), F.lit(1))
        ).alias("balls_faced"),

        # Boundaries (non_boundary flag already handled in silver)
        F.sum(F.col("is_boundary").cast("int")).alias("boundaries"),
        F.sum(F.col("is_six").cast("int")).alias("sixes"),

        # Dot balls
        F.sum(F.col("is_dot_ball").cast("int")).alias("dot_balls"),

        # Wickets (how many times this batter was dismissed)
        F.sum(
            F.when(F.col("player_out") == F.col("batter"), F.lit(1)).otherwise(F.lit(0))
        ).alias("dismissed"),
    )
    .withColumn(
        # Strike rate: protect against 0 balls faced (e.g., retired hurt without facing)
        "strike_rate",
        F.round(
            F.col("runs") * 100.0 / F.nullif(F.col("balls_faced"), F.lit(0)),
            2
        )
    )
    .withColumn(
        "boundary_pct",
        F.round(
            F.col("boundaries") * 100.0 / F.nullif(F.col("balls_faced"), F.lit(0)),
            2
        )
    )
    .withColumn(
        "dot_ball_pct",
        F.round(
            F.col("dot_balls") * 100.0 / F.nullif(F.col("balls_faced"), F.lit(0)),
            2
        )
    )
)

batting_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_BAT)
print(f"✅ {T_BAT}: {batting_df.count():,} rows")

# COMMAND ----------
# MAGIC %md ## 2. Bowling stats per player per match

# COMMAND ----------

bowling_df = (
    silver
    .groupBy("bowler_id", "bowler", "match_id", "date", "season", "event_name", "source_dataset")
    .agg(
        # Legal balls only (wides and no-balls not counted as balls bowled)
        F.count(F.when(F.col("is_legal"), F.lit(1))).alias("balls_bowled"),

        # Runs conceded: excludes byes and leg-byes (not bowler's fault)
        F.sum("runs_for_bowler").alias("runs_conceded"),

        # Wickets: run outs are NOT the bowler's wicket
        F.sum(F.col("is_bowler_wicket").cast("int")).alias("wickets"),

        # Dot balls from bowler's perspective: 0 runs off legal delivery
        F.count(
            F.when(F.col("is_legal") & (F.col("runs_total") == 0), F.lit(1))
        ).alias("dot_balls"),

        # Wides and no-balls bowled
        F.sum(F.col("is_wide").cast("int")).alias("wides_bowled"),
        F.sum(F.col("is_noball").cast("int")).alias("noballs_bowled"),
    )
    .withColumn(
        # Economy = runs per over (6 balls)
        "economy_rate",
        F.round(
            F.col("runs_conceded") * 6.0 / F.nullif(F.col("balls_bowled"), F.lit(0)),
            2
        )
    )
    .withColumn(
        # Bowling strike rate = balls per wicket
        "bowling_strike_rate",
        F.round(
            F.col("balls_bowled") * 1.0 / F.nullif(F.col("wickets"), F.lit(0)),
            2
        )
    )
)

bowling_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_BOWL)
print(f"✅ {T_BOWL}: {bowling_df.count():,} rows")

# COMMAND ----------
# MAGIC %md ## 3. Phase breakdown per player (powerplay / middle / death)

# COMMAND ----------

phase_df = (
    silver
    .groupBy("batter_id", "batter", "phase")
    .agg(
        F.sum("runs_batter").alias("runs"),
        F.count(F.when(~F.col("is_wide"), F.lit(1))).alias("balls_faced"),
        F.sum(F.col("is_boundary").cast("int")).alias("boundaries"),
        F.sum(F.col("is_dot_ball").cast("int")).alias("dot_balls"),
        F.countDistinct("match_id").alias("matches_in_phase"),
    )
    .withColumn(
        "strike_rate",
        F.round(
            F.col("runs") * 100.0 / F.nullif(F.col("balls_faced"), F.lit(0)),
            2
        )
    )
    .withColumn(
        "dot_ball_pct",
        F.round(
            F.col("dot_balls") * 100.0 / F.nullif(F.col("balls_faced"), F.lit(0)),
            2
        )
    )
)

phase_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_PH)
print(f"✅ {T_PH}: {phase_df.count():,} rows")

# COMMAND ----------
# MAGIC %md ## 4. Player career baselines + rolling form (last 10 matches)

# COMMAND ----------

bat_match = spark.table(T_BAT)

# Window for rolling 10-match form (ordered by match date)
rolling_window = (
    Window
    .partitionBy("batter_id")
    .orderBy("date")
    .rowsBetween(-9, 0)   # current row + 9 preceding = 10-match window
)

# Window for career average up to each match (cumulative, not rolling)
career_window = (
    Window
    .partitionBy("batter_id")
    .orderBy("date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

career_df = (
    bat_match
    # ── Step 1: Career aggregates per player ──────────────────────────────────
    .groupBy("batter_id", "batter")
    .agg(
        F.countDistinct("match_id").alias("matches"),
        F.sum("runs").alias("career_runs"),
        F.avg("strike_rate").alias("career_avg_sr"),
        F.stddev("strike_rate").alias("career_stddev_sr"),
        F.avg("boundary_pct").alias("career_avg_boundary_pct"),
        F.max("runs").alias("highest_score"),
        # Count centuries (100+) and half-centuries (50+)
        F.sum(F.when(F.col("runs") >= 100, 1).otherwise(0)).alias("hundreds"),
        F.sum(F.when((F.col("runs") >= 50) & (F.col("runs") < 100), 1).otherwise(0)).alias("fifties"),
    )
    # ── Step 2: Filter to players with enough data for anomaly detection ──────
    .filter(F.col("matches") >= MIN_MATCHES_FOR_ANOMALY)
)

# ── Step 3: Rolling 10-match form — must be computed at match level first ─────
# (Can't do rolling after groupBy; re-join latest match form back)
latest_form = (
    bat_match
    .withColumn("rolling_avg_sr", F.avg("strike_rate").over(rolling_window))
    .withColumn("rolling_avg_boundary_pct", F.avg("boundary_pct").over(rolling_window))
    .withColumn(
        "match_rank",
        F.row_number().over(
            Window.partitionBy("batter_id").orderBy(F.desc("date"))
        )
    )
    .filter(F.col("match_rank") == 1)   # keep only the most recent match's rolling value
    .select("batter_id", "rolling_avg_sr", "rolling_avg_boundary_pct")
)

career_with_form = career_df.join(latest_form, on="batter_id", how="left")

career_with_form.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_CAR)
print(f"✅ {T_CAR}: {career_with_form.count():,} players")

# COMMAND ----------
# MAGIC %md ## 5. Anomaly flags — players with |z-score| > 2σ

# COMMAND ----------

# z-score = (recent_form - career_baseline) / career_std_deviation
# |z| > 2 means the player's recent form is 2 standard deviations from their career norm.
# Positive z = hot form. Negative z = slump.

anomaly_df = (
    spark.table(T_CAR)
    .withColumn(
        "anomaly_score",
        F.round(
            (F.col("rolling_avg_sr") - F.col("career_avg_sr")) /
            F.nullif(F.col("career_stddev_sr"), F.lit(0)),
            3
        )
    )
    .withColumn(
        "anomaly_direction",
        F.when(F.col("anomaly_score") > 0, "HOT_FORM").otherwise("SLUMP")
    )
    .filter(F.abs(F.col("anomaly_score")) > 2.0)
    .orderBy(F.abs(F.col("anomaly_score")).desc())
)

anomaly_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_ANOM)
print(f"✅ {T_ANOM}: {anomaly_df.count():,} players flagged")

# COMMAND ----------
# MAGIC %md ## 6. Preview all Gold tables

# COMMAND ----------

print("=== Top 10 Batters by Career Runs ===")
display(
    spark.table(T_CAR)
    .orderBy(F.desc("career_runs"))
    .select("batter", "matches", "career_runs", "career_avg_sr",
            "rolling_avg_sr", "highest_score", "hundreds", "fifties")
    .limit(10)
)

# COMMAND ----------

print("=== Anomaly Flagged Players ===")
display(
    spark.table(T_ANOM)
    .select("batter", "matches", "career_avg_sr", "rolling_avg_sr",
            "anomaly_score", "anomaly_direction")
)

# COMMAND ----------

print("=== Phase Breakdown — Top 10 Death-Over Batters ===")
display(
    spark.table(T_PH)
    .filter(F.col("phase") == "death")
    .filter(F.col("balls_faced") >= 50)  # min 50 death-over balls faced
    .orderBy(F.desc("strike_rate"))
    .select("batter", "matches_in_phase", "balls_faced", "runs", "strike_rate", "dot_ball_pct")
    .limit(10)
)

# COMMAND ----------

print("=== Best Bowling Economy (min 5 matches) ===")
display(
    spark.table(T_BOWL)
    .groupBy("bowler_id", "bowler")
    .agg(
        F.countDistinct("match_id").alias("matches"),
        F.sum("balls_bowled").alias("total_balls"),
        F.sum("wickets").alias("total_wickets"),
        F.round(
            F.sum("runs_conceded") * 6.0 / F.sum("balls_bowled"), 2
        ).alias("career_economy"),
    )
    .filter(F.col("matches") >= 5)
    .orderBy("career_economy")
    .limit(10)
)
