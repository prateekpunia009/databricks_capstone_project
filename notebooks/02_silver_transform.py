# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 02 — Silver Layer: Enrichment & Business Logic
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Reads from `bronze_deliveries`
# MAGIC - Filters to `gender = 'male'` (anomaly baselines must be gender-consistent)
# MAGIC - Adds all business-rule-derived flags that downstream Gold tables need
# MAGIC - Applies correct cricket stat logic: wide exclusions, bye/legbye exclusions, non_boundary override
# MAGIC - Runs integrity checks between Silver and Bronze
# MAGIC
# MAGIC **Business rules applied here (see plan for full rationale):**
# MAGIC - `is_wide`     → extras_wides IS NOT NULL
# MAGIC - `is_noball`   → extras_noballs IS NOT NULL
# MAGIC - `is_legal`    → NOT wide AND NOT noball  (counts as a bowler's ball)
# MAGIC - `is_boundary` → runs_batter IN (4,6) AND non_boundary IS NOT True
# MAGIC - `is_dot_ball` → runs_batter = 0 AND NOT wide AND NOT bye AND NOT legbye
# MAGIC - `runs_for_bowler` → runs_total - byes - legbyes  (byes/legbyes not bowler's fault)
# MAGIC - `phase`       → powerplay (0-5) / middle (6-14) / death (15-19)
# MAGIC
# MAGIC **Output table:** `capstone.cricket.silver_deliveries`
# MAGIC **Quality log:**  `capstone.cricket.silver_quality_log`

# COMMAND ----------

CATALOG       = "capstone"
SCHEMA        = "cricket"
BRONZE_TABLE  = f"{CATALOG}.{SCHEMA}.bronze_deliveries"
SILVER_TABLE  = f"{CATALOG}.{SCHEMA}.silver_deliveries"
QUALITY_TABLE = f"{CATALOG}.{SCHEMA}.silver_quality_log"

# COMMAND ----------
# MAGIC %md ## 1. Read Bronze and filter to male internationals

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

bronze_df = spark.table(BRONZE_TABLE)

# Filter to male gender — female T20 benchmarks are fundamentally different
# and mixing them would make anomaly scores meaningless
df = bronze_df.filter(F.col("gender") == "male")

bronze_male_count = df.count()
print(f"Bronze male deliveries: {bronze_male_count:,}")
print(f"Bronze female deliveries (retained in bronze, not in silver): "
      f"{bronze_df.filter(F.col('gender') == 'female').count():,}")

# COMMAND ----------
# MAGIC %md ## 2. Add delivery-level flags (the core business logic)

# COMMAND ----------

silver_df = df.withColumns({

    # ── Extras flags ─────────────────────────────────────────────────────────
    # extras_wides/noballs/byes/legbyes are INT when present, NULL when absent
    "is_wide"      : F.col("extras_wides").isNotNull(),
    "is_noball"    : F.col("extras_noballs").isNotNull(),
    "is_bye"       : F.col("extras_byes").isNotNull(),
    "is_legbye"    : F.col("extras_legbyes").isNotNull(),

    # ── Legal delivery flag ───────────────────────────────────────────────────
    # A "legal" delivery counts as a ball in the bowler's over
    # Wides and no-balls must be re-bowled; they don't count as legal balls
    "is_legal"     : (~F.col("extras_wides").isNotNull()) &
                     (~F.col("extras_noballs").isNotNull()),

    # ── Boundary flag (honours the non_boundary override) ────────────────────
    # non_boundary = True means the umpire signalled boundary but fielder stopped it.
    # Those 4/6 runs are valid but should NOT be counted as boundaries.
    "is_boundary"  : F.col("runs_batter").isin([4, 6]) & (~F.col("non_boundary")),

    # ── Six flag (for boundary type analysis) ────────────────────────────────
    "is_six"       : (F.col("runs_batter") == 6) & (~F.col("non_boundary")),

    # ── Dot ball ─────────────────────────────────────────────────────────────
    # A dot is 0 runs to the batter, on a legal delivery
    # (byes and legbyes are also dots for the batter even though team scores)
    "is_dot_ball"  : (F.col("runs_batter") == 0) &
                     (~F.col("extras_wides").isNotNull()) &
                     (~F.col("extras_noballs").isNotNull()),

    # ── Runs charged to bowler ────────────────────────────────────────────────
    # Byes and leg-byes are on the scoreboard but NOT the bowler's responsibility.
    # This is how Cricinfo and ICC officially compute economy rate.
    "runs_for_bowler" : F.col("runs_total") -
                        F.coalesce(F.col("extras_byes"),    F.lit(0)) -
                        F.coalesce(F.col("extras_legbyes"), F.lit(0)),

    # ── Bowler wicket flag ───────────────────────────────────────────────────
    # Run outs are NOT credited to the bowler. All other wicket types are.
    "is_bowler_wicket" : F.col("is_wicket") &
                         (~F.col("wicket_kind").isin(["run out"])),

    # ── Game phase ───────────────────────────────────────────────────────────
    # Powerplay: fielding restrictions in first 6 overs (0-5 in 0-indexed)
    # Middle overs: building platform, variation bowling
    # Death overs: maximum aggression batting, yorkers/bouncers
    "phase" : F.when(F.col("over_number") <= 5,  "powerplay")
               .when(F.col("over_number") <= 14, "middle")
               .otherwise("death"),

    # ── Toss advantage flag ───────────────────────────────────────────────────
    # Did the batting team win the toss?
    "batting_team_won_toss" : F.col("team_batting") == F.col("toss_winner"),
})

print("✅ Silver enrichment columns added")
silver_df.printSchema()

# COMMAND ----------
# MAGIC %md ## 3. Write to Delta (Z-ORDER for fast player lookups)

# COMMAND ----------

(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("season")
    .saveAsTable(SILVER_TABLE)
)

# Optimise for the most common Gold query patterns: filter by player + date range
spark.sql(f"OPTIMIZE {SILVER_TABLE} ZORDER BY (batter_id, bowler_id, date)")
print(f"✅ Silver table written and optimized: {SILVER_TABLE}")

# COMMAND ----------
# MAGIC %md ## 4. Data Quality Checks

# COMMAND ----------

from datetime import datetime

results = []
ts      = datetime.now().isoformat()
tbl     = spark.table(SILVER_TABLE)
total   = tbl.count()

def log(check, status, detail=""):
    icon = "✅" if status == "PASS" else ("⚠️" if status == "WARN" else "❌")
    print(f"{icon} [{status}] {check}: {detail}")
    results.append({
        "check_name" : check,
        "status"     : status,
        "detail"     : detail,
        "total_rows" : total,
        "checked_at" : ts,
        "table"      : SILVER_TABLE
    })

# ── Check 1: Silver rows ≥ bronze male rows (no data lost in enrichment) ─────
log("silver_row_count_vs_bronze",
    "PASS" if total >= bronze_male_count else "FAIL",
    f"Silver: {total:,} | Bronze male: {bronze_male_count:,}")

# ── Check 2: No null batter_id or bowler_id (registry lookup worked) ─────────
for col_name in ["batter_id", "bowler_id"]:
    n = tbl.filter(F.col(col_name).isNull()).count()
    # Some players in old matches may not have registry entries
    # Treat as WARN (not FAIL) since it's a data quality issue in the source
    log(f"null_{col_name}",
        "PASS" if n == 0 else "WARN",
        f"{n:,} nulls — check if Cricsheet registry covers these players")

# ── Check 3: Over count per innings BETWEEN 1 and 20 (NEVER assert = 20) ─────
# Short chases, all-out innings, rain interruptions all produce < 20 overs.
# Super overs have exactly 1 over. Both are valid and must not fail.
bad_innings = (
    tbl
    .filter(~F.col("is_super_over"))  # exclude super overs from this check
    .groupBy("match_id", "innings_number")
    .agg(F.countDistinct("over_number").alias("over_count"))
    .filter(~F.col("over_count").between(1, 20))
    .count()
)
log("over_count_per_innings_1_to_20",
    "PASS" if bad_innings == 0 else "WARN",
    f"{bad_innings:,} innings with over count outside 1-20")

# ── Check 4: Player integrity — no player IDs dropped vs bronze ───────────────
# Every batter_id in silver must exist in bronze (we should only be filtering, not losing data)
bronze_batters = {r[0] for r in
    spark.table(BRONZE_TABLE)
         .filter(F.col("gender") == "male")
         .select("batter_id")
         .distinct()
         .collect()
    if r[0] is not None
}
silver_batters = {r[0] for r in
    tbl.select("batter_id").distinct().collect()
    if r[0] is not None
}
lost_batters = bronze_batters - silver_batters
log("player_integrity_batter_ids",
    "PASS" if len(lost_batters) == 0 else "WARN",
    f"{len(lost_batters):,} batter_ids in bronze but not in silver: "
    f"{list(lost_batters)[:5]}{'...' if len(lost_batters) > 5 else ''}")

# ── Check 5: All 3 phases present ────────────────────────────────────────────
phases = {r[0] for r in tbl.select("phase").distinct().collect()}
log("all_phases_present",
    "PASS" if phases == {"powerplay", "middle", "death"} else "FAIL",
    f"Phases found: {sorted(phases)}")

# ── Check 6: runs_for_bowler should always be >= 0 ────────────────────────────
neg_runs = tbl.filter(F.col("runs_for_bowler") < 0).count()
log("runs_for_bowler_non_negative",
    "PASS" if neg_runs == 0 else "WARN",
    f"{neg_runs:,} deliveries with negative runs_for_bowler")

# ── Write quality log ──────────────────────────────────────────────────────────
spark.createDataFrame(results).write.format("delta").mode("append").saveAsTable(QUALITY_TABLE)
print(f"\n📋 Quality log written to: {QUALITY_TABLE}")

# COMMAND ----------
# MAGIC %md ## 5. Preview

# COMMAND ----------

display(spark.table(SILVER_TABLE).limit(10))

# COMMAND ----------

# Phase distribution — a useful sanity check
print("=== Delivery distribution by phase ===")
display(
    spark.table(SILVER_TABLE)
    .groupBy("phase")
    .agg(
        F.count("*").alias("deliveries"),
        F.sum(F.col("is_wicket").cast("int")).alias("wickets"),
        F.sum(F.col("is_boundary").cast("int")).alias("boundaries"),
        F.sum(F.col("is_dot_ball").cast("int")).alias("dot_balls"),
        F.round(F.avg("runs_batter"), 3).alias("avg_runs_per_ball")
    )
    .orderBy(
        F.when(F.col("phase") == "powerplay", 1)
         .when(F.col("phase") == "middle", 2)
         .otherwise(3)
    )
)
