# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 01 — Bronze Layer: Raw Ingestion
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Reads raw T20 match JSON files from Unity Catalog Volumes (both datasets)
# MAGIC - Flattens the deeply nested structure into one row per delivery (the atomic fact)
# MAGIC - Extracts stable player IDs from the Cricsheet registry map
# MAGIC - Tags all edge cases: super overs, D/L matches, missing data flags
# MAGIC - Runs data quality checks and logs results to a Delta table
# MAGIC
# MAGIC **Output table:** `capstone.cricket.bronze_deliveries`
# MAGIC **Quality log:**  `capstone.cricket.bronze_quality_log`

# COMMAND ----------
# MAGIC %md ## Config — update paths if your Volume is named differently

# COMMAND ----------

CATALOG    = "capstone"
SCHEMA     = "cricket"

# Unity Catalog Volume paths (upload your local folders here before running)
VOL_T20    = f"/Volumes/{CATALOG}/raw/t20s_json"
VOL_WC     = f"/Volumes/{CATALOG}/raw/icc_wc_t20"

# Output tables
BRONZE_TABLE  = f"{CATALOG}.{SCHEMA}.bronze_deliveries"
QUALITY_TABLE = f"{CATALOG}.{SCHEMA}.bronze_quality_log"

# COMMAND ----------
# MAGIC %md ## 0. Setup — create catalog and schema if they don't exist

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"✅ Catalog and schema ready: {CATALOG}.{SCHEMA}")

# COMMAND ----------
# MAGIC %md ## 1. Read raw JSON from both sources

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType

# ── Source 1: main T20 dataset (5,100 matches) ──────────────────────────────
df_t20 = (
    spark.read
         .option("multiLine", "true")   # each file is one JSON object
         .json(VOL_T20)
         .withColumn("source_dataset", F.lit("t20s_main"))
         .withColumn("_source_file", F.input_file_name())   # capture BEFORE explode
)

# ── Source 2: ICC Men's T20 World Cup (230 matches) ─────────────────────────
df_wc = (
    spark.read
         .option("multiLine", "true")
         .json(VOL_WC)
         .withColumn("source_dataset", F.lit("icc_worldcup"))
         .withColumn("_source_file", F.input_file_name())
)

# ── Union — allow missing columns so schema differences don't break the read ─
df_raw = df_t20.unionByName(df_wc, allowMissingColumns=True)

print(f"Raw union row count (1 row = 1 match): {df_raw.count():,}")

# COMMAND ----------
# MAGIC %md ## 2. Extract match_id from filename and build registry map

# COMMAND ----------

df_raw = df_raw.withColumn(
    # Strip the full path, keep just the filename without .json
    "match_id",
    F.regexp_extract(F.col("_source_file"), r"([^/]+)\.json$", 1)
)

# info.registry.people is inferred by Spark as a Struct (dynamic keys → fields).
# We convert Struct → JSON string → MapType so we can do map[player_name] lookups.
df_raw = df_raw.withColumn(
    "registry_map",
    F.from_json(
        F.to_json(F.col("info.registry.people")),
        MapType(StringType(), StringType())
    )
)

# COMMAND ----------
# MAGIC %md ## 3. Flatten: match → innings (with position index)

# COMMAND ----------

# posexplode gives us the array index as innings_number (0 = first innings, etc.)
df_innings = (
    df_raw
    .select(
        "match_id",
        "source_dataset",
        "registry_map",
        "info",
        F.posexplode(F.col("innings")).alias("innings_number", "innings_data")
    )
)

# COMMAND ----------
# MAGIC %md ## 4. Flatten: innings → overs

# COMMAND ----------

df_overs = (
    df_innings
    .select(
        "match_id",
        "source_dataset",
        "registry_map",
        "info",
        "innings_number",
        "innings_data",
        F.explode(F.col("innings_data.overs")).alias("over_data")
    )
)

# COMMAND ----------
# MAGIC %md ## 5. Flatten: overs → deliveries (with position index for ball_index)

# COMMAND ----------

df_deliveries = (
    df_overs
    .select(
        "match_id",
        "source_dataset",
        "registry_map",
        "info",
        "innings_number",
        "innings_data",
        F.col("over_data.over").alias("over_number"),
        F.posexplode(F.col("over_data.deliveries")).alias("ball_index", "delivery")
    )
)

# COMMAND ----------
# MAGIC %md ## 6. Select and rename all columns into the Bronze schema

# COMMAND ----------

bronze_df = df_deliveries.select(

    # ── Match identifiers ────────────────────────────────────────────────────
    F.col("match_id"),
    F.col("source_dataset"),
    F.col("info.dates")[0].cast("date").alias("date"),
    F.col("info.season").cast("string").alias("season"),
    F.col("info.gender").alias("gender"),
    F.col("info.match_type").alias("match_type"),
    F.col("info.team_type").alias("team_type"),
    F.coalesce(F.col("info.venue"), F.lit("Unknown")).alias("venue"),
    F.col("info.city").alias("city"),
    F.col("info.event.name").alias("event_name"),
    F.col("info.teams")[0].alias("team_home"),
    F.col("info.teams")[1].alias("team_away"),

    # ── Toss ─────────────────────────────────────────────────────────────────
    F.col("info.toss.winner").alias("toss_winner"),
    F.col("info.toss.decision").alias("toss_decision"),

    # ── Match outcome ─────────────────────────────────────────────────────────
    # winner is null for ties and no-results
    F.col("info.outcome.winner").alias("match_winner"),
    F.col("info.outcome.result").alias("outcome_result"),     # "tie" | "no result" | null
    (F.col("info.outcome.method") == "D/L").alias("is_dl_match"),

    # ── Innings context ───────────────────────────────────────────────────────
    F.col("innings_number"),
    # super_over is True for super over innings, absent (null) for normal innings
    F.coalesce(F.col("innings_data.super_over"), F.lit(False)).alias("is_super_over"),
    F.col("innings_data.team").alias("team_batting"),
    # DLS adjusted target (only present in second innings of D/L match)
    F.col("innings_data.target.runs").alias("dl_target_runs"),

    # ── Delivery position ────────────────────────────────────────────────────
    F.col("over_number"),    # 0-indexed: over 0 = first over
    F.col("ball_index"),     # position within over's delivery array (not over-number dot notation)

    # ── Player names + stable registry IDs ───────────────────────────────────
    # IMPORTANT: always use batter_id / bowler_id for joins, never name strings
    F.col("delivery.batter").alias("batter"),
    F.col("registry_map").getItem(F.col("delivery.batter")).alias("batter_id"),
    F.col("delivery.bowler").alias("bowler"),
    F.col("registry_map").getItem(F.col("delivery.bowler")).alias("bowler_id"),
    F.col("delivery.non_striker").alias("non_striker"),

    # ── Runs ─────────────────────────────────────────────────────────────────
    F.col("delivery.runs.batter").alias("runs_batter"),   # credited to batter: 0/1/2/3/4/5/6
    F.col("delivery.runs.extras").alias("runs_extras"),   # total extras this delivery
    F.col("delivery.runs.total").alias("runs_total"),     # what goes on the scoreboard
    # non_boundary: True = umpire originally signalled 4/6 but fielder stopped it
    F.coalesce(F.col("delivery.runs.non_boundary"), F.lit(False)).alias("non_boundary"),

    # ── Extras breakdown ──────────────────────────────────────────────────────
    # Each extra type is an int (the runs value) when present, null when absent
    F.col("delivery.extras.wides").alias("extras_wides"),
    F.col("delivery.extras.noballs").alias("extras_noballs"),
    F.col("delivery.extras.byes").alias("extras_byes"),       # NOT bowler's fault
    F.col("delivery.extras.legbyes").alias("extras_legbyes"), # NOT bowler's fault

    # ── Wicket ───────────────────────────────────────────────────────────────
    # Most deliveries have no wickets array; size(null) = 0 in Spark
    (F.coalesce(F.size(F.col("delivery.wickets")), F.lit(0)) > 0).alias("is_wicket"),
    F.col("delivery.wickets")[0]["kind"].alias("wicket_kind"),       # null if no wicket
    F.col("delivery.wickets")[0]["player_out"].alias("player_out"),  # null if no wicket

    # ── Quality / edge-case flags ─────────────────────────────────────────────
    F.col("info.missing").isNotNull().alias("data_missing_flag"),    # Cricsheet withheld ball-by-ball
)

print(f"Deliveries extracted: {bronze_df.count():,}")
bronze_df.printSchema()

# COMMAND ----------
# MAGIC %md ## 7. Write to Delta (partitioned by gender + season for fast filtering)

# COMMAND ----------

(
    bronze_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("gender", "season")
    .saveAsTable(BRONZE_TABLE)
)

print(f"✅ Bronze table written: {BRONZE_TABLE}")

# COMMAND ----------
# MAGIC %md ## 8. Data Quality Checks

# COMMAND ----------

from datetime import datetime

results = []
ts      = datetime.now().isoformat()
tbl     = spark.table(BRONZE_TABLE)
total   = tbl.count()

def log(check, status, detail=""):
    icon = "✅" if status == "PASS" else ("⚠️" if status == "WARN" else "❌")
    print(f"{icon} [{status}] {check}: {detail}")
    results.append({
        "check_name"  : check,
        "status"      : status,
        "detail"      : detail,
        "total_rows"  : total,
        "checked_at"  : ts,
        "table"       : BRONZE_TABLE
    })

# ── Check 1: Row count must exceed 1M ────────────────────────────────────────
log("row_count_gt_1M",
    "PASS" if total > 1_000_000 else "FAIL",
    f"{total:,} rows")

# ── Check 2: Both source datasets present ────────────────────────────────────
sources = {r[0] for r in tbl.select("source_dataset").distinct().collect()}
both_ok = "t20s_main" in sources and "icc_worldcup" in sources
log("both_sources_present",
    "PASS" if both_ok else "FAIL",
    f"Found: {sorted(sources)}")

# ── Check 3: Null checks on mandatory columns ─────────────────────────────────
for col_name in ["match_id", "innings_number", "over_number", "ball_index", "batter", "bowler"]:
    n = tbl.filter(F.col(col_name).isNull()).count()
    log(f"null_{col_name}",
        "PASS" if n == 0 else "WARN",
        f"{n:,} nulls")

# ── Check 4: Over number range (skip super over innings — they only have over 0) ──
bad_overs = tbl.filter(
    (~F.col("is_super_over")) &
    (~F.col("over_number").between(0, 19))
).count()
log("over_range_0_to_19",
    "PASS" if bad_overs == 0 else "WARN",
    f"{bad_overs:,} deliveries with over_number outside 0-19")

# ── Check 5: Runs total range (0-7: max = 6 + 1 noball, 5 = penalty) ─────────
bad_runs = tbl.filter(~F.col("runs_total").between(0, 7)).count()
log("runs_total_range_0_to_7",
    "PASS" if bad_runs == 0 else "WARN",
    f"{bad_runs:,} deliveries with runs_total outside 0-7")

# ── Check 6: Duplicate delivery key ──────────────────────────────────────────
dup_count = (
    tbl
    .groupBy("match_id", "innings_number", "over_number", "ball_index", "batter")
    .count()
    .filter(F.col("count") > 1)
    .count()
)
log("no_duplicate_deliveries",
    "PASS" if dup_count == 0 else "WARN",
    f"{dup_count:,} duplicate delivery keys (match+innings+over+ball+batter)")

# ── Write quality log ─────────────────────────────────────────────────────────
spark.createDataFrame(results).write.format("delta").mode("append").saveAsTable(QUALITY_TABLE)
print(f"\n📋 Quality log written to: {QUALITY_TABLE}")

# COMMAND ----------
# MAGIC %md ## 9. Preview

# COMMAND ----------

display(spark.table(BRONZE_TABLE).limit(10))

# COMMAND ----------

# Summary stats
print("=== Bronze Layer Summary ===")
display(
    spark.table(BRONZE_TABLE)
    .groupBy("source_dataset", "gender")
    .agg(
        F.count("*").alias("total_deliveries"),
        F.countDistinct("match_id").alias("matches"),
        F.countDistinct("batter").alias("unique_batters"),
        F.sum(F.col("is_wicket").cast("int")).alias("total_wickets"),
    )
    .orderBy("source_dataset", "gender")
)
