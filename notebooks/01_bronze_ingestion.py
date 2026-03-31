# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Layer: Raw Ingestion
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Reads raw T20 match JSON files from Unity Catalog Volume
# MAGIC - Flattens the deeply nested structure into one row per delivery (the atomic fact)
# MAGIC - Extracts stable player IDs from the Cricsheet registry map
# MAGIC - Tags all edge cases: super overs, D/L matches, missing data flags
# MAGIC - Runs data quality checks and logs results to a Delta table
# MAGIC
# MAGIC **Output table:** `tabular.dataexpert.prateek_capstone_project_bronze_deliveries`
# MAGIC **Quality log:**  `tabular.dataexpert.prateek_capstone_project_bronze_quality_log`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Why binaryFile instead of spark.read.json?**
# MAGIC
# MAGIC The JSON files use player names as object keys in `info.registry.people`.
# MAGIC Spark infers each unique key as a struct field (column). Across thousands
# MAGIC of files, case variants like "N D'Souza" vs "N D'souza" collide under
# MAGIC Spark's case-insensitive inference → `COLUMN_ALREADY_EXISTS` error.
# MAGIC Reading as binary + parsing in Python sidesteps this entirely.

# COMMAND ----------

import json
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from datetime import datetime

# ── Configuration ────────────────────────────────────────────────────────────
CATALOG       = "tabular"
SCHEMA        = "dataexpert"
VOL_DATA      = "/Volumes/tabular/dataexpert/prateek_capstone_project"
BRONZE_TABLE  = f"{CATALOG}.{SCHEMA}.prateek_capstone_project_bronze_deliveries"
QUALITY_TABLE = f"{CATALOG}.{SCHEMA}.prateek_capstone_project_bronze_quality_log"

print(f"✅ Config ready | Table: {BRONZE_TABLE}")

# COMMAND ----------
# MAGIC %md ## Step 1 — Read JSON files as binary blobs

# COMMAND ----------

# binaryFile reads each file as one row: path, modificationTime, length, content (binary)
# We cast content → string to get raw JSON, and extract match_id from the filename.
df_raw = (
    spark.read
    .format("binaryFile")
    .option("pathGlobFilter", "*.json")
    .load(VOL_DATA)
    .withColumn("json_str", F.col("content").cast("string"))
    .withColumn("match_id", F.regexp_extract(F.col("path"), r"([^/]+)\.json$", 1))
    .select("match_id", "json_str")
)

print(f"✅ Loaded {df_raw.count():,} JSON files")

# COMMAND ----------
# MAGIC %md ## Step 2 — Parse function: one match JSON → list of delivery dicts

# COMMAND ----------

def parse_match_json(json_str):
    """Parse a single Cricsheet match JSON string into a flat list of delivery dicts.

    Each Cricsheet JSON has the structure:
      { "info": { ...match metadata... },
        "innings": [ { "team": "...", "overs": [ { "over": 0, "deliveries": [...] } ] } ] }

    This function denormalizes match-level metadata onto every delivery row.
    Returns an empty list on parse failure (malformed files are silently skipped).
    """
    try:
        match       = json.loads(json_str)
        info        = match.get("info", {})
        innings_list = match.get("innings", [])

        # Player name → Cricsheet UUID (e.g. "V Kohli" → "abc-123")
        registry = info.get("registry", {}).get("people", {})

        # ── Match-level metadata (same for every delivery in this match) ─────
        match_date    = info.get("dates", [None])[0]
        season        = info.get("season", "")
        gender        = info.get("gender", "")          # "male" or "female"
        match_type    = info.get("match_type", "")      # "T20"
        venue         = info.get("venue", "Unknown")
        city          = info.get("city", "Unknown")
        event_name    = info.get("event", {}).get("name", "")
        teams         = info.get("teams", [])
        team_home     = teams[0] if len(teams) > 0 else None
        team_away     = teams[1] if len(teams) > 1 else None

        toss          = info.get("toss", {})
        toss_winner   = toss.get("winner")
        toss_decision = toss.get("decision")            # "bat" or "field"

        outcome        = info.get("outcome", {})
        match_winner   = outcome.get("winner")          # null for ties / no result
        outcome_result = outcome.get("result")          # "tie" | "no result" | null
        is_dl          = outcome.get("method") == "D/L"
        data_missing   = info.get("missing") is not None  # Cricsheet withheld ball-by-ball

        # ── Flatten: innings → overs → deliveries ────────────────────────────
        deliveries = []
        for inn_idx, innings in enumerate(innings_list):
            team_batting  = innings.get("team")
            is_super_over = innings.get("super_over", False)

            for over_num, over in enumerate(innings.get("overs", [])):
                for ball_idx, delivery in enumerate(over.get("deliveries", [])):
                    batter  = delivery.get("batter")
                    bowler  = delivery.get("bowler")
                    runs    = delivery.get("runs", {})
                    extras  = delivery.get("extras", {})
                    wickets = delivery.get("wickets", [])

                    deliveries.append({
                        # match_id backfilled in parse_batch() from the Spark row
                        "match_id":          None,
                        "match_date":        match_date,
                        "season":            season,
                        "gender":            gender,
                        "match_type":        match_type,
                        "venue":             venue,
                        "city":              city,
                        "event_name":        event_name,
                        "team_home":         team_home,
                        "team_away":         team_away,
                        "toss_winner":       toss_winner,
                        "toss_decision":     toss_decision,
                        "match_winner":      match_winner,
                        "outcome_result":    outcome_result,
                        "is_dl_match":       is_dl,
                        "data_missing_flag": data_missing,
                        "innings_number":    inn_idx,       # 0 = first innings
                        "team_batting":      team_batting,
                        "is_super_over":     is_super_over,
                        "over_number":       over_num,      # 0-indexed
                        "ball_index":        ball_idx,      # position within over
                        "batter":            batter,
                        "batter_id":         registry.get(batter) if batter else None,
                        "bowler":            bowler,
                        "bowler_id":         registry.get(bowler) if bowler else None,
                        "non_striker":       delivery.get("non_striker"),
                        "runs_batter":       runs.get("batter", 0),
                        "runs_extras":       runs.get("extras", 0),
                        "runs_total":        runs.get("total", 0),
                        "extras_wides":      extras.get("wides"),     # null if not a wide
                        "extras_noballs":    extras.get("noballs"),   # null if not a no-ball
                        "extras_byes":       extras.get("byes"),      # null if no byes
                        "extras_legbyes":    extras.get("legbyes"),   # null if no leg byes
                        "is_wicket":         len(wickets) > 0,
                        "wicket_kind":       wickets[0].get("kind") if wickets else None,
                        "player_out":        wickets[0].get("player_out") if wickets else None,
                    })
        return deliveries
    except Exception:
        return []  # malformed files silently skipped; show up in DQ null checks

# COMMAND ----------
# MAGIC %md ## Step 3 — Smoke test: parse one file to verify the function

# COMMAND ----------

sample            = df_raw.limit(1).collect()[0]
sample_deliveries = parse_match_json(sample["json_str"])
print(f"✅ Sample match → {len(sample_deliveries)} deliveries")
print(f"   First delivery keys: {list(sample_deliveries[0].keys()) if sample_deliveries else 'none'}")

# COMMAND ----------
# MAGIC %md ## Step 4 — Output schema (must match parse_match_json dict keys exactly)

# COMMAND ----------

delivery_schema = StructType([
    StructField("match_id",          StringType(),  True),
    StructField("match_date",        StringType(),  True),
    StructField("season",            StringType(),  True),
    StructField("gender",            StringType(),  True),
    StructField("match_type",        StringType(),  True),
    StructField("venue",             StringType(),  True),
    StructField("city",              StringType(),  True),
    StructField("event_name",        StringType(),  True),
    StructField("team_home",         StringType(),  True),
    StructField("team_away",         StringType(),  True),
    StructField("toss_winner",       StringType(),  True),
    StructField("toss_decision",     StringType(),  True),
    StructField("match_winner",      StringType(),  True),
    StructField("outcome_result",    StringType(),  True),
    StructField("is_dl_match",       BooleanType(), True),
    StructField("data_missing_flag", BooleanType(), True),
    StructField("innings_number",    IntegerType(), True),
    StructField("team_batting",      StringType(),  True),
    StructField("is_super_over",     BooleanType(), True),
    StructField("over_number",       IntegerType(), True),
    StructField("ball_index",        IntegerType(), True),
    StructField("batter",            StringType(),  True),
    StructField("batter_id",         StringType(),  True),
    StructField("bowler",            StringType(),  True),
    StructField("bowler_id",         StringType(),  True),
    StructField("non_striker",       StringType(),  True),
    StructField("runs_batter",       IntegerType(), True),
    StructField("runs_extras",       IntegerType(), True),
    StructField("runs_total",        IntegerType(), True),
    StructField("extras_wides",      IntegerType(), True),
    StructField("extras_noballs",    IntegerType(), True),
    StructField("extras_byes",       IntegerType(), True),
    StructField("extras_legbyes",    IntegerType(), True),
    StructField("is_wicket",         BooleanType(), True),
    StructField("wicket_kind",       StringType(),  True),
    StructField("player_out",        StringType(),  True),
])

# COMMAND ----------
# MAGIC %md ## Step 5 — Distributed parsing via mapInPandas

# COMMAND ----------

def parse_batch(iterator):
    """Parse each Spark partition's match JSONs into delivery rows.

    mapInPandas receives an iterator of Pandas DataFrames (one per partition)
    and must yield Pandas DataFrames conforming to delivery_schema.

    Type coercion is applied before yielding to prevent PyArrow serialization
    errors caused by mixed types (e.g. season = 2025 int vs "2025/26" string).
    """
    str_cols  = ["match_id","match_date","season","gender","match_type","venue","city",
                 "event_name","team_home","team_away","toss_winner","toss_decision",
                 "match_winner","outcome_result","team_batting","batter","batter_id",
                 "bowler","bowler_id","non_striker","wicket_kind","player_out"]
    int_cols  = ["innings_number","over_number","ball_index","runs_batter","runs_extras",
                 "runs_total","extras_wides","extras_noballs","extras_byes","extras_legbyes"]
    bool_cols = ["is_dl_match","data_missing_flag","is_super_over","is_wicket"]

    for batch_df in iterator:
        rows = []
        for _, row in batch_df.iterrows():
            deliveries = parse_match_json(row["json_str"])
            for d in deliveries:
                d["match_id"] = row["match_id"]  # backfill match_id
            rows.extend(deliveries)

        pdf = pd.DataFrame(rows)

        if pdf.empty:
            yield pdf
            continue

        for col in str_cols:
            if col in pdf.columns:
                pdf[col] = pdf[col].where(pdf[col].isna(), pdf[col].astype(str))
        for col in int_cols:
            if col in pdf.columns:
                pdf[col] = pd.to_numeric(pdf[col], errors="coerce").astype("Int64")
        for col in bool_cols:
            if col in pdf.columns:
                pdf[col] = pdf[col].astype("boolean")

        yield pdf

# COMMAND ----------
# MAGIC %md ## Step 6 — Execute distributed parse

# COMMAND ----------

bronze_df = df_raw.mapInPandas(parse_batch, schema=delivery_schema)

total_deliveries = bronze_df.count()
total_matches    = bronze_df.select("match_id").distinct().count()

print(f"✅ Total deliveries : {total_deliveries:,}")
print(f"✅ Distinct matches : {total_matches:,}")

# COMMAND ----------
# MAGIC %md ## Step 7 — Write to Delta bronze table

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
# MAGIC %md ## Step 8 — Data quality checks + append to log table

# COMMAND ----------

bronze = spark.table(BRONZE_TABLE)
run_ts = datetime.utcnow().isoformat()

checks = [
    ("total_rows",          bronze.count()),
    ("distinct_matches",    bronze.select("match_id").distinct().count()),
    ("null_batters",        bronze.filter(F.col("batter").isNull()).count()),
    ("null_bowlers",        bronze.filter(F.col("bowler").isNull()).count()),
    ("null_match_id",       bronze.filter(F.col("match_id").isNull()).count()),
    ("null_innings_number", bronze.filter(F.col("innings_number").isNull()).count()),
    ("bad_runs_over_36",    bronze.filter(F.col("runs_total") > 36).count()),
    ("bad_overs_over_19",   bronze.filter(
                                (F.col("over_number") > 19) &
                                (F.col("is_super_over") == False)
                            ).count()),
]

def get_status(name, value):
    if name in ("total_rows", "distinct_matches"):
        return "PASS" if value > 0 else "FAIL"
    return "PASS" if value == 0 else "WARN"

log_rows = [
    {
        "run_ts":     run_ts,
        "table_name": BRONZE_TABLE,
        "check_name": name,
        "value":      int(value),
        "status":     get_status(name, value),
    }
    for name, value in checks
]

# Print summary
print(f"\n📊 DQ Check Results — {run_ts}")
print(f"{'Check':<25} {'Value':>12}  Status")
print("─" * 48)
for row in log_rows:
    icon = "✅" if row["status"] == "PASS" else "⚠️ " if row["status"] == "WARN" else "❌"
    print(f"{icon} {row['check_name']:<23} {row['value']:>12,}  {row['status']}")

# Append to log table (creates on first run, appends on every subsequent run)
log_df = spark.createDataFrame(log_rows)

(
    log_df
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(QUALITY_TABLE)
)

total_log_entries = spark.table(QUALITY_TABLE).count()
print(f"\n✅ DQ log appended → {QUALITY_TABLE}")
print(f"   Total log entries so far: {total_log_entries:,}")
