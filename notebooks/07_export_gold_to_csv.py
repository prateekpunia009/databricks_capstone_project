# Databricks notebook source
# MAGIC %md
# MAGIC ## Export Gold Tables to CSV
# MAGIC Run this once in Databricks to export all gold tables for static hosting.

# COMMAND ----------

import pandas as pd

BASE = "tabular.dataexpert.prateek_capstone_project"

tables = {
    "gold_batter_career":    f"{BASE}_gold_batter_career",
    "gold_batter_by_match":  f"{BASE}_gold_batter_by_match",
    "gold_batter_by_phase":  f"{BASE}_gold_batter_by_phase",
    "gold_bowler_career":    f"{BASE}_gold_bowler_career",
    "gold_bowler_by_match":  f"{BASE}_gold_bowler_by_match",
    "gold_bowler_by_phase":  f"{BASE}_gold_bowler_by_phase",
    "gold_matchup":          f"{BASE}_gold_matchup",
    "anomaly_narratives":    f"{BASE}_anomaly_narratives",
}

# Export to /tmp then download
for name, full_table in tables.items():
    df = spark.table(full_table).toPandas()
    path = f"/tmp/{name}.csv"
    df.to_csv(path, index=False)
    print(f"✅  {name}: {len(df)} rows → {path}")

print("\nAll tables exported. Download from File → Open in Databricks, or use dbutils:")
print('dbutils.fs.cp("file:/tmp/gold_batter_career.csv", "dbfs:/FileStore/exports/gold_batter_career.csv")')

# COMMAND ----------

# Copy all to DBFS so you can download via browser
import os

for name in tables.keys():
    local = f"file:/tmp/{name}.csv"
    dbfs  = f"dbfs:/FileStore/capstone_exports/{name}.csv"
    dbutils.fs.cp(local, dbfs)
    print(f"Copied → {dbfs}")

print("\nDownload links (replace <your-workspace-host>):")
for name in tables.keys():
    print(f"  https://<your-workspace-host>/files/capstone_exports/{name}.csv")
