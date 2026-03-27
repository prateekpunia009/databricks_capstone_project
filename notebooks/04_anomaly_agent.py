# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # 04 — Anomaly Detection Agent: AI-Powered Insights
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC - Reads flagged players from `gold_anomaly_flags`
# MAGIC - Calls Databricks AI Functions (`ai_generate_text`) to produce a
# MAGIC   natural-language analyst insight for each flagged player
# MAGIC - Saves the enriched results to `gold_anomaly_insights`
# MAGIC - Falls back to rule-based insights if AI Functions quota is exceeded
# MAGIC
# MAGIC **This is the "agentic action" component of the capstone:**
# MAGIC The pipeline autonomously detects anomalies using statistical thresholds
# MAGIC and then generates analyst-ready narrative — no human writes these summaries.

# COMMAND ----------

CATALOG       = "capstone"
SCHEMA        = "cricket"
ANOMALY_TABLE = f"{CATALOG}.{SCHEMA}.gold_anomaly_flags"
INSIGHTS_TABLE = f"{CATALOG}.{SCHEMA}.gold_anomaly_insights"

# COMMAND ----------
# MAGIC %md ## 1. Check flagged players

# COMMAND ----------

from pyspark.sql import functions as F

anomaly_df = spark.table(ANOMALY_TABLE)

print(f"Total flagged players: {anomaly_df.count()}")
display(
    anomaly_df
    .select("batter", "matches", "career_avg_sr", "rolling_avg_sr", "anomaly_score", "anomaly_direction")
    .orderBy(F.abs(F.col("anomaly_score")).desc())
)

# COMMAND ----------
# MAGIC %md ## 2. Generate AI insights using Databricks AI Functions
# MAGIC
# MAGIC `ai_generate_text()` is a Databricks SQL AI Function that calls a managed LLM endpoint.
# MAGIC It's available on Databricks workspaces with Unity Catalog and is billed per token.
# MAGIC
# MAGIC If you hit quota limits, the fallback cell below generates rule-based insights instead.

# COMMAND ----------

try:
    # Register the anomaly table as a temp view to use in spark.sql
    anomaly_df.createOrReplaceTempView("anomaly_flagged")

    insights_df = spark.sql("""
        SELECT
            batter_id,
            batter,
            matches,
            career_runs,
            ROUND(career_avg_sr, 1)          AS career_avg_sr,
            ROUND(rolling_avg_sr, 1)          AS rolling_avg_sr,
            ROUND(anomaly_score, 2)           AS anomaly_score,
            anomaly_direction,
            highest_score,
            hundreds,
            fifties,
            ai_generate_text(
                CONCAT(
                    'You are a senior cricket analyst writing a brief scout report. ',
                    'Player: ', batter, '. ',
                    'Career T20I matches: ', matches, '. ',
                    'Career average strike rate: ', ROUND(career_avg_sr, 1), '. ',
                    'Recent form (last 10 matches) average strike rate: ', ROUND(rolling_avg_sr, 1), '. ',
                    'Statistical anomaly score (z-score): ', ROUND(anomaly_score, 2), '. ',
                    CASE WHEN anomaly_score > 0
                         THEN 'The player is in exceptional form — significantly above their career baseline. '
                         ELSE 'The player is in a notable slump — significantly below their career baseline. '
                    END,
                    'Write exactly 2 sentences: ',
                    '(1) Describe the nature of this performance shift and what it signals. ',
                    '(2) What should a team selector or opposition analyst watch for in their next match?'
                )
            )                                AS ai_insight,
            current_timestamp()              AS generated_at
        FROM anomaly_flagged
        WHERE ABS(anomaly_score) > 2.0
        ORDER BY ABS(anomaly_score) DESC
    """)

    insights_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(INSIGHTS_TABLE)

    print(f"✅ AI insights generated and saved to: {INSIGHTS_TABLE}")
    print(f"   Players with insights: {insights_df.count()}")

except Exception as e:
    print(f"⚠️  AI Functions failed: {e}")
    print("→ Falling back to rule-based insights...")
    raise   # let the fallback cell handle it

# COMMAND ----------
# MAGIC %md ## 3. Fallback: Rule-based insights (if AI Functions quota exceeded)
# MAGIC
# MAGIC Run this cell manually if the AI Functions cell above fails.

# COMMAND ----------

# UNCOMMENT this cell and run it if ai_generate_text fails
# -------------------------------------------------------------------------

# anomaly_df = spark.table(ANOMALY_TABLE)
#
# from pyspark.sql import functions as F
#
# fallback_df = anomaly_df.withColumn(
#     "ai_insight",
#     F.when(
#         F.col("anomaly_score") > 2,
#         F.concat(
#             F.col("batter"),
#             F.lit(" is in exceptional batting form with a recent strike rate of "),
#             F.round(F.col("rolling_avg_sr"), 1).cast("string"),
#             F.lit(" vs a career average of "),
#             F.round(F.col("career_avg_sr"), 1).cast("string"),
#             F.lit(". Opposition analysts should prepare for an aggressive batting approach"
#                   " — this player is likely to target early boundaries.")
#         )
#     ).when(
#         F.col("anomaly_score") < -2,
#         F.concat(
#             F.col("batter"),
#             F.lit(" is in a significant batting slump with a recent strike rate of "),
#             F.round(F.col("rolling_avg_sr"), 1).cast("string"),
#             F.lit(" vs a career average of "),
#             F.round(F.col("career_avg_sr"), 1).cast("string"),
#             F.lit(". Team selectors should monitor closely — consider whether form "
#                   "warrants a place in the playing XI for the next fixture.")
#         )
#     )
# ).withColumn("generated_at", F.current_timestamp())
#
# fallback_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .option("overwriteSchema", "true") \
#     .saveAsTable(INSIGHTS_TABLE)
#
# print(f"✅ Rule-based insights saved to: {INSIGHTS_TABLE}")

# COMMAND ----------
# MAGIC %md ## 4. Review generated insights

# COMMAND ----------

display(
    spark.table(INSIGHTS_TABLE)
    .select(
        "batter",
        "matches",
        "career_avg_sr",
        "rolling_avg_sr",
        "anomaly_score",
        "anomaly_direction",
        "ai_insight",
        "generated_at"
    )
    .orderBy(F.abs(F.col("anomaly_score")).desc())
)

# COMMAND ----------
# MAGIC %md ## 5. Summary stats of the anomaly feed

# COMMAND ----------

hot_count   = spark.table(INSIGHTS_TABLE).filter(F.col("anomaly_direction") == "HOT_FORM").count()
slump_count = spark.table(INSIGHTS_TABLE).filter(F.col("anomaly_direction") == "SLUMP").count()

print(f"🟢 Players in HOT FORM (z > +2): {hot_count}")
print(f"🔴 Players in SLUMP   (z < -2): {slump_count}")
print(f"📊 Total insights generated:     {hot_count + slump_count}")
