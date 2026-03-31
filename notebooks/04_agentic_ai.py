# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Agentic AI: Cricket Performance Intelligence Agent
# MAGIC
# MAGIC **What this agent does:**
# MAGIC The agent autonomously runs a 4-step loop for every flagged anomaly:
# MAGIC
# MAGIC 1. **OBSERVE**  — reads statistical anomalies from `gold_anomaly_feed`
# MAGIC 2. **REASON**   — calls LLM (Llama 3.3 70B) to generate a narrative for each anomaly
# MAGIC 3. **DECIDE**   — based on Z-score and severity, decides what action to take
# MAGIC 4. **ACT**      — routes to HIGH → alerts table, MEDIUM → watchlist, LOW → log only
# MAGIC 5. **LOG**      — records every decision in `gold_agent_actions` (full audit trail)
# MAGIC
# MAGIC **Why this is agentic (not just an LLM call):**
# MAGIC - The agent makes **different decisions** based on what it observes
# MAGIC - It **acts differently** for each case — not a single fixed output
# MAGIC - Every action is **logged** with reasoning — full auditability
# MAGIC - It operates **without human intervention** end to end
# MAGIC
# MAGIC **Output tables:**
# MAGIC - `gold_anomaly_narratives` — AI narrative per anomaly
# MAGIC - `gold_agent_alerts`       — HIGH severity cases requiring immediate attention
# MAGIC - `gold_agent_watchlist`    — MEDIUM severity cases to monitor
# MAGIC - `gold_agent_actions`      — full audit log of every agent decision

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from datetime import datetime
import json

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG = "tabular"
SCHEMA  = "dataexpert"
PREFIX  = f"{CATALOG}.{SCHEMA}.prateek_capstone_project"

ANOMALY_TABLE    = f"{PREFIX}_gold_anomaly_feed"
NARRATIVE_TABLE  = f"{PREFIX}_anomaly_narratives"
ALERTS_TABLE     = f"{PREFIX}_agent_alerts"
WATCHLIST_TABLE  = f"{PREFIX}_agent_watchlist"
ACTIONS_TABLE    = f"{PREFIX}_agent_actions"
MODEL            = "databricks-meta-llama-3-3-70b-instruct"
RECENT_WINDOW    = 10
ZSCORE_HIGH      = 2.5   # Z-score threshold for HIGH severity action
ZSCORE_MEDIUM    = 1.5   # Z-score threshold for MEDIUM severity action

print(f"✅ Agent config ready")
print(f"   Observing  : {ANOMALY_TABLE}")
print(f"   Alerts     : {ALERTS_TABLE}")
print(f"   Watchlist  : {WATCHLIST_TABLE}")
print(f"   Actions log: {ACTIONS_TABLE}")
print(f"   Model      : {MODEL}")

# COMMAND ----------
# MAGIC %md ## Step 1 — OBSERVE: Load flagged anomalies

# COMMAND ----------

anomalies = spark.table(ANOMALY_TABLE).filter(F.col("anomaly_flag") == True)
total_flagged = anomalies.count()

print(f"🔍 Agent observing {total_flagged:,} flagged anomalies")
print(f"   Batters : {anomalies.filter(F.col('entity_type') == 'batter').count():,}")
print(f"   Bowlers : {anomalies.filter(F.col('entity_type') == 'bowler').count():,}")
print(f"   HIGH    : {anomalies.filter(F.col('severity') == 'HIGH').count():,}")
print(f"   MEDIUM  : {anomalies.filter(F.col('severity') == 'MEDIUM').count():,}")
print(f"   LOW     : {anomalies.filter(F.col('severity') == 'LOW').count():,}")

anomalies.createOrReplaceTempView("flagged_anomalies")
print("\n✅ Anomalies loaded into temp view: flagged_anomalies")

# COMMAND ----------
# MAGIC %md ## Step 2 — REASON: Generate AI narratives via ai_query()
# MAGIC
# MAGIC The LLM reasons about each anomaly in cricket context and produces
# MAGIC a human-readable insight a coach or analyst can act on directly.

# COMMAND ----------

narratives_df = spark.sql(f"""
    SELECT
        player,
        player_id,
        entity_type,
        anomaly_type,
        severity,
        career_metric,
        recent_metric,
        sr_delta,
        sr_zscore,
        career_dot_pct,
        recent_dot_pct,
        dot_delta,
        recent_matches,
        career_matches,
        checked_at,
        ai_query(
            '{MODEL}',
            CONCAT(
                'You are a cricket performance analyst and team selector. Analyse this player anomaly and respond in exactly this format with no extra text:\n',
                'INSIGHT: [2-3 sentences explaining what is happening and why it matters. Use cricket terminology.]\n',
                'RECOMMENDED ACTION: [One specific, actionable recommendation for the team management. ',
                'For batters: batting order, role, phase to use/avoid. ',
                'For bowlers: spell length, phase to bowl, matchup to target. ',
                'Be direct and prescriptive.]\n\n',
                'Player: ', player,
                '\\nRole: ', entity_type,
                '\\nAnomaly type: ', anomaly_type,
                '\\nSeverity: ', severity,
                '\\nCareer average metric: ', ROUND(career_metric, 1),
                '\\nRecent form metric (last 10 matches): ', ROUND(recent_metric, 1),
                '\\nChange (delta): ', ROUND(sr_delta, 1),
                '\\nZ-score (standard deviations from career norm): ', ROUND(sr_zscore, 2),
                '\\nCareer dot ball %: ', ROUND(career_dot_pct, 1),
                '\\nRecent dot ball %: ', ROUND(recent_dot_pct, 1),
                '\\nMatches analysed (recent window): ', recent_matches,
                '\\nCareer matches: ', career_matches
            )
        ) AS ai_response
    FROM flagged_anomalies
    ORDER BY ABS(sr_zscore) DESC
""")

# Split ai_response into narrative + recommended_action
narratives_final = (
    narratives_df
    # FIX Bug5: (?s) = DOTALL mode so . matches newlines; stops before RECOMMENDED ACTION:
    .withColumn("ai_narrative",
        F.regexp_extract(F.col("ai_response"), r"(?s)INSIGHT:\s*(.*?)(?=\s*RECOMMENDED ACTION:)", 1))
    # Greedy .* captures full multiline recommended action text to end of string
    .withColumn("recommended_action",
        F.regexp_extract(F.col("ai_response"), r"(?s)RECOMMENDED ACTION:\s*(.*)\s*$", 1))
    .drop("ai_response")
    .withColumn("generated_at", F.lit(datetime.utcnow().isoformat()))
    .withColumn("model_used",   F.lit(MODEL))
)

# Write narratives table
(
    narratives_final
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(NARRATIVE_TABLE)
)
print(f"✅ AI narratives written: {spark.table(NARRATIVE_TABLE).count():,} rows → {NARRATIVE_TABLE}")

# COMMAND ----------
# MAGIC %md ## Step 3 — DECIDE + ACT: Agent routes each anomaly based on severity
# MAGIC
# MAGIC **Decision rules:**
# MAGIC - |Z-score| >= 2.5 OR severity == HIGH   → ALERT   (immediate attention required)
# MAGIC - |Z-score| >= 1.5 OR severity == MEDIUM → WATCHLIST (monitor closely)
# MAGIC - Everything else                        → LOG      (record only)
# MAGIC
# MAGIC This is the agentic loop — the agent observes each case,
# MAGIC makes a decision, acts on it, and logs what it did.

# COMMAND ----------

run_ts = datetime.utcnow().isoformat()
narratives = spark.table(NARRATIVE_TABLE).collect()

alerts    = []
watchlist = []
actions   = []

print(f"🤖 Agent processing {len(narratives)} narratives...\n")

for row in narratives:
    zscore   = abs(row["sr_zscore"]) if row["sr_zscore"] is not None else 0
    severity = row["severity"]
    delta    = row["sr_delta"] if row["sr_delta"] is not None else 0

    # ── DECIDE ────────────────────────────────────────────────────────────────
    if zscore >= ZSCORE_HIGH or severity == "HIGH":
        action       = "ALERT"
        action_reason = f"Z-score {row['sr_zscore']:.2f} exceeds HIGH threshold ({ZSCORE_HIGH}). Immediate attention required."
    elif zscore >= ZSCORE_MEDIUM or severity == "MEDIUM":
        action        = "WATCHLIST"
        action_reason = f"Z-score {row['sr_zscore']:.2f} exceeds MEDIUM threshold ({ZSCORE_MEDIUM}). Monitor closely over next 3 matches."
    else:
        action        = "LOG"
        action_reason = f"Z-score {row['sr_zscore']:.2f} below thresholds. Logging for historical record."

    # ── ACT ───────────────────────────────────────────────────────────────────
    base = {
        "player":               row["player"],
        "player_id":            row["player_id"],
        "entity_type":          row["entity_type"],
        "anomaly_type":         row["anomaly_type"],
        "severity":             severity,
        "sr_zscore":            row["sr_zscore"],
        "sr_delta":             row["sr_delta"],
        "career_metric":        row["career_metric"],
        "recent_metric":        row["recent_metric"],
        "ai_narrative":         row["ai_narrative"],
        "recommended_action":   row["recommended_action"],
        "actioned_at":          run_ts,
    }

    if action == "ALERT":
        alerts.append(base)
    elif action == "WATCHLIST":
        watchlist.append(base)

    # ── LOG every decision (full audit trail) ─────────────────────────────────
    actions.append({
        "run_ts":             run_ts,
        "player":             row["player"],
        "player_id":          row["player_id"],
        "entity_type":        row["entity_type"],
        "anomaly_type":       row["anomaly_type"],
        "severity":           severity,
        "sr_zscore":          row["sr_zscore"],
        "action_taken":       action,
        "action_reason":      action_reason,
        "ai_narrative":       row["ai_narrative"],
        "recommended_action": row["recommended_action"],
    })

    icon = "🚨" if action == "ALERT" else "👁️ " if action == "WATCHLIST" else "📝"
    print(f"{icon} [{action:<10}] {row['player']:<25} Z={row['sr_zscore']:>6.2f}  {row['anomaly_type']}")

print(f"\n✅ Agent decisions complete")
print(f"   🚨 ALERTS    : {len(alerts)}")
print(f"   👁️  WATCHLIST : {len(watchlist)}")
print(f"   📝 LOG ONLY  : {len(actions) - len(alerts) - len(watchlist)}")

# COMMAND ----------
# MAGIC %md ## Step 4 — Write agent outputs to Delta tables

# COMMAND ----------

# Write ALERTS table
if alerts:
    spark.createDataFrame(alerts).write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(ALERTS_TABLE)
    print(f"🚨 Alerts written    : {len(alerts)} rows → {ALERTS_TABLE}")
else:
    print(f"✅ No HIGH severity alerts at this time")

# Write WATCHLIST table
if watchlist:
    spark.createDataFrame(watchlist).write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(WATCHLIST_TABLE)
    print(f"👁️  Watchlist written : {len(watchlist)} rows → {WATCHLIST_TABLE}")
else:
    print(f"✅ No MEDIUM severity watchlist items at this time")

# Write ACTIONS log (always append — full history of every agent run)
if actions:
    (
        spark.createDataFrame(actions)
        .write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(ACTIONS_TABLE)
    )
    total_actions = spark.table(ACTIONS_TABLE).count()
    print(f"📝 Actions logged    : {len(actions)} new rows → {ACTIONS_TABLE}")
    print(f"   Total agent actions across all runs: {total_actions:,}")

# COMMAND ----------
# MAGIC %md ## Step 5 — Agent run summary

# COMMAND ----------

print("=" * 65)
print(f"  AGENT RUN COMPLETE — {run_ts}")
print("=" * 65)
print(f"  Anomalies observed : {total_flagged:,}")
print(f"  Narratives generated: {spark.table(NARRATIVE_TABLE).count():,}")
print(f"  🚨 ALERTS triggered : {len(alerts)}")
print(f"  👁️  WATCHLIST added  : {len(watchlist)}")
print(f"  📝 LOG only         : {len(actions) - len(alerts) - len(watchlist)}")
print("=" * 65)

# COMMAND ----------
# MAGIC %md ## Step 6 — Preview alerts and narratives

# COMMAND ----------

if alerts:
    print("🚨 HIGH SEVERITY ALERTS — Immediate Action Required\n")
    for row in alerts:
        delta_str = f"+{row['sr_delta']}" if row['sr_delta'] and row['sr_delta'] > 0 else str(row['sr_delta'])
        print(f"{'─' * 65}")
        print(f"  {row['player'].upper()}  |  {row['entity_type'].upper()}  |  {row['anomaly_type']}")
        print(f"  Career: {round(row['career_metric'], 1)}  →  Recent: {round(row['recent_metric'], 1)}  ({delta_str})  Z: {row['sr_zscore']}")
        print(f"\n  📊 INSIGHT: {row['ai_narrative']}")
        print(f"\n  ✅ RECOMMENDED ACTION: {row['recommended_action']}\n")

# COMMAND ----------

print("\n📋 Full Agent Actions Log (latest run)\n")
display(
    spark.table(ACTIONS_TABLE)
    .filter(F.col("run_ts") == run_ts)
    .select("player", "entity_type", "anomaly_type", "severity",
            "sr_zscore", "action_taken", "action_reason", "recommended_action")
    .orderBy(F.desc(F.abs(F.col("sr_zscore"))))
)

# COMMAND ----------

print("\n📊 Agent Decision Distribution\n")
display(
    spark.table(ACTIONS_TABLE)
    .filter(F.col("run_ts") == run_ts)
    .groupBy("action_taken", "entity_type")
    .count()
    .orderBy("action_taken", "entity_type")
)
