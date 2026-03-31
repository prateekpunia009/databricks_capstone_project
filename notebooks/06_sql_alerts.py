# Databricks notebook source
# MAGIC %md
# MAGIC # Cricket Intelligence Platform — SQL Alerts Setup
# MAGIC
# MAGIC Creates two native Databricks SQL Alerts:
# MAGIC
# MAGIC | Alert | Trigger | Channel |
# MAGIC |-------|---------|---------|
# MAGIC | **HIGH Anomaly Alert** | New HIGH-severity agent alert in last 24 h | Email |
# MAGIC | **Watchlist Summary** | MEDIUM anomalies > 5 in last 24 h | Email |
# MAGIC
# MAGIC These alerts run on a schedule and email you automatically — no external services needed.

# COMMAND ----------

# MAGIC %md ## Configuration — update YOUR_EMAIL before running

# COMMAND ----------

# ── Edit these ────────────────────────────────────────────────────────────────
ALERT_EMAIL = "YOUR_EMAIL@example.com"        # <-- change to your email
# ─────────────────────────────────────────────────────────────────────────────

PREFIX = "tabular.dataexpert.prateek_capstone_project"

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dbsql

w = WorkspaceClient()

# Get the best available SQL warehouse
warehouses = list(w.warehouses.list())
warehouse  = next((wh for wh in warehouses if wh.state and wh.state.name in ("RUNNING", "STOPPED")), warehouses[0])
WAREHOUSE_ID = warehouse.id
print(f"Using warehouse: {warehouse.name}  ({WAREHOUSE_ID})")

# COMMAND ----------

# MAGIC %md ## Alert 1 — HIGH Severity Anomaly Alert

# COMMAND ----------

# SQL query for the alert — returns row count of new HIGH alerts in last 24h
HIGH_ALERT_SQL = f"""
SELECT COUNT(*) AS high_alert_count
FROM {PREFIX}_agent_alerts
WHERE created_at >= date_sub(current_timestamp(), 1)
"""

# Create a named query first (alerts are based on named queries)
high_query = w.queries.create(
    query=dbsql.CreateQueryRequestQuery(
        display_name  = "Cricket — HIGH Anomaly Count (24h)",
        warehouse_id  = WAREHOUSE_ID,
        description   = "Counts HIGH severity cricket anomalies in the last 24 hours",
        query_text    = HIGH_ALERT_SQL,
    )
)
print(f"✅ Query created: {high_query.id}  —  {high_query.display_name}")

# COMMAND ----------

# Create the alert on top of the query
high_alert = w.alerts.create(
    alert=dbsql.CreateAlertRequestAlert(
        display_name = "🚨 Cricket — HIGH Severity Anomaly Detected",
        query_id     = high_query.id,
        condition    = dbsql.AlertCondition(
            op      = dbsql.AlertOperator.GREATER_THAN,
            operand = dbsql.AlertConditionOperand(
                value=dbsql.AlertOperandValue(double_value=0)   # trigger if count > 0
            ),
            empty_result_state = dbsql.AlertState.OK,
        ),
        notification_subscriptions = [
            dbsql.AlertNotificationSubscriptions(
                user_email=ALERT_EMAIL
            )
        ],
        seconds_to_retrigger = 86400,    # don't re-alert for 24h after triggering
    )
)
print(f"✅ HIGH alert created: {high_alert.id}")
print(f"   Name : {high_alert.display_name}")

# COMMAND ----------

# MAGIC %md ## Alert 2 — Watchlist Accumulation Alert

# COMMAND ----------

WATCHLIST_SQL = f"""
SELECT COUNT(*) AS watchlist_count
FROM {PREFIX}_agent_watchlist
WHERE created_at >= date_sub(current_timestamp(), 1)
"""

watchlist_query = w.queries.create(
    query=dbsql.CreateQueryRequestQuery(
        display_name  = "Cricket — MEDIUM Anomaly Count (24h)",
        warehouse_id  = WAREHOUSE_ID,
        description   = "Counts MEDIUM severity cricket anomalies in the last 24 hours",
        query_text    = WATCHLIST_SQL,
    )
)
print(f"✅ Query created: {watchlist_query.id}  —  {watchlist_query.display_name}")

# COMMAND ----------

watchlist_alert = w.alerts.create(
    alert=dbsql.AlertRequestAlert(
        display_name = "⚠️ Cricket — Watchlist Accumulating (>5 MEDIUM alerts)",
        query_id     = watchlist_query.id,
        condition    = dbsql.AlertCondition(
            op      = dbsql.AlertOperator.GREATER_THAN,
            operand = dbsql.AlertConditionOperand(
                value=dbsql.AlertOperandValue(double_value=5)   # trigger if > 5 MEDIUM alerts
            ),
            empty_result_state = dbsql.AlertState.OK,
        ),
        notification_subscriptions = [
            dbsql.AlertNotificationSubscriptions(
                user_email=ALERT_EMAIL
            )
        ],
        seconds_to_retrigger = 86400,
    )
)
print(f"✅ Watchlist alert created: {watchlist_alert.id}")
print(f"   Name : {watchlist_alert.display_name}")

# COMMAND ----------

# MAGIC %md ## Schedule Notebook 04 (Agentic AI) to run daily

# COMMAND ----------

# This schedules notebook 04_agentic_ai to run every day at 6 AM UTC
# so alerts always have fresh data to evaluate

from databricks.sdk.service import jobs as dbj

USER = w.current_user.me().user_name
NOTEBOOK_PATH = f"/Workspace/Users/{USER}/capstone_project/notebooks/04_agentic_ai"

job = w.jobs.create(
    name = "Cricket — Daily Agentic AI Run",
    tasks = [dbj.Task(
        task_key          = "run_agentic_ai",
        description       = "Run agentic loop, refresh anomaly alerts and watchlist",
        notebook_task     = dbj.NotebookTask(notebook_path=NOTEBOOK_PATH),
        existing_cluster_id = None,   # uses serverless
    )],
    schedule = dbj.CronSchedule(
        quartz_cron_expression = "0 0 6 * * ?",   # daily at 6:00 AM UTC
        timezone_id            = "UTC",
    ),
    email_notifications = dbj.JobEmailNotifications(
        on_failure = [ALERT_EMAIL],
    ),
)
print(f"✅ Scheduled job created: {job.job_id}")
print(f"   Runs daily at 06:00 UTC")
print(f"   On failure → email sent to: {ALERT_EMAIL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Done — Summary
# MAGIC
# MAGIC | Item | Status |
# MAGIC |------|--------|
# MAGIC | HIGH severity alert | ✅ Created — fires when any HIGH anomaly detected in last 24h |
# MAGIC | Watchlist alert | ✅ Created — fires when >5 MEDIUM anomalies in last 24h |
# MAGIC | Daily agentic job | ✅ Scheduled — runs notebook 04 every day at 06:00 UTC |
# MAGIC
# MAGIC **To view/manage alerts in Databricks UI:**
# MAGIC - Sidebar → **SQL** → **Alerts**
# MAGIC
# MAGIC **To view/manage jobs:**
# MAGIC - Sidebar → **Workflows** → **Jobs**
# MAGIC
# MAGIC **Email requirements:**
# MAGIC - Your Databricks workspace must have SMTP configured (most enterprise workspaces do)
# MAGIC - If you don't receive emails, ask your workspace admin to enable alert notifications
