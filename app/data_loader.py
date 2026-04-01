"""
data_loader.py — Static CSV fallback for Databricks gold tables.

When DATABRICKS_TOKEN is not available, the app reads from local CSV files
in the app/data/ directory instead of querying the SQL warehouse live.

Usage:
    from data_loader import run_q   # drop-in replacement for the Databricks run_q()
"""

import os
import pandas as pd
from pathlib import Path

# CSV files live in app/data/
DATA_DIR = Path(__file__).parent / "data"

# Map table suffix → CSV filename
TABLE_MAP = {
    "gold_batter_career":   "gold_batter_career.csv",
    "gold_batter_by_match": "gold_batter_by_match.csv",
    "gold_batter_by_phase": "gold_batter_by_phase.csv",
    "gold_bowler_career":   "gold_bowler_career.csv",
    "gold_bowler_by_match": "gold_bowler_by_match.csv",
    "gold_bowler_by_phase": "gold_bowler_by_phase.csv",
    "gold_matchup":         "gold_matchup.csv",
    "anomaly_narratives":   "anomaly_narratives.csv",
}

# Loaded once at startup
_cache: dict[str, pd.DataFrame] = {}


def _load(table_suffix: str) -> pd.DataFrame:
    if table_suffix not in _cache:
        path = DATA_DIR / TABLE_MAP[table_suffix]
        if not path.exists():
            raise FileNotFoundError(f"CSV not found: {path}. Run notebook 07_export_gold_to_csv.py first.")
        _cache[table_suffix] = pd.read_csv(path)
    return _cache[table_suffix]


def _resolve_table(full_table_name: str) -> str:
    """Extract the suffix from a full Unity Catalog table name."""
    # e.g. tabular.dataexpert.prateek_capstone_project_gold_batter_career
    # → gold_batter_career
    name = full_table_name.split(".")[-1]  # last segment after schema
    prefix = "prateek_capstone_project_"
    if name.startswith(prefix):
        return name[len(prefix):]
    return name


def run_q_static(sql: str) -> pd.DataFrame:
    """
    Very lightweight SQL interpreter — handles the SELECT patterns used in app.py.
    Supports: WHERE LOWER(col) = LOWER('val'), ORDER BY, LIMIT, basic aggregates.
    For complex queries (GROUP BY aggregates) returns empty df gracefully.
    """
    import re, sqlite3

    # Extract table name from SQL
    match = re.search(r"FROM\s+([\w.]+)", sql, re.IGNORECASE)
    if not match:
        return pd.DataFrame()

    full_table = match.group(1)
    suffix = _resolve_table(full_table)

    try:
        df = _load(suffix)
    except FileNotFoundError:
        return pd.DataFrame()

    # Use in-memory SQLite to run the actual query against the loaded DataFrame
    conn = sqlite3.connect(":memory:")
    df.to_sql(suffix, conn, index=False, if_exists="replace")

    # Replace the full Unity Catalog table name with the SQLite table name
    sqlite_sql = re.sub(re.escape(full_table), suffix, sql, flags=re.IGNORECASE)

    # SQLite doesn't have ai_query() — return empty for AI live queries
    if "ai_query(" in sqlite_sql.lower():
        return pd.DataFrame()

    try:
        result = pd.read_sql_query(sqlite_sql, conn)
    except Exception:
        result = pd.DataFrame()
    finally:
        conn.close()

    return result
