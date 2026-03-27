"""
Cricket Player Performance Intelligence Platform
Databricks Bootcamp Capstone — Streamlit Dashboard

Tabs:
  1. Player Explorer  — search any player, view career stats + SR trend + phase breakdown
  2. Match Feed       — browse top performances by date/event, highlights World Cup matches
  3. Anomaly Feed     — AI-generated scout reports for statistically flagged players

Connection: Databricks SQL Warehouse via databricks-sql-connector
Run locally:  streamlit run streamlit_app.py
Deploy:       databricks apps create capstone-cricket (set env vars below)
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from databricks import sql

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Cricket Intelligence Platform",
    page_icon="🏏",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Connection config (set as environment variables on Databricks Apps) ───────
DATABRICKS_HOST  = os.getenv("DATABRICKS_HOST",  "your-workspace.azuredatabricks.net")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN",  "dapiXXXXXX")
HTTP_PATH        = os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/XXXX")

CATALOG = "capstone"
SCHEMA  = "cricket"

# ── DB connection (cached — opens once per session) ───────────────────────────
@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname = DATABRICKS_HOST,
        http_path       = HTTP_PATH,
        access_token    = DATABRICKS_TOKEN,
    )

@st.cache_data(ttl=300)  # cache query results for 5 minutes
def run_query(query: str) -> pd.DataFrame:
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=cols)

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🏏 Cricket Player Performance Intelligence")
st.caption(
    "Male international T20 data · Ball-by-ball analytics · "
    f"Source: Cricsheet | Pipeline: Databricks Medallion Architecture"
)

tab1, tab2, tab3 = st.tabs(["🔍 Player Explorer", "📅 Match Feed", "⚡ Anomaly Feed"])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — PLAYER EXPLORER
# ═══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("Player Explorer")
    st.caption("Search for any T20 international batter. Stats exclude super overs.")

    col_search, col_type = st.columns([3, 1])
    with col_search:
        player_name = st.text_input("Search player name", placeholder="e.g. Virat Kohli")
    with col_type:
        stat_type = st.radio("View stats as", ["Batting", "Bowling"], horizontal=True)

    if player_name:
        # ── Career stats card ────────────────────────────────────────────────
        if stat_type == "Batting":
            career_q = f"""
                SELECT batter, matches, career_runs,
                       ROUND(career_avg_sr, 1) AS career_sr,
                       highest_score, hundreds, fifties,
                       ROUND(rolling_avg_sr, 1) AS last_10_sr
                FROM {CATALOG}.{SCHEMA}.gold_player_career
                WHERE LOWER(batter) LIKE LOWER('%{player_name}%')
                ORDER BY career_runs DESC
                LIMIT 5
            """
            career_df = run_query(career_q)

            if career_df.empty:
                st.warning(f"No batter found matching '{player_name}'. Try a partial name.")
            else:
                # If multiple matches, let user pick
                if len(career_df) > 1:
                    chosen = st.selectbox("Multiple players found — select one:",
                                         career_df["batter"].tolist())
                    career_df = career_df[career_df["batter"] == chosen]

                row = career_df.iloc[0]
                st.markdown(f"### {row['batter']}")

                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Matches",       int(row["matches"]))
                m2.metric("Career Runs",   int(row["career_runs"]))
                m3.metric("Career SR",     row["career_sr"])
                m4.metric("Last 10 SR",    row["last_10_sr"],
                          delta=f"{round(float(row['last_10_sr']) - float(row['career_sr']), 1)}")
                m5.metric("100s / 50s",    f"{int(row['hundreds'])} / {int(row['fifties'])}")

                # ── SR trend chart ───────────────────────────────────────────
                trend_q = f"""
                    SELECT date, runs, balls_faced, strike_rate, event_name
                    FROM {CATALOG}.{SCHEMA}.gold_batting_by_match
                    WHERE LOWER(batter) = LOWER('{row["batter"]}')
                    ORDER BY date DESC
                    LIMIT 30
                """
                trend_df = run_query(trend_q).sort_values("date")

                if not trend_df.empty:
                    trend_df["date"] = pd.to_datetime(trend_df["date"])
                    fig = px.line(
                        trend_df, x="date", y="strike_rate",
                        markers=True,
                        title=f"{row['batter']} — Strike Rate (last 30 matches)",
                        labels={"strike_rate": "Strike Rate", "date": "Match Date"},
                        hover_data={"runs": True, "balls_faced": True, "event_name": True},
                        color_discrete_sequence=["#1f77b4"],
                    )
                    # Career average line
                    fig.add_hline(
                        y=float(row["career_sr"]),
                        line_dash="dash",
                        line_color="gray",
                        annotation_text=f"Career avg: {row['career_sr']}",
                    )
                    fig.update_layout(height=350, margin=dict(l=0, r=0, t=40, b=0))
                    st.plotly_chart(fig, use_container_width=True)

                # ── Phase breakdown chart ─────────────────────────────────────
                phase_q = f"""
                    SELECT phase, balls_faced, runs, strike_rate, dot_ball_pct
                    FROM {CATALOG}.{SCHEMA}.gold_phase_stats
                    WHERE LOWER(batter) = LOWER('{row["batter"]}')
                """
                phase_df = run_query(phase_q)

                if not phase_df.empty:
                    st.markdown("#### Phase Breakdown")
                    phase_df["phase"] = pd.Categorical(
                        phase_df["phase"], categories=["powerplay", "middle", "death"], ordered=True
                    )
                    phase_df = phase_df.sort_values("phase")

                    fig2 = go.Figure()
                    fig2.add_trace(go.Bar(
                        x=phase_df["phase"], y=phase_df["strike_rate"],
                        name="Strike Rate", marker_color=["#2196F3", "#FF9800", "#F44336"]
                    ))
                    fig2.update_layout(
                        title="Strike Rate by Phase",
                        yaxis_title="Strike Rate",
                        height=280,
                        margin=dict(l=0, r=0, t=40, b=0),
                    )
                    pcol1, pcol2 = st.columns([2, 1])
                    with pcol1:
                        st.plotly_chart(fig2, use_container_width=True)
                    with pcol2:
                        st.dataframe(
                            phase_df[["phase", "balls_faced", "runs", "strike_rate", "dot_ball_pct"]]
                            .rename(columns={
                                "phase": "Phase",
                                "balls_faced": "Balls",
                                "runs": "Runs",
                                "strike_rate": "SR",
                                "dot_ball_pct": "Dot%",
                            }),
                            hide_index=True
                        )

        else:  # Bowling
            bowl_q = f"""
                SELECT bowler,
                       COUNT(DISTINCT match_id) AS matches,
                       SUM(balls_bowled)         AS total_balls,
                       SUM(wickets)              AS total_wickets,
                       ROUND(SUM(runs_conceded) * 6.0 / SUM(balls_bowled), 2) AS career_economy,
                       ROUND(SUM(balls_bowled) * 1.0 / NULLIF(SUM(wickets), 0), 1) AS bowling_sr
                FROM {CATALOG}.{SCHEMA}.gold_bowling_by_match
                WHERE LOWER(bowler) LIKE LOWER('%{player_name}%')
                GROUP BY bowler
                ORDER BY total_wickets DESC
                LIMIT 5
            """
            bowl_career = run_query(bowl_q)

            if bowl_career.empty:
                st.warning(f"No bowler found matching '{player_name}'.")
            else:
                if len(bowl_career) > 1:
                    chosen = st.selectbox("Multiple players found:", bowl_career["bowler"].tolist())
                    bowl_career = bowl_career[bowl_career["bowler"] == chosen]

                row = bowl_career.iloc[0]
                st.markdown(f"### {row['bowler']}")

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Matches",    int(row["matches"]))
                m2.metric("Wickets",    int(row["total_wickets"]))
                m3.metric("Economy",    row["career_economy"])
                m4.metric("Bowling SR", row["bowling_sr"])

                # Economy trend
                econ_q = f"""
                    SELECT date, economy_rate, wickets, balls_bowled, event_name
                    FROM {CATALOG}.{SCHEMA}.gold_bowling_by_match
                    WHERE LOWER(bowler) = LOWER('{row["bowler"]}')
                    ORDER BY date DESC LIMIT 30
                """
                econ_df = run_query(econ_q).sort_values("date")
                if not econ_df.empty:
                    econ_df["date"] = pd.to_datetime(econ_df["date"])
                    fig = px.line(
                        econ_df, x="date", y="economy_rate", markers=True,
                        title=f"{row['bowler']} — Economy Rate (last 30 matches)",
                        hover_data={"wickets": True, "balls_bowled": True, "event_name": True},
                        color_discrete_sequence=["#e74c3c"],
                    )
                    fig.add_hline(y=float(row["career_economy"]), line_dash="dash",
                                  line_color="gray",
                                  annotation_text=f"Career avg: {row['career_economy']}")
                    fig.update_layout(height=350, margin=dict(l=0, r=0, t=40, b=0))
                    st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("👆 Type a player's name above to start exploring.")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — MATCH FEED
# ═══════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("Match Performance Feed")
    st.caption("Top batting performances per match. World Cup matches are highlighted.")

    filter_col1, filter_col2, filter_col3 = st.columns(3)

    with filter_col1:
        # Date range
        date_range = st.date_input(
            "Date range",
            value=(pd.to_datetime("2024-01-01"), pd.to_datetime("2026-12-31")),
        )

    with filter_col2:
        # Event filter
        events_q = f"""
            SELECT DISTINCT event_name
            FROM {CATALOG}.{SCHEMA}.gold_batting_by_match
            WHERE event_name IS NOT NULL
            ORDER BY event_name
        """
        events_df = run_query(events_q)
        events_list = ["All Events"] + events_df["event_name"].tolist()
        selected_event = st.selectbox("Event / Tournament", events_list)

    with filter_col3:
        min_runs = st.slider("Minimum runs scored", 0, 100, 30)

    # Build query
    date_from = date_range[0] if isinstance(date_range, tuple) else date_range
    date_to   = date_range[1] if isinstance(date_range, tuple) and len(date_range) > 1 else date_range

    event_filter = "" if selected_event == "All Events" else f"AND event_name = '{selected_event}'"

    match_q = f"""
        SELECT batter, date, event_name, source_dataset,
               runs, balls_faced, strike_rate, boundaries, sixes, dot_ball_pct
        FROM {CATALOG}.{SCHEMA}.gold_batting_by_match
        WHERE date BETWEEN '{date_from}' AND '{date_to}'
          AND runs >= {min_runs}
          {event_filter}
        ORDER BY runs DESC
        LIMIT 100
    """
    match_df = run_query(match_q)

    if match_df.empty:
        st.warning("No matches found for the selected filters.")
    else:
        # Format the DataFrame for display
        match_df["date"] = pd.to_datetime(match_df["date"]).dt.strftime("%d %b %Y")
        match_df["🏆"] = match_df["source_dataset"].apply(
            lambda x: "🏆 World Cup" if x == "icc_worldcup" else ""
        )

        st.dataframe(
            match_df[[
                "🏆", "batter", "date", "event_name",
                "runs", "balls_faced", "strike_rate", "boundaries", "sixes", "dot_ball_pct"
            ]].rename(columns={
                "batter": "Batter",
                "date": "Date",
                "event_name": "Event",
                "runs": "Runs",
                "balls_faced": "Balls",
                "strike_rate": "SR",
                "boundaries": "4s",
                "sixes": "6s",
                "dot_ball_pct": "Dot%",
            }),
            hide_index=True,
            use_container_width=True,
        )

        # World Cup vs overall comparison
        wc_count = match_df[match_df["🏆"] == "🏆 World Cup"].shape[0]
        st.caption(f"Showing {len(match_df)} performances · {wc_count} from World Cup")

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — ANOMALY FEED
# ═══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.subheader("⚡ Anomaly Detection Feed")
    st.caption(
        "Players whose recent form (last 10 matches) deviates significantly "
        "from their career baseline. Z-score > +2 = hot form, < -2 = slump."
    )

    # Load insights
    insights_q = f"""
        SELECT batter, matches, career_avg_sr, rolling_avg_sr,
               anomaly_score, anomaly_direction, highest_score, ai_insight, generated_at
        FROM {CATALOG}.{SCHEMA}.gold_anomaly_insights
        ORDER BY ABS(anomaly_score) DESC
    """
    insights_df = run_query(insights_q)

    if insights_df.empty:
        st.info("No anomalies flagged yet — run notebook 04_anomaly_agent to generate insights.")
    else:
        # Summary metrics
        hot   = insights_df[insights_df["anomaly_direction"] == "HOT_FORM"]
        slump = insights_df[insights_df["anomaly_direction"] == "SLUMP"]

        sm1, sm2, sm3 = st.columns(3)
        sm1.metric("Total Flagged Players", len(insights_df))
        sm2.metric("🟢 Hot Form",  len(hot))
        sm3.metric("🔴 In Slump", len(slump))

        # Filter
        direction_filter = st.radio(
            "Show:", ["All", "🟢 Hot Form Only", "🔴 Slump Only"], horizontal=True
        )
        if "Hot" in direction_filter:
            display_df = hot
        elif "Slump" in direction_filter:
            display_df = slump
        else:
            display_df = insights_df

        st.divider()

        # Render each player as a card
        for _, row in display_df.iterrows():
            is_hot = row["anomaly_direction"] == "HOT_FORM"
            color  = "#1b5e20" if is_hot else "#b71c1c"
            emoji  = "🟢" if is_hot else "🔴"
            label  = "HOT FORM" if is_hot else "SLUMP"
            delta  = round(float(row["rolling_avg_sr"]) - float(row["career_avg_sr"]), 1)
            delta_str = f"+{delta}" if delta > 0 else str(delta)

            with st.container():
                st.markdown(
                    f"""
                    <div style='background-color:{color}22; border-left: 4px solid {color};
                                border-radius: 4px; padding: 12px 16px; margin-bottom: 12px;'>
                        <strong style='font-size:16px;'>{emoji} {row["batter"]}</strong>
                        &nbsp;&nbsp;
                        <span style='color:{color}; font-weight:bold;'>{label}</span>
                        &nbsp;·&nbsp; z-score: <strong>{round(float(row["anomaly_score"]), 2)}</strong>
                        &nbsp;·&nbsp; {int(row["matches"])} matches
                    </div>
                    """,
                    unsafe_allow_html=True
                )

                c1, c2, c3 = st.columns(3)
                c1.metric("Career Avg SR",  round(float(row["career_avg_sr"]), 1))
                c2.metric("Recent Avg SR",  round(float(row["rolling_avg_sr"]), 1),
                           delta=delta_str)
                c3.metric("Anomaly Score",  round(float(row["anomaly_score"]), 2))

                if row["ai_insight"]:
                    st.markdown(f"> 🤖 **AI Insight:** {row['ai_insight']}")

                st.caption(f"Insight generated: {row['generated_at']}")
                st.divider()

# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown(
    """
    ---
    **Data source:** [Cricsheet](https://cricsheet.org) — ball-by-ball T20 international data
    **Pipeline:** Bronze → Silver → Gold → Anomaly Agent | Databricks Medallion Architecture
    **Model:** Anomaly detection via z-score on rolling 10-match strike rate vs career baseline
    """,
    unsafe_allow_html=False,
)
