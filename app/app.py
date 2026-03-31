"""
Cricket Intelligence Platform
Databricks Bootcamp Capstone — 3-page Dash app

Pages:
  /         → Player Scout   (individual analysis + AI verdict)
  /anomaly  → Anomaly Feed   (AI-narrated statistical outliers)
  /matchup  → Matchup Intel  (batter vs bowler H2H + AI tactics)

Tables (confirmed from Unity Catalog):
  tabular.dataexpert.prateek_capstone_project_gold_batter_career
  tabular.dataexpert.prateek_capstone_project_gold_batter_by_match
  tabular.dataexpert.prateek_capstone_project_gold_batter_by_phase
  tabular.dataexpert.prateek_capstone_project_gold_bowler_career
  tabular.dataexpert.prateek_capstone_project_gold_bowler_by_match
  tabular.dataexpert.prateek_capstone_project_gold_bowler_by_phase
  tabular.dataexpert.prateek_capstone_project_gold_matchup
  tabular.dataexpert.prateek_capstone_project_anomaly_narratives
"""

import os, json, subprocess, threading
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, html, dcc, Input, Output, State, no_update, ctx
import dash_bootstrap_components as dbc

try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

from databricks import sql

# ── Config ────────────────────────────────────────────────────────────────────

HOST      = os.environ.get("DATABRICKS_HOST", "dbc-7b106152-caf3.cloud.databricks.com")
WAREHOUSE = os.environ.get("DATABRICKS_WAREHOUSE_ID", "b15d3d6f837ba428")
BASE      = "tabular.dataexpert.prateek_capstone_project"

T = {
    "bc":  f"{BASE}_gold_batter_career",
    "bm":  f"{BASE}_gold_batter_by_match",
    "bp":  f"{BASE}_gold_batter_by_phase",
    "wc":  f"{BASE}_gold_bowler_career",
    "wm":  f"{BASE}_gold_bowler_by_match",
    "wp":  f"{BASE}_gold_bowler_by_phase",
    "mu":  f"{BASE}_gold_matchup",
    "an":  f"{BASE}_anomaly_narratives",
}

# ── App initialisation ────────────────────────────────────────────────────────

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title="Cricket Intelligence",
    update_title=None,
)
server = app.server

# ── Auth + Connection ─────────────────────────────────────────────────────────

_lock = threading.Lock()
_conn = None


def _get_token() -> str:
    t = os.environ.get("DATABRICKS_TOKEN", "")
    if t:
        return t
    try:
        profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "dbc-7b106152-caf3")
        r = subprocess.run(["databricks", "auth", "token", "--profile", profile],
                           capture_output=True, text=True, timeout=10)
        t = json.loads(r.stdout).get("access_token", "")
        if t:
            return t
    except Exception:
        pass
    try:
        from databricks.sdk import WorkspaceClient
        headers = WorkspaceClient().config.authenticate()
        t = headers.get("Authorization", "").replace("Bearer ", "").strip()
        if t:
            return t
    except Exception:
        pass
    return ""


def _make_conn():
    token = _get_token()
    if not token:
        raise RuntimeError("No Databricks token found.")
    return sql.connect(server_hostname=HOST,
                       http_path=f"/sql/1.0/warehouses/{WAREHOUSE}",
                       access_token=token)


def get_conn():
    global _conn
    with _lock:
        if _conn is None:
            _conn = _make_conn()
        return _conn


def run_q(q: str) -> pd.DataFrame:
    global _conn
    for attempt in range(2):
        try:
            cur = get_conn().cursor()
            cur.execute(q)
            cols = [d[0] for d in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
        except Exception as exc:
            if attempt == 0:
                with _lock:
                    _conn = None
            else:
                raise exc


# ── Helpers ───────────────────────────────────────────────────────────────────

def position_to_role(pos) -> str:
    try:
        p = int(float(pos))
    except (TypeError, ValueError):
        return "Unknown"
    if p <= 2:   return "Opener"
    if p == 3:   return "Top Order"
    if p <= 5:   return "Middle Order"
    if p <= 7:   return "Finisher"
    return "Lower Order"


def fmt(val, decimals=1, suffix="") -> str:
    try:
        return f"{float(val):.{decimals}f}{suffix}"
    except (TypeError, ValueError):
        return "—"


def _safe(name) -> str:
    """Escape single quotes in player names so they are safe to embed in SQL strings.
    e.g. "D'Souza" → "D''Souza"  (SQL standard escaping, not a backslash)
    This prevents both query failures and SQL injection from dropdown values.
    """
    return str(name).replace("'", "''")


def delta_class(val) -> str:
    try:
        return "delta-up" if float(val) > 0 else ("delta-down" if float(val) < 0 else "delta-flat")
    except (TypeError, ValueError):
        return "delta-flat"


def verdict_cls(zscore) -> str:
    try:
        z = float(zscore)
        if z > 0.5:    return "verdict-green"
        if z < -0.5:   return "verdict-red"
        return "verdict-amber"
    except (TypeError, ValueError):
        return "verdict-blue"


def make_trend_fig(df: pd.DataFrame, y_col: str, baseline: float,
                   color: str = "#1d4ed8", label: str = "Strike Rate") -> go.Figure:
    fig = go.Figure()
    if df.empty:
        return fig
    df = df.copy()
    df["match_date"] = pd.to_datetime(df["match_date"])
    df = df.sort_values("match_date")

    fig.add_trace(go.Scatter(
        x=df["match_date"], y=df[y_col],
        mode="lines+markers",
        line=dict(color=color, width=2),
        marker=dict(size=5, color=color),
        name=label,
        hovertemplate="<b>%{x|%d %b %Y}</b><br>" + label + ": %{y}<br>" +
                      "vs %{customdata[0]}<extra></extra>",
        customdata=df[["opponent"]].values if "opponent" in df.columns else df[[y_col]].values,
    ))

    fig.add_hline(y=baseline, line_dash="dash", line_color="#94a3b8", line_width=1.5,
                  annotation_text=f"Career avg  {fmt(baseline)}",
                  annotation_font_color="#94a3b8", annotation_font_size=10)

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Inter, sans-serif", size=12, color="#0f172a"),
        height=240,
        margin=dict(l=4, r=4, t=12, b=4),
        xaxis=dict(showgrid=False, color="#94a3b8", tickfont=dict(size=10)),
        yaxis=dict(showgrid=True, gridcolor="#f1f5f9", color="#94a3b8", tickfont=dict(size=10)),
        legend=dict(orientation="h", x=0, y=1.1, font=dict(size=10),
                    bgcolor="rgba(0,0,0,0)"),
        hovermode="x unified",
    )
    return fig


# ── Navbar ────────────────────────────────────────────────────────────────────

def navbar(active: str = "/"):
    links = [("/", "🏏 Player Scout"),
             ("/anomaly", "⚡ Anomaly Feed"),
             ("/matchup", "🎯 Matchup Intel")]
    return html.Div([
        html.Div([
            html.Span("🏏", style={"fontSize": "20px"}),
            html.Span("Cricket", style={"color": "#1d4ed8", "fontWeight": "800"}),
            html.Span(" Intelligence", style={"fontWeight": "700"}),
        ], className="navbar-brand"),
        html.Nav([
            html.A(label, href=href,
                   className=f"nav-link {'nav-link-active' if href == active else ''}")
            for href, label in links
        ], className="nav-links"),
    ], className="navbar")


# ── Placeholder ───────────────────────────────────────────────────────────────

def placeholder(icon: str, text: str, subtext: str = ""):
    return html.Div([
        html.Div(icon, className="placeholder-icon"),
        html.Div(text, style={"fontWeight": "600", "marginBottom": "4px"}),
        html.Div(subtext, style={"fontSize": "12px"}) if subtext else None,
    ], className="placeholder")


def data_disclaimer():
    return html.Div(
        "Stats sourced from Cricsheet (cricsheet.org). Coverage may not include every match. "
        "Values are consistent with official records but may differ slightly due to data availability.",
        className="data-disclaimer"
    )


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 1 — PLAYER SCOUT
# ─────────────────────────────────────────────────────────────────────────────

def page_scout():
    return html.Div([
        html.Div([
            html.Div("Player Scout", className="page-title"),
            html.Div("Individual performance analysis with AI-powered role assessment.",
                     className="page-subtitle"),
            data_disclaimer(),
        ]),

        # Controls
        html.Div([
            html.Div([
                html.Div("BATTER / BOWLER", className="control-label-text"),
                html.Div([
                    html.Button("BAT",  id="scout-bat",  n_clicks=0, className="toggle-btn toggle-btn-active"),
                    html.Button("BOWL", id="scout-bowl", n_clicks=0, className="toggle-btn"),
                ], className="toggle-group"),
            ], className="control-group"),

            html.Div([
                html.Div("SELECT PLAYER", className="control-label-text"),
                dcc.Dropdown(id="scout-player", options=[], value="RG Sharma",
                             placeholder="Search or select…",
                             searchable=True, clearable=True,
                             className="clean-dropdown", style={"width": "320px"}),
            ], className="control-group"),
        ], className="controls-bar"),

        dcc.Store(id="scout-type", data="bat"),

        dcc.Loading(type="circle", color="#1d4ed8",
                    children=html.Div(id="scout-content",
                                      children=placeholder("🏏",
                                          "Select a player above to load their story",
                                          "Choose BAT or BOWL, then pick a name"))),
    ], className="page-wrapper")


# ── Scout callbacks ───────────────────────────────────────────────────────────

@app.callback(
    Output("scout-player", "options"),
    Output("scout-player", "value"),
    Input("url", "pathname"),
    Input("scout-type", "data"),
)
def load_scout_options(pathname, stype):
    if pathname != "/":
        return no_update, no_update
    default = "RG Sharma" if stype != "bowl" else no_update
    try:
        if stype == "bowl":
            df = run_q(f"SELECT bowler AS name FROM {T['wc']} ORDER BY matches DESC LIMIT 500")
            default = df.iloc[0]["name"] if not df.empty else no_update
        else:
            df = run_q(f"SELECT batter AS name FROM {T['bc']} ORDER BY matches DESC LIMIT 500")
        return [{"label": r["name"], "value": r["name"]} for _, r in df.iterrows()], default
    except Exception as e:
        return [{"label": f"⚠ {e}", "value": "", "disabled": True}], no_update


@app.callback(
    Output("scout-type", "data"),
    Output("scout-bat",  "className"),
    Output("scout-bowl", "className"),
    Input("scout-bat",  "n_clicks"),
    Input("scout-bowl", "n_clicks"),
    prevent_initial_call=True,
)
def toggle_scout_type(bat, bowl):
    triggered = ctx.triggered_id
    is_bat = (triggered != "scout-bowl")
    base, active = "toggle-btn", "toggle-btn toggle-btn-active"
    return ("bat" if is_bat else "bowl",
            active if is_bat else base,
            base if is_bat else active)


@app.callback(
    Output("scout-player", "options", allow_duplicate=True),
    Output("scout-player", "value",   allow_duplicate=True),
    Input("scout-type", "data"),
    State("url", "pathname"),
    prevent_initial_call=True,
)
def refresh_options_on_toggle(stype, pathname):
    if pathname != "/":
        return no_update, no_update
    try:
        if stype == "bowl":
            df = run_q(f"SELECT bowler AS name FROM {T['wc']} ORDER BY matches DESC LIMIT 500")
            default = df.iloc[0]["name"] if not df.empty else None
        else:
            df = run_q(f"SELECT batter AS name FROM {T['bc']} ORDER BY matches DESC LIMIT 500")
            default = "RG Sharma"
        return [{"label": r["name"], "value": r["name"]} for _, r in df.iterrows()], default
    except Exception as e:
        return [{"label": f"⚠ {e}", "value": "", "disabled": True}], no_update


@app.callback(
    Output("scout-content", "children"),
    Input("scout-player", "value"),
    State("scout-type",   "data"),
)
def render_scout(player, stype):
    if not player:
        return placeholder("🏏", "Select a player above to load their story")
    try:
        return _render_batter(player) if stype == "bat" else _render_bowler(player)
    except Exception as e:
        return html.Div(f"Error: {e}", className="error-box")


def _render_batter(name: str):
    safe_name = _safe(name)   # escape ' → '' to prevent SQL errors on names like D'Souza
    # Career stats — use strike_rate (true overall SR = runs/balls*100), not career_avg_sr
    df = run_q(f"""
        SELECT batter, matches, runs, balls_faced, strike_rate AS career_sr,
               rolling_avg_sr, batting_avg, dot_pct, boundary_pct,
               form_trajectory, impact_score, consistency_label
        FROM {T['bc']}
        WHERE LOWER(batter) = LOWER('{safe_name}')
    """)
    if df.empty:
        return placeholder("🔍", f"No data found for '{name}'",
                           "Try searching with a different spelling")
    r = df.iloc[0]

    # Typical batting position + team
    safe_batter = _safe(r['batter'])
    pos_df = run_q(f"""
        SELECT batting_position, team_batting, COUNT(*) as cnt
        FROM {T['bm']} WHERE batter = '{safe_batter}'
        GROUP BY batting_position, team_batting
        ORDER BY cnt DESC LIMIT 1
    """)

    # All match-level aggregates in one query:
    # highest_score, hundreds, fifties, fours, sixes — all correctly computed at match grain
    ml_df = run_q(f"""
        SELECT
            MAX(runs)                                              AS highest_score,
            SUM(CASE WHEN runs >= 100 THEN 1 ELSE 0 END)          AS hundreds,
            SUM(CASE WHEN runs >= 50 AND runs < 100 THEN 1 ELSE 0 END) AS fifties,
            SUM(fours)                                             AS fours,
            SUM(sixes)                                             AS sixes
        FROM {T['bm']} WHERE batter = '{safe_batter}'
    """)
    position      = pos_df.iloc[0]["batting_position"] if not pos_df.empty else None
    team          = pos_df.iloc[0]["team_batting"]     if not pos_df.empty else "—"
    role          = position_to_role(position)
    if not ml_df.empty and ml_df.iloc[0]["highest_score"] is not None:
        ml            = ml_df.iloc[0]
        highest_score = int(ml["highest_score"])
        hundreds      = int(ml["hundreds"])
        fifties       = int(ml["fifties"])
        fours         = int(ml["fours"])
        sixes         = int(ml["sixes"])
    else:
        highest_score = hundreds = fifties = fours = sixes = 0

    # Trend data
    trend = run_q(f"""
        SELECT match_date, runs, strike_rate, opponent
        FROM {T['bm']} WHERE batter = '{safe_batter}'
        ORDER BY match_date ASC
    """)

    # Phase stats — case-insensitive phase match guards against Silver table casing variations
    phase_df = run_q(f"""
        SELECT phase, strike_rate, batting_avg, dot_pct, boundary_pct, matches
        FROM {T['bp']} WHERE batter = '{safe_batter}'
    """)
    # Normalise phase names so Powerplay/powerplay/POWERPLAY all map correctly
    if not phase_df.empty:
        phase_df["phase"] = phase_df["phase"].str.capitalize()

    # AI verdict
    verdict_df = run_q(f"""
        SELECT recommended_action, sr_delta, sr_zscore, anomaly_type
        FROM {T['an']}
        WHERE LOWER(player) = LOWER('{safe_batter}') AND entity_type = 'batter'
        LIMIT 1
    """)

    # Form delta: rolling avg SR vs true overall career SR
    career_sr = float(r["career_sr"] or 0)
    rolling_sr = float(r["rolling_avg_sr"] or 0)
    delta = round(rolling_sr - career_sr, 1)
    delta_sign = "+" if delta >= 0 else ""
    delta_cls  = "delta-up" if delta >= 0 else "delta-down"
    traj = str(r.get("form_trajectory", "Stable"))

    # Trend chart baseline = true career SR
    fig = make_trend_fig(trend, "strike_rate", career_sr, "#1d4ed8", "Strike Rate")

    # Phase cards
    phase_cards = []
    for phase, cls in [("Powerplay","phase-card-pp"), ("Middle","phase-card-mid"), ("Death","phase-card-death")]:
        ph = phase_df[phase_df["phase"] == phase]
        if not ph.empty:
            p = ph.iloc[0]
            phase_cards.append(html.Div([
                html.Div(phase.upper(), className="phase-label"),
                html.Div([
                    html.Div([
                        html.Div(fmt(p["strike_rate"]), className="phase-stat-val"),
                        html.Div("Strike Rate", className="phase-stat-lbl"),
                    ], className="phase-stat-item"),
                    html.Div([
                        html.Div(fmt(p["batting_avg"]), className="phase-stat-val"),
                        html.Div("Average", className="phase-stat-lbl"),
                    ], className="phase-stat-item"),
                    html.Div([
                        html.Div(fmt(p["boundary_pct"]) + "%", className="phase-stat-val"),
                        html.Div("Boundary %", className="phase-stat-lbl"),
                    ], className="phase-stat-item"),
                    html.Div([
                        html.Div(fmt(p["dot_pct"]) + "%", className="phase-stat-val"),
                        html.Div("Dot Ball %", className="phase-stat-lbl"),
                    ], className="phase-stat-item"),
                ], className="phase-stats"),
            ], className=f"phase-card {cls}"))
        else:
            phase_cards.append(html.Div([
                html.Div(phase.upper(), className="phase-label"),
                html.Div("No data", style={"color": "#94a3b8", "fontSize": "12px"}),
            ], className=f"phase-card {cls}"))

    # AI verdict box
    if not verdict_df.empty:
        v = verdict_df.iloc[0]
        vcls = verdict_cls(v.get("sr_zscore", 0))
        verdict_box = html.Div([
            html.Div("🤖  AI ANALYST RECOMMENDATION", className="verdict-header"),
            html.Div(str(v["recommended_action"]), className="verdict-text"),
        ], className=f"verdict-box {vcls}")
    else:
        verdict_box = html.Div([
            html.Div("📊  FORM SUMMARY", className="verdict-header"),
            html.Div(f"{r['batter']} has played {r['matches']} T20 internationals, "
                     f"scoring {r['runs']} runs at a career strike rate of {fmt(career_sr)}. "
                     f"Current form is {traj.lower()}.",
                     className="verdict-text"),
        ], className="verdict-box verdict-blue")

    return html.Div([
        # Player header
        html.Div([
            html.Div(r["batter"], className="player-name"),
            html.Div([
                html.Span(team, className="badge badge-white"),
                html.Span(role, className="badge badge-white"),
                html.Span(f"#{int(float(position)) if position else '?'}", className="badge badge-white"),
                html.Span(traj, className="badge badge-white"),
            ], className="player-meta"),
        ], className="player-header-card"),

        # Key stats
        html.Div([
            html.Div([html.Div("MATCHES",    className="stat-label"),
                      html.Div(str(r["matches"]),  className="stat-value"),], className="stat-tile"),
            html.Div([html.Div("RUNS",       className="stat-label"),
                      html.Div(str(r["runs"]),     className="stat-value"),], className="stat-tile"),
            html.Div([html.Div("CAREER SR",  className="stat-label"),
                      html.Div(fmt(career_sr), className="stat-value"),], className="stat-tile"),
            html.Div([html.Div("CURRENT FORM SR", className="stat-label"),
                      html.Div([
                          html.Span(fmt(rolling_sr), className="stat-value"),
                          html.Span(f"  {delta_sign}{fmt(delta)}", className=delta_cls,
                                    style={"fontSize": "14px", "marginLeft": "6px"}),
                      ]),], className="stat-tile"),
            html.Div([html.Div("AVERAGE",    className="stat-label"),
                      html.Div(fmt(r["batting_avg"]), className="stat-value"),], className="stat-tile"),
            html.Div([html.Div("IMPACT SCORE", className="stat-label"),
                      html.Div(fmt(r["impact_score"]), className="stat-value"),], className="stat-tile"),
            html.Div([html.Div("HIGHEST SCORE", className="stat-label"),
                      html.Div(str(highest_score), className="stat-value"),], className="stat-tile"),
            html.Div([html.Div("100s / 50s", className="stat-label"),
                      html.Div(f"{hundreds} / {fifties}", className="stat-value"),], className="stat-tile"),
        ], className="stat-grid", style={"marginBottom": "20px"}),

        # Phase breakdown
        html.Div("Phase Breakdown", className="section-header"),
        html.Div(phase_cards, className="stat-grid stat-grid-3",
                 style={"marginBottom": "16px"}),

        # AI verdict / form summary
        verdict_box,

        # Form trend chart — at the bottom
        html.Div([
            html.Div("Strike Rate — Match by Match", className="card-title"),
            dcc.Graph(figure=fig, config={"displayModeBar": False}),
        ], className="card", style={"marginTop": "16px"}),
    ])


def _render_bowler(name: str):
    safe_name = _safe(name)   # escape ' → '' for names like D'Souza
    df = run_q(f"""
        SELECT bowler, matches, wickets, economy, bowling_avg, bowling_sr,
               dot_pct, boundary_pct_conceded, form_trajectory,
               impact_score, rolling_avg_economy, career_avg_economy,
               economy_vs_career_delta
        FROM {T['wc']}
        WHERE LOWER(bowler) = LOWER('{safe_name}')
    """)
    if df.empty:
        return placeholder("🔍", f"No data found for '{name}'")
    r = df.iloc[0]

    safe_bowler = _safe(r['bowler'])
    trend = run_q(f"""
        SELECT match_date, economy, wickets,
               -- opponent = whichever team is NOT the bowler's team (team_away is the batting side
               -- when bowler's team is listed as team_home, so team_away is the opposition)
               COALESCE(team_away, team_home, '') AS opponent
        FROM {T['wm']} WHERE bowler = '{safe_bowler}'
        ORDER BY match_date ASC
    """)

    # Normalise phase names so Powerplay/powerplay/POWERPLAY all match
    phase_df = run_q(f"""
        SELECT phase, economy, bowling_sr, dot_pct, boundary_pct_conceded, wickets, matches
        FROM {T['wp']} WHERE bowler = '{safe_bowler}'
    """)
    if not phase_df.empty:
        phase_df["phase"] = phase_df["phase"].str.capitalize()

    verdict_df = run_q(f"""
        SELECT recommended_action, sr_zscore
        FROM {T['an']}
        WHERE LOWER(player) = LOWER('{safe_bowler}') AND entity_type = 'bowler'
        LIMIT 1
    """)

    delta  = float(r["economy_vs_career_delta"] or 0)
    delta_sign = "+" if delta >= 0 else ""
    delta_cls2 = "delta-down" if delta >= 0 else "delta-up"  # higher economy = worse
    traj = str(r.get("form_trajectory", "Stable"))

    baseline = float(r["career_avg_economy"] or 0)
    fig = make_trend_fig(trend, "economy", baseline, "#7c3aed", "Economy")

    phase_cards = []
    for phase, cls in [("Powerplay","phase-card-pp"), ("Middle","phase-card-mid"), ("Death","phase-card-death")]:
        ph = phase_df[phase_df["phase"] == phase]
        if not ph.empty:
            p = ph.iloc[0]
            phase_cards.append(html.Div([
                html.Div(phase.upper(), className="phase-label"),
                html.Div([
                    html.Div([html.Div(fmt(p["economy"]),   className="phase-stat-val"),
                              html.Div("Economy",           className="phase-stat-lbl")], className="phase-stat-item"),
                    html.Div([html.Div(str(p["wickets"]),   className="phase-stat-val"),
                              html.Div("Wickets",           className="phase-stat-lbl")], className="phase-stat-item"),
                    html.Div([html.Div(fmt(p["dot_pct"])+"%", className="phase-stat-val"),
                              html.Div("Dot %",             className="phase-stat-lbl")], className="phase-stat-item"),
                    html.Div([html.Div(fmt(p["boundary_pct_conceded"])+"%", className="phase-stat-val"),
                              html.Div("Boundary %",        className="phase-stat-lbl")], className="phase-stat-item"),
                ], className="phase-stats"),
            ], className=f"phase-card {cls}"))
        else:
            phase_cards.append(html.Div([
                html.Div(phase.upper(), className="phase-label"),
                html.Div("No data", style={"color": "#94a3b8", "fontSize": "12px"}),
            ], className=f"phase-card {cls}"))

    if not verdict_df.empty:
        v = verdict_df.iloc[0]
        vcls = verdict_cls(v.get("sr_zscore", 0))
        verdict_box = html.Div([
            html.Div("🤖  AI ANALYST RECOMMENDATION", className="verdict-header"),
            html.Div(str(v["recommended_action"]), className="verdict-text"),
        ], className=f"verdict-box {vcls}")
    else:
        verdict_box = html.Div([
            html.Div("📊  BOWLING SUMMARY", className="verdict-header"),
            html.Div(f"{r['bowler']} has taken {r['wickets']} wickets in {r['matches']} matches "
                     f"at a career economy of {fmt(r['career_avg_economy'], 2)} "
                     f"(current form: {fmt(r['rolling_avg_economy'], 2)}). "
                     f"Form trajectory: {traj.lower()}.",
                     className="verdict-text"),
        ], className="verdict-box verdict-blue")

    return html.Div([
        html.Div([
            html.Div(r["bowler"], className="player-name"),
            html.Div([
                html.Span("Bowler", className="badge badge-white"),
                html.Span(traj, className="badge badge-white"),
            ], className="player-meta"),
        ], className="player-header-card"),

        html.Div([
            html.Div([html.Div("MATCHES",   className="stat-label"),
                      html.Div(str(r["matches"]), className="stat-value")], className="stat-tile"),
            html.Div([html.Div("WICKETS",   className="stat-label"),
                      html.Div(str(r["wickets"]), className="stat-value",
                               style={"color": "#7c3aed"})], className="stat-tile"),
            html.Div([html.Div("ECONOMY",   className="stat-label"),
                      html.Div(fmt(r["economy"], 2), className="stat-value")], className="stat-tile"),
            html.Div([html.Div("CURRENT FORM ECO", className="stat-label"),
                      html.Div([
                          html.Span(fmt(r["rolling_avg_economy"], 2), className="stat-value"),
                          html.Span(f"  {delta_sign}{fmt(delta, 2)}", className=delta_cls2,
                                    style={"fontSize": "14px", "marginLeft": "6px"}),
                      ])], className="stat-tile"),
            html.Div([html.Div("BOWLING AVG", className="stat-label"),
                      html.Div(fmt(r["bowling_avg"]), className="stat-value")], className="stat-tile"),
            html.Div([html.Div("BOWLING SR",  className="stat-label"),
                      html.Div(fmt(r["bowling_sr"]), className="stat-value")], className="stat-tile"),
            html.Div([html.Div("DOT BALL %",  className="stat-label"),
                      html.Div(fmt(r["dot_pct"]) + "%", className="stat-value")], className="stat-tile"),
            html.Div([html.Div("IMPACT SCORE", className="stat-label"),
                      html.Div(fmt(r["impact_score"]), className="stat-value")], className="stat-tile"),
        ], className="stat-grid", style={"marginBottom": "20px"}),

        # Phase breakdown first (same order as batter page)
        html.Div("Phase Breakdown", className="section-header"),
        html.Div(phase_cards, className="stat-grid stat-grid-3",
                 style={"marginBottom": "16px"}),

        verdict_box,

        # Trend chart at the bottom (consistent with batter page layout)
        html.Div([
            html.Div("Economy Rate — Match by Match", className="card-title"),
            dcc.Graph(figure=fig, config={"displayModeBar": False}),
        ], className="card", style={"marginTop": "16px"}),
    ])


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 2 — ANOMALY FEED
# ─────────────────────────────────────────────────────────────────────────────

def page_anomaly():
    return html.Div([
        html.Div([
            html.Div("Anomaly Feed", className="page-title"),
            html.Div("AI-generated insights on players whose form has deviated from their career baseline.",
                     className="page-subtitle"),
            data_disclaimer(),
        ]),

        # Filters
        html.Div([
            html.Div([
                html.Div("SIGNAL TYPE", className="control-label-text"),
                html.Div([
                    html.Button("All",         id="sig-all",   n_clicks=0, className="toggle-btn toggle-btn-active"),
                    html.Button("Hot Streaks", id="sig-hot",   n_clicks=0, className="toggle-btn"),
                    html.Button("Form Slumps", id="sig-slump", n_clicks=0, className="toggle-btn"),
                ], className="toggle-group"),
            ], className="control-group"),

            html.Div([
                html.Div("PLAYER TYPE", className="control-label-text"),
                html.Div([
                    html.Button("All",     id="etype-all",    n_clicks=0, className="toggle-btn toggle-btn-active"),
                    html.Button("Batters", id="etype-batter", n_clicks=0, className="toggle-btn"),
                    html.Button("Bowlers", id="etype-bowler", n_clicks=0, className="toggle-btn"),
                ], className="toggle-group"),
            ], className="control-group"),

            html.Div([
                html.Div([
                    html.Span("MIN DEVIATION:  ", className="control-label-text",
                              style={"display": "inline"}),
                    html.Span(id="threshold-label", children="0.5σ",
                              style={"fontWeight": "700", "color": "#1d4ed8", "fontSize": "13px"}),
                ]),
                dcc.Slider(id="an-threshold", min=0.2, max=1.2, step=0.1, value=0.5,
                           marks={0.2: "0.2", 0.5: "0.5", 0.8: "0.8", 1.0: "1.0", 1.2: "1.2"},
                           tooltip={"always_visible": False}),
            ], className="control-group", style={"minWidth": "280px"}),
        ], className="controls-bar"),

        dcc.Store(id="an-signal", data="ALL"),
        dcc.Store(id="an-etype",  data="ALL"),

        dcc.Loading(type="circle", color="#1d4ed8",
                    children=html.Div(id="anomaly-content")),
    ], className="page-wrapper")


@app.callback(
    Output("an-signal", "data"),
    Output("sig-all",   "className"),
    Output("sig-hot",   "className"),
    Output("sig-slump", "className"),
    Input("sig-all",    "n_clicks"),
    Input("sig-hot",    "n_clicks"),
    Input("sig-slump",  "n_clicks"),
    prevent_initial_call=True,
)
def toggle_signal(a, h, s):
    t = ctx.triggered_id
    sig = {"sig-all": "ALL", "sig-hot": "HOT", "sig-slump": "SLUMP"}.get(t, "ALL")
    b, ac = "toggle-btn", "toggle-btn toggle-btn-active"
    return sig, ac if sig == "ALL" else b, ac if sig == "HOT" else b, ac if sig == "SLUMP" else b


@app.callback(
    Output("an-etype",      "data"),
    Output("etype-all",     "className"),
    Output("etype-batter",  "className"),
    Output("etype-bowler",  "className"),
    Input("etype-all",      "n_clicks"),
    Input("etype-batter",   "n_clicks"),
    Input("etype-bowler",   "n_clicks"),
    prevent_initial_call=True,
)
def toggle_etype(a, b, w):
    t = ctx.triggered_id
    et = {"etype-all": "ALL", "etype-batter": "batter", "etype-bowler": "bowler"}.get(t, "ALL")
    base, ac = "toggle-btn", "toggle-btn toggle-btn-active"
    return et, ac if et == "ALL" else base, ac if et == "batter" else base, ac if et == "bowler" else base


@app.callback(
    Output("threshold-label", "children"),
    Input("an-threshold", "value"),
)
def update_threshold_label(v):
    return f"{v:.1f}σ"


@app.callback(
    Output("anomaly-content", "children"),
    Input("an-signal",    "data"),
    Input("an-etype",     "data"),
    Input("an-threshold", "value"),
)
def render_anomaly(signal, etype, threshold):
    threshold = threshold or 0.5

    delta_filter = ""
    if signal == "HOT":   delta_filter = "AND sr_delta > 0"
    elif signal == "SLUMP": delta_filter = "AND sr_delta < 0"

    etype_filter = "" if etype == "ALL" else f"AND entity_type = '{etype}'"

    try:
        df = run_q(f"""
            SELECT player, entity_type, anomaly_type, severity,
                   ROUND(career_metric, 1)  AS career_metric,
                   ROUND(recent_metric, 1)  AS recent_metric,
                   ROUND(sr_delta, 1)       AS sr_delta,
                   ROUND(sr_zscore, 2)      AS sr_zscore,
                   recommended_action
            FROM {T['an']}
            WHERE ABS(sr_zscore) >= {threshold}
            {delta_filter}
            {etype_filter}
            ORDER BY ABS(sr_zscore) DESC
            LIMIT 30
        """)
    except Exception as e:
        return html.Div(f"Error loading feed: {e}", className="error-box")

    if df.empty:
        return placeholder("🔍", "No anomalies match these filters",
                           "Try lowering the threshold or changing the signal type")

    cards = []
    for _, r in df.iterrows():
        delta    = float(r["sr_delta"] or 0)
        zscore   = float(r["sr_zscore"] or 0)
        is_hot   = delta > 0
        d_cls    = "anomaly-delta-positive" if is_hot else "anomaly-delta-negative"
        pct      = round(delta / float(r["career_metric"] or 1) * 100) if r["career_metric"] else 0
        metric_label = "STRIKE RATE" if r["entity_type"] == "batter" else "ECONOMY"
        atype    = str(r["anomaly_type"]).replace("_", " ").title()

        cards.append(
            dbc.Col(html.Div([
                # Header
                html.Div([
                    html.Div([
                        html.Div(r["player"], className="anomaly-player-name"),
                        html.Div([
                            html.Span(r["entity_type"].title(), className="badge badge-blue",
                                      style={"marginRight": "6px"}),
                            html.Span(atype, className="badge badge-grey"),
                        ]),
                    ]),
                    html.Div([
                        html.Div(f"{'+' if is_hot else ''}{pct}%" if abs(pct) > 2 else f"{abs(zscore):.1f}σ",
                                 className=d_cls),
                        html.Div(metric_label, style={"fontSize": "10px", "color": "#94a3b8",
                                                       "textAlign": "right", "marginTop": "2px"}),
                    ], style={"textAlign": "right"}),
                ], style={"display": "flex", "justifyContent": "space-between",
                           "alignItems": "flex-start"}),

                # Career vs recent
                html.Div([
                    html.Div([
                        html.Div("CAREER",              className="anomaly-stat-label"),
                        html.Div(fmt(r["career_metric"]), className="anomaly-stat-value"),
                    ], className="anomaly-stat-col"),
                    html.Div([
                        html.Div("RECENT (L10)",         className="anomaly-stat-label"),
                        html.Div(fmt(r["recent_metric"]), className="anomaly-stat-value",
                                 style={"color": "#16a34a" if is_hot else "#dc2626"}),
                    ], className="anomaly-stat-col"),
                    html.Div([
                        html.Div("Z-SCORE",             className="anomaly-stat-label"),
                        html.Div(fmt(zscore, 2),         className="anomaly-stat-value"),
                    ], className="anomaly-stat-col"),
                ], className="anomaly-stat-row"),

                # AI recommendation
                html.Div(str(r["recommended_action"]) if r.get("recommended_action") else "—",
                         className="anomaly-action"),
            ], className="anomaly-card"), width=12, md=6, style={"marginBottom": "12px"})
        )

    return dbc.Row(cards)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 3 — MATCHUP INTEL
# ─────────────────────────────────────────────────────────────────────────────

def page_matchup():
    return html.Div([
        html.Div([
            html.Div("Matchup Intel", className="page-title"),
            html.Div("Head-to-head analysis with AI tactical recommendations for team selectors.",
                     className="page-subtitle"),
            data_disclaimer(),
        ]),

        html.Div([
            html.Div([
                html.Div("SELECT BATTER  🏏", className="control-label-text"),
                dcc.Dropdown(id="mu-batter", options=[], placeholder="Search batter…",
                             searchable=True, clearable=True,
                             className="clean-dropdown", style={"width": "280px"}),
            ], className="control-group"),

            html.Span("VS", style={"fontWeight": "800", "fontSize": "16px",
                                    "color": "#94a3b8", "alignSelf": "flex-end",
                                    "paddingBottom": "8px"}),

            html.Div([
                html.Div("SELECT BOWLER  🎳", className="control-label-text"),
                dcc.Dropdown(id="mu-bowler", options=[], placeholder="Search bowler…",
                             searchable=True, clearable=True,
                             className="clean-dropdown", style={"width": "280px"}),
            ], className="control-group"),
        ], className="controls-bar"),

        dcc.Loading(type="circle", color="#1d4ed8",
                    children=html.Div(id="matchup-content",
                                      children=placeholder("🎯",
                                          "Select both a batter and a bowler",
                                          "Minimum 6 balls required for H2H data"))),
    ], className="page-wrapper")


@app.callback(
    Output("mu-batter", "options"),
    Output("mu-bowler", "options"),
    Input("url", "pathname"),
)
def load_matchup_options(pathname):
    if pathname != "/matchup":
        return no_update, no_update
    try:
        batters = run_q(f"SELECT batter AS name FROM {T['bc']} ORDER BY matches DESC LIMIT 500")
        b_opts  = [{"label": r["name"], "value": r["name"]} for _, r in batters.iterrows()]
    except Exception as e:
        b_opts = [{"label": f"⚠ {e}", "value": "", "disabled": True}]
    try:
        bowlers = run_q(f"SELECT bowler AS name FROM {T['wc']} ORDER BY matches DESC LIMIT 500")
        w_opts  = [{"label": r["name"], "value": r["name"]} for _, r in bowlers.iterrows()]
    except Exception as e:
        w_opts = [{"label": f"⚠ {e}", "value": "", "disabled": True}]
    return b_opts, w_opts


@app.callback(
    Output("matchup-content", "children"),
    Input("mu-batter", "value"),
    Input("mu-bowler", "value"),
    prevent_initial_call=True,
)
def render_matchup(batter, bowler):
    if not batter or not bowler:
        return placeholder("🎯", "Select both a batter and a bowler",
                           "Minimum 6 balls required for H2H data")
    safe_batter = _safe(batter)
    safe_bowler = _safe(bowler)
    try:
        df = run_q(f"""
            SELECT batter, bowler, balls, runs, dismissals, matches_faced,
                   ROUND(strike_rate, 1)  AS strike_rate,
                   ROUND(batting_avg, 1)  AS batting_avg,
                   ROUND(dot_pct, 1)      AS dot_pct,
                   ROUND(boundary_pct, 1) AS boundary_pct,
                   ROUND(dismissal_rate * 100, 1) AS dismissal_pct
            FROM {T['mu']}
            WHERE LOWER(batter) = LOWER('{safe_batter}') AND LOWER(bowler) = LOWER('{safe_bowler}')
        """)

        if df.empty:
            return placeholder("🔍",
                f"No H2H data for {batter} vs {bowler}",
                "Need at least 6 balls faced between them")

        r = df.iloc[0]
        sr    = float(r["strike_rate"] or 0)
        drate = float(r["dismissal_pct"] or 0)

        is_batter_adv = sr > 120
        adv_label = "BATTER ADVANTAGE" if is_batter_adv else "BOWLER ADVANTAGE"
        adv_color = "#16a34a"            if is_batter_adv else "#dc2626"
        adv_bg    = "#dcfce7"            if is_batter_adv else "#fee2e2"

        # Dangermen: best bowlers vs this batter
        danger_df = run_q(f"""
            SELECT bowler, balls, runs, dismissals,
                   ROUND(strike_rate, 1)      AS sr,
                   ROUND(dot_pct, 1)          AS dot_pct,
                   ROUND(dismissal_rate * 100, 1) AS dismissal_pct
            FROM {T['mu']}
            WHERE LOWER(batter) = LOWER('{safe_batter}') AND balls >= 12
            ORDER BY dismissal_rate DESC
            LIMIT 6
        """)

        # AI tactical insight (try live, fallback to rule-based)
        ai_text = None
        try:
            ai_df = run_q(f"""
                SELECT ai_query(
                    'databricks-meta-llama-3-3-70b-instruct',
                    CONCAT(
                        'You are a T20 cricket analyst. ',
                        'Batter: {_safe(r["batter"])}. Bowler: {_safe(r["bowler"])}. ',
                        'Head to head: {r["balls"]} balls, {r["runs"]} runs, ',
                        'strike rate {r["strike_rate"]}, {r["dismissals"]} dismissals, ',
                        'dot ball %: {r["dot_pct"]}%. ',
                        'Write exactly 2 sentences: one tactical recommendation for the captain ',
                        'and one about the key moment to use this bowler.'
                    )
                ) AS insight
            """)
            if not ai_df.empty:
                ai_text = str(ai_df.iloc[0]["insight"])
        except Exception:
            pass

        if not ai_text:
            # Rule-based fallback
            if is_batter_adv:
                ai_text = (f"{r['batter']} dominates this matchup with a strike rate of {r['strike_rate']} "
                           f"and only {r['dismissal_pct']}% dismissal rate. "
                           f"Consider using {r['bowler']} only in non-critical overs, "
                           f"or set a defensive field to limit boundaries.")
            else:
                ai_text = (f"{r['bowler']} has the upper hand — {r['batter']} scores at only SR {r['strike_rate']} "
                           f"with a {r['dismissal_pct']}% dismissal rate. "
                           f"Deploy {r['bowler']} when {r['batter']} is new to the crease "
                           f"or in a pressure situation.")

        # Danger table rows
        danger_rows = []
        for rank, (_, d) in enumerate(danger_df.iterrows(), 1):
            is_current = d["bowler"].lower() == bowler.lower()
            danger_rows.append(html.Tr([
                html.Td(f"#{rank}", style={"color": "#94a3b8", "fontWeight": "600"}),
                html.Td([
                    html.Span(d["bowler"]),
                    html.Span(" ← current", style={"color": "#1d4ed8", "fontSize": "11px",
                                                    "marginLeft": "6px"}) if is_current else None,
                ]),
                html.Td(str(d["balls"])),
                html.Td(str(d["sr"])),
                html.Td(f"{d['dot_pct']}%"),
                html.Td(f"{d['dismissal_pct']}%",
                        style={"color": "#dc2626" if d["dismissal_pct"] > 10 else "#0f172a",
                               "fontWeight": "700" if d["dismissal_pct"] > 10 else "400"}),
            ]))

        return html.Div([
            # Header
            html.Div([
                html.Div(r["batter"], className="matchup-player",
                         style={"color": "#1d4ed8"}),
                html.Span("vs", style={"color": "#94a3b8", "fontWeight": "500",
                                       "fontSize": "14px", "margin": "0 8px"}),
                html.Div(r["bowler"], className="matchup-player",
                         style={"color": "#7c3aed"}),
                html.Span(adv_label, className="badge",
                          style={"background": adv_bg, "color": adv_color,
                                 "fontSize": "11px", "fontWeight": "700",
                                 "marginLeft": "12px"}),
            ], className="matchup-header"),

            # H2H Stats
            html.Div([
                html.Div([html.Div("BALLS FACED",   className="stat-label"),
                          html.Div(str(r["balls"]),        className="stat-value")], className="stat-tile"),
                html.Div([html.Div("RUNS SCORED",   className="stat-label"),
                          html.Div(str(r["runs"]),         className="stat-value",
                                   style={"color": "#16a34a" if is_batter_adv else "#0f172a"})], className="stat-tile"),
                html.Div([html.Div("STRIKE RATE",   className="stat-label"),
                          html.Div(str(r["strike_rate"]),  className="stat-value")], className="stat-tile"),
                html.Div([html.Div("DISMISSALS",    className="stat-label"),
                          html.Div(str(r["dismissals"]),   className="stat-value",
                                   style={"color": "#dc2626"})], className="stat-tile"),
                html.Div([html.Div("DOT BALL %",    className="stat-label"),
                          html.Div(f"{r['dot_pct']}%",     className="stat-value")], className="stat-tile"),
                html.Div([html.Div("BOUNDARY %",    className="stat-label"),
                          html.Div(f"{r['boundary_pct']}%", className="stat-value")], className="stat-tile"),
                html.Div([html.Div("DISMISSAL RATE", className="stat-label"),
                          html.Div(f"{r['dismissal_pct']}%", className="stat-value",
                                   style={"color": "#dc2626" if drate > 10 else "#0f172a"})], className="stat-tile"),
                html.Div([html.Div("MATCHES FACED", className="stat-label"),
                          html.Div(str(r["matches_faced"]), className="stat-value")], className="stat-tile"),
            ], className="stat-grid", style={"marginBottom": "20px"}),

            # AI tactical card
            html.Div([
                html.Div("🤖  AI TACTICAL BRIEF", className="verdict-header"),
                html.Div(ai_text, className="verdict-text"),
            ], className=f"verdict-box {'verdict-green' if is_batter_adv else 'verdict-red'}",
               style={"marginBottom": "20px"}),

            # Dangermen table
            html.Div("Bowlers with Best Record vs " + r["batter"], className="section-header"),
            html.Div([
                html.Table([
                    html.Thead(html.Tr([
                        html.Th("#"),
                        html.Th("BOWLER"),
                        html.Th("BALLS"),
                        html.Th("SR CONCEDED"),
                        html.Th("DOT %"),
                        html.Th("DISMISSAL %"),
                    ])),
                    html.Tbody(danger_rows),
                ], className="danger-table"),
            ], className="card") if not danger_df.empty else None,

        ])
    except Exception as e:
        return html.Div(f"Error: {e}", className="error-box")


# ─────────────────────────────────────────────────────────────────────────────
# ROOT LAYOUT + ROUTING
# ─────────────────────────────────────────────────────────────────────────────

app.layout = html.Div([
    dcc.Location(id="url", refresh=False),
    html.Div(id="nav-container"),
    html.Div(id="page-container"),
])


@app.callback(
    Output("nav-container",  "children"),
    Output("page-container", "children"),
    Input("url", "pathname"),
)
def route(pathname):
    path = pathname or "/"
    if path == "/anomaly":
        return navbar("/anomaly"), page_anomaly()
    elif path == "/matchup":
        return navbar("/matchup"), page_matchup()
    else:
        return navbar("/"), page_scout()


# ── Health check ──────────────────────────────────────────────────────────────

import flask

@server.route("/health")
def health():
    tok = _get_token()
    try:
        run_q("SELECT 1 AS ok")
        db = "connected"
    except Exception as e:
        db = str(e)
    return flask.jsonify({
        "token_present": bool(tok),
        "warehouse": WAREHOUSE,
        "host": HOST,
        "db_ping": db,
    })


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
