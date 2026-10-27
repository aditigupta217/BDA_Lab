"""Smart Traffic Analysis - Streamlit dashboard.

Reads the aggregated CSV outputs produced by src/pipeline.py (run the
pipeline first: `python -m src.pipeline`) plus a sample of the raw dataset
for the Data Explorer tab. Does not run Spark itself -- it is a pure
presentation layer over the pipeline's results.
"""
import json

import pandas as pd
import plotly.express as px
import streamlit as st

from src import config

st.set_page_config(
    page_title="Smart Traffic Analysis",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------- styling --
# A navy/blue "professional dashboard" theme. Data-series colors (CATEGORICAL,
# SEQUENTIAL_BLUE) stay the validated dataviz-skill palette below -- only the
# chrome (backgrounds, cards, sidebar, nav) uses the richer blue treatment.
PAGE_PLANE = "#0a0f1e"
SURFACE = "#111a2e"
SURFACE_RAISED = "#16233d"
SIDEBAR_TOP = "#0e1730"
SIDEBAR_BOTTOM = "#0a0f1e"
BORDER = "rgba(79,141,253,0.16)"
INK_PRIMARY = "#f3f6fc"
INK_SECONDARY = "#aab6cc"
INK_MUTED = "#7788a3"
GRIDLINE = "#1f2c47"
AXIS_LINE = "#2a3a5c"

BLUE_BRIGHT = "#4f8dfd"
BLUE_DEEP = "#1e4fd6"

# Fixed-order categorical hues (dark steps) -- never cycled, assigned by entity
CATEGORICAL = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"]
SEQUENTIAL_BLUE = ["#0d366b", "#104281", "#184f95", "#1c5cab", "#256abf", "#2a78d6", "#3987e5", "#5598e7", "#6da7ec", "#86b6ef", "#9ec5f4", "#b7d3f6", "#cde2fb"]
STATUS_GOOD = "#22c55e"
STATUS_CRITICAL = "#f0576b"
STATUS_WARNING = "#fab219"
ACCENT = BLUE_BRIGHT
KPI_ACCENTS = [BLUE_BRIGHT, CATEGORICAL[2], CATEGORICAL[3], CATEGORICAL[6]]

st.markdown(
    f"""
    <style>
    .stApp {{
        background: radial-gradient(circle at 15% 0%, #0f1b38 0%, {PAGE_PLANE} 45%);
    }}
    [data-testid="stHeader"] {{ background: transparent; }}

    /* ---------------- Sidebar ---------------- */
    section[data-testid="stSidebar"] {{
        background: linear-gradient(180deg, {SIDEBAR_TOP} 0%, {SIDEBAR_BOTTOM} 100%);
        border-right: 1px solid {BORDER};
    }}
    section[data-testid="stSidebar"] .block-container {{ padding-top: 1.4rem; }}

    .sidebar-brand {{
        display: flex; align-items: center; gap: 12px;
        padding: 2px 4px 16px 4px; margin-bottom: 10px;
        border-bottom: 1px solid {BORDER};
    }}
    .sidebar-brand .badge {{
        width: 38px; height: 38px; border-radius: 10px; flex-shrink: 0;
        background: linear-gradient(135deg, {BLUE_BRIGHT}, {BLUE_DEEP});
        box-shadow: 0 0 18px rgba(79,141,253,0.45);
        display: flex; align-items: center; justify-content: center;
        font-size: 1.15rem;
    }}
    .sidebar-brand .name {{ font-weight: 700; font-size: 1.0rem; line-height: 1.25; color: {INK_PRIMARY}; }}
    .sidebar-brand .tag {{
        color: {INK_MUTED}; font-size: 0.68rem; text-transform: uppercase;
        letter-spacing: 0.08em; margin-top: 1px;
    }}

    /* Radio-as-nav-list styling */
    section[data-testid="stSidebar"] div[role="radiogroup"] {{ gap: 2px; }}
    section[data-testid="stSidebar"] div[role="radiogroup"] label {{
        display: flex; align-items: center; width: 100%;
        padding: 9px 12px; border-radius: 9px; margin-bottom: 2px;
        border-left: 3px solid transparent;
        transition: background 0.15s ease, border-color 0.15s ease;
        cursor: pointer;
    }}
    section[data-testid="stSidebar"] div[role="radiogroup"] label:hover {{
        background: rgba(79,141,253,0.08);
    }}
    section[data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked) {{
        background: linear-gradient(90deg, rgba(79,141,253,0.22), rgba(79,141,253,0.03));
        border-left: 3px solid {BLUE_BRIGHT};
    }}
    section[data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked) p {{
        color: {INK_PRIMARY} !important; font-weight: 600;
    }}
    section[data-testid="stSidebar"] div[role="radiogroup"] label p {{
        color: {INK_SECONDARY}; font-size: 0.9rem; margin: 0;
    }}
    section[data-testid="stSidebar"] div[role="radiogroup"] label > div:first-child {{
        display: none;
    }}
    section[data-testid="stSidebar"] hr {{ border-color: {BORDER}; margin: 14px 0; }}
    .sidebar-footer {{ color: {INK_MUTED}; font-size: 0.68rem; line-height: 1.5; padding-top: 10px; }}

    /* ---------------- Header ---------------- */
    .app-header {{
        display: flex; align-items: center; gap: 14px;
        padding-bottom: 14px; margin-bottom: 20px;
        border-bottom: 1px solid {BORDER};
    }}
    .app-header .badge {{
        width: 44px; height: 44px; border-radius: 12px;
        background: linear-gradient(135deg, {BLUE_BRIGHT}, {CATEGORICAL[6]});
        box-shadow: 0 0 24px rgba(79,141,253,0.35);
        display: flex; align-items: center; justify-content: center;
        font-size: 1.4rem; flex-shrink: 0;
    }}
    .app-header h1 {{ margin: 0; font-size: 1.55rem; font-weight: 700; color: {INK_PRIMARY}; }}
    .app-header .subtitle {{ margin: 0; color: {INK_MUTED}; font-size: 0.85rem; }}

    .section-eyebrow {{
        color: {BLUE_BRIGHT}; font-size: 0.74rem; font-weight: 700;
        text-transform: uppercase; letter-spacing: 0.09em; margin: 28px 0 6px 0;
    }}

    /* ---------------- KPI cards ---------------- */
    .kpi-card {{
        position: relative; overflow: hidden;
        background: linear-gradient(160deg, {SURFACE_RAISED} 0%, {SURFACE} 100%);
        border: 1px solid {BORDER};
        border-radius: 12px;
        padding: 16px 18px 15px 18px;
        margin-bottom: 10px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.25);
        transition: transform 0.15s ease, border-color 0.15s ease;
    }}
    .kpi-card::before {{
        content: ""; position: absolute; top: 0; left: 0; right: 0; height: 3px;
        background: var(--kpi-accent, {BLUE_BRIGHT});
    }}
    .kpi-card:hover {{ transform: translateY(-1px); border-color: rgba(79,141,253,0.35); }}
    .kpi-label {{
        color: {INK_MUTED}; font-size: 0.71rem; text-transform: uppercase;
        letter-spacing: 0.06em; margin-bottom: 6px; font-weight: 600;
    }}
    .kpi-value {{
        color: {INK_PRIMARY}; font-size: 1.7rem; font-weight: 700;
        font-variant-numeric: tabular-nums; line-height: 1.15;
    }}
    .kpi-sub {{ color: {INK_SECONDARY}; font-size: 0.74rem; margin-top: 4px; }}

    .status-pill {{
        display: inline-flex; align-items: center; gap: 6px;
        padding: 4px 12px; border-radius: 999px; font-size: 0.82rem; font-weight: 600;
    }}
    .status-pill.ok {{ background: rgba(34,197,94,0.15); color: {STATUS_GOOD}; }}
    .status-pill.fail {{ background: rgba(240,87,107,0.15); color: {STATUS_CRITICAL}; }}

    h1, h2, h3 {{ color: {INK_PRIMARY}; }}
    h2 {{ font-size: 1.15rem !important; font-weight: 700 !important; margin-top: 4px !important; }}
    p, .stMarkdown, label, .stCaption {{ color: {INK_SECONDARY}; }}
    .proxy-note {{
        color: {INK_MUTED}; font-size: 0.8rem; font-style: italic;
        border-left: 2px solid {BLUE_BRIGHT}; padding-left: 10px; margin: 10px 0 18px 0;
    }}
    [data-testid="stDataFrame"] {{ border: 1px solid {BORDER}; border-radius: 8px; }}
    hr {{ border-color: {BORDER}; }}

    /* Buttons / inputs pick up the blue accent */
    .stButton button, .stDownloadButton button {{
        background: linear-gradient(135deg, {BLUE_BRIGHT}, {BLUE_DEEP});
        border: none; color: white; font-weight: 600;
    }}
    .stSlider [data-baseweb="slider"] div[role="slider"] {{ background-color: {BLUE_BRIGHT}; }}
    </style>
    """,
    unsafe_allow_html=True,
)

PLOTLY_TEMPLATE = "plotly_dark"
px.defaults.template = PLOTLY_TEMPLATE
px.defaults.color_discrete_sequence = CATEGORICAL

CHART_LAYOUT = dict(
    template=PLOTLY_TEMPLATE,
    paper_bgcolor=SURFACE,
    plot_bgcolor=SURFACE,
    font=dict(color=INK_SECONDARY, family="system-ui, -apple-system, 'Segoe UI', sans-serif", size=13),
    title_font=dict(color=INK_PRIMARY, size=14),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
    margin=dict(t=40, l=10, r=10, b=10),
)


def style_chart(fig, height=380):
    fig.update_layout(**CHART_LAYOUT, height=height)
    fig.update_xaxes(gridcolor=GRIDLINE, linecolor=AXIS_LINE, zerolinecolor=AXIS_LINE)
    fig.update_yaxes(gridcolor=GRIDLINE, linecolor=AXIS_LINE, zerolinecolor=AXIS_LINE)
    return fig


def section_header(title):
    st.markdown(f'<div class="section-eyebrow">{title}</div>', unsafe_allow_html=True)


def kpi_card(label, value, sub="", accent=None):
    style = f'style="--kpi-accent:{accent}"' if accent else ""
    st.markdown(
        f"""<div class="kpi-card" {style}><div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-sub">{sub}</div></div>""",
        unsafe_allow_html=True,
    )


@st.cache_data
def load_outputs():
    out = {}
    names = [
        "agg_by_hour", "agg_by_date", "agg_by_weekday", "agg_by_month",
        "agg_by_zone", "agg_by_intersection", "agg_by_weather",
        "agg_by_road_type", "vehicle_mix_totals", "vehicle_mix_by_hour",
        "hotspot_ranking",
    ]
    for name in names:
        path = config.OUTPUTS_DIR / f"{name}.csv"
        out[name] = pd.read_csv(path) if path.exists() else None
    return out


@st.cache_data
def load_raw_sample(n=50000):
    df = pd.read_csv(config.TRAFFIC_CSV_PATH)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    if len(df) > n:
        df = df.sample(n, random_state=42).sort_values("timestamp")
    return df


def load_status():
    if config.PIPELINE_STATUS_PATH.exists():
        with open(config.PIPELINE_STATUS_PATH) as f:
            return json.load(f)
    return None


outputs = load_outputs()
status = load_status()
missing_outputs = [k for k, v in outputs.items() if v is None]

st.sidebar.markdown(
    """<div class="sidebar-brand"><div class="badge">🚦</div>
    <div><div class="name">Smart Traffic<br/>Analysis</div>
    <div class="tag">Big Data Dashboard</div></div></div>""",
    unsafe_allow_html=True,
)
section = st.sidebar.radio(
    "Navigate",
    ["📊  Overview", "⏱️  Traffic Patterns", "📍  Location Analysis", "🚗  Vehicle Analysis",
     "🔥  Traffic Hotspots", "🔎  Data Explorer", "⚙️  Pipeline Status"],
    label_visibility="collapsed",
)
section = section.split("  ", 1)[1]

if missing_outputs:
    st.sidebar.warning(
        "Some pipeline outputs are missing. Run:\n\n`python -m src.pipeline`"
    )

st.sidebar.markdown("<hr/>", unsafe_allow_html=True)
st.sidebar.markdown(
    f"""<div class="sidebar-footer">
    Hadoop HDFS 3.4.3 &middot; PySpark 3.5.3<br/>
    Mode: <strong style="color:{BLUE_BRIGHT};">{(status or {}).get('mode', 'local').upper()}</strong>
    </div>""",
    unsafe_allow_html=True,
)

st.markdown(
    f"""<div class="app-header"><div class="badge">🚦</div>
    <div><h1>{section}</h1><p class="subtitle">Smart Traffic Analysis &middot; Big Data Processing &amp; Traffic Pattern Visualization</p></div>
    </div>""",
    unsafe_allow_html=True,
)

# -------------------------------------------------------------- Overview --
if section == "Overview":
    raw = load_raw_sample()
    by_hour = outputs["agg_by_hour"]
    by_date = outputs["agg_by_date"]

    total_records = status["row_count"] if status else len(raw)
    total_vehicles = int(by_hour["total_vehicles"].sum()) if by_hour is not None else int(raw["vehicle_count"].sum())
    avg_volume = round(by_hour["avg_vehicles"].mean(), 1) if by_hour is not None else round(raw["vehicle_count"].mean(), 1)
    peak_hour = int(by_hour.loc[by_hour["avg_vehicles"].idxmax(), "hour"]) if by_hour is not None else None
    avg_speed = round(raw["average_speed"].mean(), 1)
    avg_congestion = round(raw["congestion_score"].mean(), 1)
    date_min, date_max = raw["timestamp"].min().date(), raw["timestamp"].max().date()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        kpi_card("Total Records", f"{total_records:,}", "rows processed by the pipeline", accent=KPI_ACCENTS[0])
    with c2:
        kpi_card("Total Recorded Vehicles", f"{total_vehicles:,}", accent=KPI_ACCENTS[1])
    with c3:
        kpi_card("Avg Traffic Volume / hr-window", f"{avg_volume:,}", accent=KPI_ACCENTS[2])
    with c4:
        kpi_card("Peak Average-Volume Hour", f"{peak_hour}:00" if peak_hour is not None else "—", accent=KPI_ACCENTS[3])

    c5, c6, c7, c8 = st.columns(4)
    with c5:
        kpi_card("Average Speed", f"{avg_speed} km/h", accent=KPI_ACCENTS[0])
    with c6:
        kpi_card("Average Congestion Score", f"{avg_congestion}", "proxy indicator, not ground truth", accent=KPI_ACCENTS[1])
    with c7:
        kpi_card("Dataset Date Range", f"{(date_max - date_min).days + 1} days", accent=KPI_ACCENTS[2])
    with c8:
        kpi_card("Date Range", f"{date_min} → {date_max}", accent=KPI_ACCENTS[3])

    section_header("Traffic Volume Trend")
    if by_date is not None:
        fig = px.line(by_date, x="event_date", y="total_vehicles",
                       labels={"event_date": "Date", "total_vehicles": "Total Vehicles"})
        fig.update_traces(line_color=ACCENT, line_width=2)
        st.plotly_chart(style_chart(fig), use_container_width=True)
    else:
        st.info("Run the pipeline to populate the daily trend.")

# --------------------------------------------------------- Traffic Patterns
elif section == "Traffic Patterns":
    by_hour = outputs["agg_by_hour"]
    by_weekday = outputs["agg_by_weekday"]
    by_month = outputs["agg_by_month"]
    by_weather = outputs["agg_by_weather"]

    if by_hour is not None:
        section_header("Average & Total Traffic by Hour")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(by_hour, x="hour", y="avg_vehicles",
                         labels={"hour": "Hour of Day", "avg_vehicles": "Avg Vehicles"})
            fig.update_traces(marker_color=ACCENT)
            st.plotly_chart(style_chart(fig, height=340), use_container_width=True)
        with col2:
            fig = px.line(by_hour, x="hour", y="total_vehicles",
                          labels={"hour": "Hour of Day", "total_vehicles": "Total Vehicles"})
            fig.update_traces(line_color=CATEGORICAL[1], line_width=2)
            st.plotly_chart(style_chart(fig, height=340), use_container_width=True)

    if by_weekday is not None:
        section_header("Weekday vs Weekend")
        wk = by_weekday.copy()
        wk["type"] = wk["is_weekend"].map({0: "Weekday", 1: "Weekend"})
        fig = px.bar(wk.sort_values("avg_vehicles", ascending=False),
                     x="weekday_name", y="avg_vehicles", color="type",
                     color_discrete_sequence=[CATEGORICAL[0], CATEGORICAL[1]],
                     labels={"weekday_name": "Day", "avg_vehicles": "Avg Vehicles", "type": ""})
        st.plotly_chart(style_chart(fig), use_container_width=True)

    if by_month is not None and by_month["month"].nunique() > 1:
        section_header("Monthly Trend")
        fig = px.bar(by_month, x="month", y="total_vehicles")
        fig.update_traces(marker_color=ACCENT)
        st.plotly_chart(style_chart(fig), use_container_width=True)

    if by_weather is not None:
        section_header("Weather vs Traffic")
        st.markdown(
            '<p class="proxy-note">avg_congestion_score and avg_speed are dataset-supplied proxy indicators.</p>',
            unsafe_allow_html=True,
        )
        # Two measures on different scales -> two charts side by side, not a dual-axis plot.
        col1, col2 = st.columns(2)
        wby = by_weather.sort_values("avg_congestion_score", ascending=False)
        with col1:
            fig = px.bar(wby, x="weather_condition", y="avg_vehicles",
                         labels={"weather_condition": "Weather", "avg_vehicles": "Avg Vehicles"})
            fig.update_traces(marker_color=ACCENT)
            st.plotly_chart(style_chart(fig, height=340), use_container_width=True)
        with col2:
            fig = px.bar(wby, x="weather_condition", y="avg_congestion_score",
                         labels={"weather_condition": "Weather", "avg_congestion_score": "Avg Congestion Score"})
            fig.update_traces(marker_color=CATEGORICAL[7])
            st.plotly_chart(style_chart(fig, height=340), use_container_width=True)

# -------------------------------------------------------- Location Analysis
elif section == "Location Analysis":
    by_zone = outputs["agg_by_zone"]
    by_intersection = outputs["agg_by_intersection"]

    if by_zone is not None:
        section_header("Traffic by Zone")
        fig = px.bar(by_zone.sort_values("total_vehicles", ascending=True),
                     x="total_vehicles", y="city_zone", orientation="h",
                     labels={"total_vehicles": "Total Vehicles", "city_zone": "Zone"})
        fig.update_traces(marker_color=ACCENT)
        st.plotly_chart(style_chart(fig), use_container_width=True)

    if by_intersection is not None:
        section_header("Busiest Intersections")
        top_n = st.slider("Show top N intersections", 5, 50, 15)
        top = by_intersection.sort_values("total_vehicles", ascending=False).head(top_n)
        fig = px.bar(top.sort_values("total_vehicles"), x="total_vehicles",
                     y="intersection_id", color="city_zone", orientation="h",
                     color_discrete_sequence=CATEGORICAL)
        st.plotly_chart(style_chart(fig, height=max(340, 24 * top_n)), use_container_width=True)

        section_header("Geographic Distribution (coordinates as supplied in dataset)")
        # scatter_mapbox (not the newer scatter_map/MapLibre trace) -- Streamlit's
        # bundled plotly.js doesn't yet render the newer "scattermap" trace type,
        # which showed as a blank cartesian grid instead of a map.
        fig = px.scatter_mapbox(
            by_intersection, lat="latitude", lon="longitude",
            size="total_vehicles", color="avg_congestion_score",
            hover_name="intersection_id", hover_data=["city_zone", "road_id"],
            zoom=10, height=500,
            color_continuous_scale=SEQUENTIAL_BLUE[::-1],
        )
        # "open-street-map" is the one mapbox_style that needs no API key/token.
        fig.update_layout(mapbox_style="open-street-map", template=PLOTLY_TEMPLATE,
                           paper_bgcolor=SURFACE, margin=dict(t=10, l=0, r=0, b=0))
        st.plotly_chart(fig, use_container_width=True)

# --------------------------------------------------------- Vehicle Analysis
elif section == "Vehicle Analysis":
    mix = outputs["vehicle_mix_totals"]
    mix_by_hour = outputs["vehicle_mix_by_hour"]

    if mix is not None:
        section_header("Vehicle Category Distribution")
        row = mix.iloc[0]
        other = max(row["total_vehicles"] - row["total_heavy"] - row["total_motorcycle"] - row["total_public_transport"], 0)
        pie_df = pd.DataFrame({
            "category": ["Heavy Vehicles", "Motorcycles", "Public Transport", "Other / Cars"],
            "count": [row["total_heavy"], row["total_motorcycle"], row["total_public_transport"], other],
        })
        fig = px.pie(pie_df, names="category", values="count", hole=0.5,
                     color_discrete_sequence=CATEGORICAL)
        fig.update_traces(marker=dict(line=dict(color=SURFACE, width=2)))
        st.plotly_chart(style_chart(fig), use_container_width=True)

    if mix_by_hour is not None:
        section_header("Vehicle Categories by Hour")
        melted = mix_by_hour.melt(id_vars="hour", value_vars=["heavy", "motorcycle", "public_transport"],
                                   var_name="category", value_name="count")
        fig = px.area(melted, x="hour", y="count", color="category",
                      color_discrete_sequence=CATEGORICAL,
                      labels={"hour": "Hour of Day", "count": "Vehicles", "category": ""})
        st.plotly_chart(style_chart(fig), use_container_width=True)

# -------------------------------------------------------- Traffic Hotspots
elif section == "Traffic Hotspots":
    st.markdown(
        """<p class="proxy-note"><strong>Ranking method (transparent, proxy-based):</strong>
        each zone's <code>hotspot_score</code> is a weighted blend of three
        normalized (0&ndash;1) indicators: 0.4&nbsp;&times;&nbsp;congestion_score +
        0.3&nbsp;&times;&nbsp;queue_length + 0.3&nbsp;&times;&nbsp;(1 &minus; normalized
        average_speed). Higher score = more likely hotspot. These are
        dataset-supplied proxy indicators, not verified ground-truth congestion
        measurements &mdash; treat rankings as a starting point for
        investigation, not a final verdict.</p>""",
        unsafe_allow_html=True,
    )
    hotspots = outputs["hotspot_ranking"]
    if hotspots is not None:
        section_header("Zone Ranking")
        fig = px.bar(hotspots.sort_values("hotspot_score"), x="hotspot_score", y="city_zone",
                     orientation="h", color="hotspot_score",
                     color_continuous_scale=SEQUENTIAL_BLUE[::-1][6:])
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(style_chart(fig), use_container_width=True)
        st.dataframe(
            hotspots[["city_zone", "hotspot_score", "total_vehicles", "avg_speed",
                      "avg_congestion_score", "avg_queue_length"]].round(2),
            use_container_width=True,
        )

    by_intersection = outputs["agg_by_intersection"]
    if by_intersection is not None:
        section_header("Top Flagged Intersections (by congestion_score)")
        flagged = by_intersection.sort_values("avg_congestion_score", ascending=False).head(10)
        st.dataframe(flagged.round(2), use_container_width=True)

# -------------------------------------------------------------- Data Explorer
elif section == "Data Explorer":
    raw = load_raw_sample()

    col1, col2, col3 = st.columns(3)
    with col1:
        date_range = st.date_input(
            "Date range",
            value=(raw["timestamp"].min().date(), raw["timestamp"].max().date()),
        )
    with col2:
        zones = st.multiselect("City zone", sorted(raw["city_zone"].unique()))
    with col3:
        hours = st.slider("Hour range", 0, 23, (0, 23))

    filtered = raw.copy()
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start, end = date_range
        filtered = filtered[(filtered["timestamp"].dt.date >= start) & (filtered["timestamp"].dt.date <= end)]
    if zones:
        filtered = filtered[filtered["city_zone"].isin(zones)]
    filtered = filtered[(filtered["hour"] >= hours[0]) & (filtered["hour"] <= hours[1])]

    st.caption(f"Showing {len(filtered):,} of {len(raw):,} sampled rows "
               f"({config.TRAFFIC_CSV_PATH.name} sample for interactive exploration).")

    if filtered.empty:
        st.warning("No data matches the selected filters. Try widening your date, zone, or hour range.")
    else:
        st.dataframe(filtered.head(2000), use_container_width=True)
        st.download_button(
            "Download filtered CSV",
            filtered.to_csv(index=False).encode("utf-8"),
            file_name="traffic_filtered.csv",
            mime="text/csv",
        )

# -------------------------------------------------------------- Pipeline Status
elif section == "Pipeline Status":
    if status is None:
        st.error("No pipeline run found. Execute `python -m src.pipeline` first.")
    else:
        ok = status["success"]
        pill_class = "ok" if ok else "fail"
        pill_label = "Success" if ok else "Failed"
        pill_icon = "✓" if ok else "✕"
        finished = status.get("finished_at", "")
        finished_display = finished.replace("T", "  ·  ").split(".")[0] + " UTC" if finished else "—"

        st.markdown(
            f"""<div class="kpi-card" style="margin-bottom:20px;">
            <span class="status-pill {pill_class}">{pill_icon} &nbsp;{pill_label}</span>
            <span style="color:{INK_MUTED};font-size:0.82rem;margin-left:14px;">Last run: {finished_display}</span>
            </div>""",
            unsafe_allow_html=True,
        )

        c1, c2, c3 = st.columns(3)
        with c1:
            kpi_card("Execution Mode", status["mode"].upper(), "local = no HDFS required · hdfs = read via Hadoop", accent=KPI_ACCENTS[0])
        with c2:
            kpi_card("Rows Processed", f"{status.get('row_count', 0):,}" if status.get("row_count") else "—", accent=KPI_ACCENTS[1])
        with c3:
            kpi_card("Aggregated Tables", f"{len(status.get('outputs', []))}", "written to outputs/", accent=KPI_ACCENTS[2])

        if status.get("errors"):
            section_header("Errors")
            for e in status["errors"]:
                st.error(e)
