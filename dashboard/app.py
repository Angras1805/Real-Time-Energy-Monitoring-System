import sys
import time
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_DATABASE,
    DASHBOARD_REFRESH_INTERVAL_MS,
    MODEL_PATH,
)
from database.db_connection import db_manager

# Set up page configuration
st.set_page_config(
    page_title="Smart Energy Monitoring & Anomaly Detection",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling for Industry-Grade Modern Appearance
CUSTOM_CSS = """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=Inter:wght@400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Outfit', 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    .main .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        max-width: 98%;
    }

    /* Header styling */
    .dashboard-header {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.7) 0%, rgba(15, 23, 42, 0.9) 100%);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px;
        padding: 1.25rem 1.75rem;
        margin-bottom: 1.5rem;
        box-shadow: 0 10px 25px -5px rgba(0, 0, 0, 0.3), 0 8px 10px -6px rgba(0, 0, 0, 0.2);
        display: flex;
        justify-content: space-between;
        align-items: center;
    }

    .dashboard-title {
        font-size: 1.75rem;
        font-weight: 700;
        background: linear-gradient(90deg, #38bdf8 0%, #818cf8 50%, #c084fc 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
        letter-spacing: -0.02em;
    }

    .dashboard-subtitle {
        color: #94a3b8;
        font-size: 0.9rem;
        margin-top: 0.25rem;
        font-weight: 400;
    }

    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(145deg, #1e293b 0%, #0f172a 100%);
        border: 1px solid rgba(255, 255, 255, 0.07);
        border-radius: 14px;
        padding: 1.1rem 1.25rem;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.25);
        transition: transform 0.2s ease, border-color 0.2s ease;
        position: relative;
        overflow: hidden;
    }

    .kpi-card:hover {
        transform: translateY(-2px);
        border-color: rgba(56, 189, 248, 0.3);
    }

    .kpi-card::before {
        content: "";
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, #38bdf8, #818cf8);
    }

    .kpi-card.anomaly-card::before {
        background: linear-gradient(90deg, #f43f5e, #fb7185);
    }

    .kpi-card.warning-card::before {
        background: linear-gradient(90deg, #f59e0b, #fbbf24);
    }

    .kpi-card.success-card::before {
        background: linear-gradient(90deg, #10b981, #34d399);
    }

    .kpi-label {
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.06em;
        color: #94a3b8;
        font-weight: 600;
        margin-bottom: 0.35rem;
        display: flex;
        align-items: center;
        gap: 0.4rem;
    }

    .kpi-value {
        font-size: 1.85rem;
        font-weight: 700;
        color: #f8fafc;
        line-height: 1.1;
        font-feature-settings: "tnum";
    }

    .kpi-unit {
        font-size: 0.9rem;
        font-weight: 500;
        color: #64748b;
        margin-left: 0.25rem;
    }

    .kpi-subtext {
        font-size: 0.75rem;
        color: #64748b;
        margin-top: 0.4rem;
    }

    /* Status Pill */
    .status-badge {
        display: inline-flex;
        align-items: center;
        gap: 0.35rem;
        padding: 0.25rem 0.65rem;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
    }

    .status-live {
        background: rgba(16, 185, 129, 0.15);
        color: #34d399;
        border: 1px solid rgba(52, 211, 153, 0.3);
    }

    .status-alert {
        background: rgba(244, 63, 94, 0.15);
        color: #fb7185;
        border: 1px solid rgba(251, 113, 133, 0.3);
    }

    .live-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background-color: #10b981;
        box-shadow: 0 0 8px #10b981;
        animation: pulse 1.8s infinite;
    }

    @keyframes pulse {
        0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
        70% { transform: scale(1.05); box-shadow: 0 0 0 6px rgba(16, 185, 129, 0); }
        100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }

    /* Container panels */
    .content-panel {
        background: #0f172a;
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 14px;
        padding: 1.25rem;
        margin-bottom: 1.25rem;
    }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


def check_kafka_status(bootstrap_servers: str) -> bool:
    """Verifies socket connectivity to Kafka broker."""
    import socket
    try:
        host, port = bootstrap_servers.split(":")
        with socket.create_connection((host, int(port)), timeout=1.0):
            return True
    except Exception:
        return False


def load_dashboard_data():
    """Fetches real-time readings and aggregated system metrics directly from MySQL."""
    db_connected = db_manager.check_connection()
    metrics = None
    readings_df = pd.DataFrame()
    anomalies_df = pd.DataFrame()

    if db_connected:
        metrics = db_manager.get_latest_metrics()
        readings = db_manager.get_recent_readings(limit=180)
        anomalies = db_manager.get_anomaly_records(limit=25)

        if readings:
            readings_df = pd.DataFrame(readings)
            readings_df["reading_time"] = pd.to_datetime(readings_df["reading_time"])
            readings_df["global_active_power"] = readings_df["global_active_power"].astype(float)
            readings_df["rolling_avg_30"] = readings_df["rolling_avg_30"].astype(float)
            readings_df["is_anomaly"] = readings_df["is_anomaly"].astype(int)

        if anomalies:
            anomalies_df = pd.DataFrame(anomalies)

    return db_connected, metrics, readings_df, anomalies_df


# ==========================================
# SIDEBAR NAVIGATION & CONTROLS
# ==========================================
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/flash-on.png", width=64)
    st.title("Smart Energy")
    st.caption("Data Stream Analytics • Mini-Project")

    st.markdown("---")
    st.subheader("⚙️ System Pipeline Health")

    # Real-time health checks
    kafka_online = check_kafka_status(KAFKA_BOOTSTRAP_SERVERS)
    db_online = db_manager.check_connection()
    model_online = MODEL_PATH.exists()

    st.markdown(
        f"""
        **Kafka Broker** (`{KAFKA_BOOTSTRAP_SERVERS}`):  
        {'🟢 <span class="status-badge status-live">Online</span>' if kafka_online else '🔴 <span class="status-badge status-alert">Offline</span>'}  
        <br>
        **MySQL Database** (`{MYSQL_DATABASE}`):  
        {'🟢 <span class="status-badge status-live">Connected</span>' if db_online else '🔴 <span class="status-badge status-alert">Disconnected</span>'}  
        <br>
        **ML Isolation Forest**:  
        {'🟢 <span class="status-badge status-live">Model Ready</span>' if model_online else '🟡 <span class="status-badge status-alert">Uninitialized</span>'}  
        """,
        unsafe_allow_html=True,
    )

    st.markdown("---")
    st.subheader("🎛️ Stream Configuration")
    auto_refresh_enabled = st.checkbox("Auto Refresh Stream (2s)", value=True)
    refresh_rate = st.slider("Polling Frequency (sec)", min_value=1, max_value=10, value=2)
    view_window = st.select_slider("Chart History (points)", options=[30, 60, 100, 150, 200], value=100)

    st.markdown("---")
    st.subheader("📋 Dataset & Schema")
    st.markdown(f"**Topic**: `{KAFKA_TOPIC}`")
    st.markdown("**Window**: `30-Reading Sliding Mean`")
    st.markdown("**Algorithm**: `Isolation Forest (IForest)`")

    if st.button("🔄 Manual Data Refresh", use_container_width=True):
        st.rerun()


# ==========================================
# FETCH DATA
# ==========================================
db_connected, metrics, readings_df, anomalies_df = load_dashboard_data()

# Header Bar
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.markdown(
        """
        <div class="dashboard-header">
            <div>
                <h1 class="dashboard-title">Smart Energy Monitoring & Anomaly Detection</h1>
                <div class="dashboard-subtitle">
                    Real-time electricity stream processing with Apache Kafka, Scikit-learn Isolation Forest, and MySQL
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with col_h2:
    current_time_str = datetime.now().strftime("%H:%M:%S")
    st.markdown(
        f"""
        <div style="background: #1e293b; padding: 1rem; border-radius: 14px; border: 1px solid rgba(255,255,255,0.06); text-align: center;">
            <div style="display: flex; align-items: center; justify-content: center; gap: 0.5rem; margin-bottom: 0.25rem;">
                <div class="live-dot"></div>
                <span style="font-weight: 700; color: #f8fafc; font-size: 0.9rem;">STREAM ACTIVE</span>
            </div>
            <div style="font-size: 0.8rem; color: #94a3b8;">Updated at {current_time_str}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# ==========================================
# SECTION A: KPI METRICS CARDS
# ==========================================
curr_p = float(metrics.get("current_power", 0.0) or 0.0) if metrics else 0.0
roll_p = float(metrics.get("rolling_average", 0.0) or 0.0) if metrics else 0.0
peak_p = float(metrics.get("peak_power", 0.0) or 0.0) if metrics else 0.0
tot_energy = float(metrics.get("total_energy", 0.0) or 0.0) if metrics else 0.0
tot_readings = int(metrics.get("total_readings", 0) or 0) if metrics else 0
tot_anomalies = int(metrics.get("total_anomalies", 0) or 0) if metrics else 0
anomaly_pct = (tot_anomalies / tot_readings * 100) if tot_readings > 0 else 0.0

kpi_cols = st.columns(6)

with kpi_cols[0]:
    st.markdown(
        f"""
        <div class="kpi-card success-card">
            <div class="kpi-label">⚡ Current Power</div>
            <div class="kpi-value">{curr_p:.2f}<span class="kpi-unit">kW</span></div>
            <div class="kpi-subtext">Latest sensor reading</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with kpi_cols[1]:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">📈 Rolling Average</div>
            <div class="kpi-value">{roll_p:.2f}<span class="kpi-unit">kW</span></div>
            <div class="kpi-subtext">Sliding window (30 events)</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with kpi_cols[2]:
    st.markdown(
        f"""
        <div class="kpi-card warning-card">
            <div class="kpi-label">🔥 Peak Power</div>
            <div class="kpi-value">{peak_p:.2f}<span class="kpi-unit">kW</span></div>
            <div class="kpi-subtext">Max surge recorded</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with kpi_cols[3]:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">🔋 Cumulative Energy</div>
            <div class="kpi-value">{tot_energy:.2f}<span class="kpi-unit">kWh</span></div>
            <div class="kpi-subtext">Total energy integrated</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with kpi_cols[4]:
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">📊 Total Readings</div>
            <div class="kpi-value">{tot_readings:,}<span class="kpi-unit">events</span></div>
            <div class="kpi-subtext">Kafka stream events</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

with kpi_cols[5]:
    st.markdown(
        f"""
        <div class="kpi-card anomaly-card">
            <div class="kpi-label">🚨 Anomalies Detected</div>
            <div class="kpi-value" style="color: #fb7185;">{tot_anomalies:,}<span class="kpi-unit">({anomaly_pct:.1f}%)</span></div>
            <div class="kpi-subtext">Isolation Forest flags</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("<br>", unsafe_allow_html=True)


# ==========================================
# SECTION B: LIVE CONSUMPTION CHART
# ==========================================
if not readings_df.empty:
    chart_df = readings_df.tail(view_window).copy()

    fig_main = go.Figure()

    # 1. Rolling Average Trendline
    fig_main.add_trace(
        go.Scatter(
            x=chart_df["reading_time"],
            y=chart_df["rolling_avg_30"],
            mode="lines",
            name="Rolling Average (30)",
            line=dict(color="#38bdf8", width=2.5, dash="dash"),
            hovertemplate="<b>Rolling Avg:</b> %{y:.3f} kW<br><b>Time:</b> %{x|%Y-%m-%d %H:%M:%S}<extra></extra>",
        )
    )

    # 2. Actual Power Line
    fig_main.add_trace(
        go.Scatter(
            x=chart_df["reading_time"],
            y=chart_df["global_active_power"],
            mode="lines+markers",
            name="Actual Power Consumption",
            line=dict(color="#10b981", width=2),
            marker=dict(size=4, color="#10b981"),
            hovertemplate="<b>Actual Power:</b> %{y:.3f} kW<br><b>Time:</b> %{x|%Y-%m-%d %H:%M:%S}<extra></extra>",
        )
    )

    # 3. Anomalous Readings Highlight (Red Markers)
    anomaly_points = chart_df[chart_df["is_anomaly"] == 1]
    if not anomaly_points.empty:
        fig_main.add_trace(
            go.Scatter(
                x=anomaly_points["reading_time"],
                y=anomaly_points["global_active_power"],
                mode="markers",
                name="🚨 Detected Anomaly",
                marker=dict(
                    color="#ef4444",
                    size=12,
                    symbol="circle",
                    line=dict(color="#ffffff", width=2),
                ),
                hovertemplate="<b style='color:#ef4444;'>🚨 ANOMALY DETECTED</b><br><b>Power:</b> %{y:.3f} kW<br><b>Time:</b> %{x|%Y-%m-%d %H:%M:%S}<extra></extra>",
            )
        )

    fig_main.update_layout(
        title=dict(
            text="<b>Real-Time Electricity Consumption & Anomaly Detection</b>",
            font=dict(size=18, color="#f8fafc"),
            x=0.01,
        ),
        xaxis=dict(
            title="Reading Timestamp",
            gridcolor="rgba(255,255,255,0.06)",
            showgrid=True,
            zeroline=False,
            color="#94a3b8",
        ),
        yaxis=dict(
            title="Global Active Power (kW)",
            gridcolor="rgba(255,255,255,0.06)",
            showgrid=True,
            zeroline=False,
            color="#94a3b8",
        ),
        template="plotly_dark",
        paper_bgcolor="#0f172a",
        plot_bgcolor="#0f172a",
        margin=dict(l=40, r=20, t=50, b=40),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            bgcolor="rgba(15, 23, 42, 0.8)",
            bordercolor="rgba(255,255,255,0.1)",
            borderwidth=1,
        ),
        hovermode="x unified",
        height=420,
    )

    st.plotly_chart(fig_main, use_container_width=True)

else:
    st.info("⚡ Waiting for incoming live readings from Kafka topic `smartmeter-events`... Please ensure Kafka Producer is running.")


# ==========================================
# SECTION C & D: ADVANCED ANALYTICS
# ==========================================
col_an1, col_an2 = st.columns([1, 1])

with col_an1:
    st.subheader("🔍 Power Distribution & Outlier Spread")
    if not readings_df.empty:
        fig_dist = px.histogram(
            readings_df,
            x="global_active_power",
            color="is_anomaly",
            color_discrete_map={0: "#10b981", 1: "#ef4444"},
            nbins=35,
            opacity=0.85,
            labels={"global_active_power": "Power Consumption (kW)", "is_anomaly": "Classification"},
            template="plotly_dark",
        )
        fig_dist.update_layout(
            paper_bgcolor="#0f172a",
            plot_bgcolor="#0f172a",
            margin=dict(l=30, r=20, t=30, b=30),
            height=300,
            showlegend=True,
            legend=dict(
                title=None,
                orientation="h",
                y=1.1,
                x=0.5,
                xanchor="center",
            ),
        )
        st.plotly_chart(fig_dist, use_container_width=True)
    else:
        st.write("Awaiting data stream...")

with col_an2:
    st.subheader("⏱️ Hourly Consumption Pattern")
    if not readings_df.empty:
        df_hourly = readings_df.copy()
        df_hourly["hour"] = df_hourly["reading_time"].dt.hour
        hourly_stats = df_hourly.groupby("hour")["global_active_power"].mean().reset_index()

        fig_hour = px.bar(
            hourly_stats,
            x="hour",
            y="global_active_power",
            labels={"hour": "Hour of Day (0-23)", "global_active_power": "Avg Active Power (kW)"},
            template="plotly_dark",
            color="global_active_power",
            color_continuous_scale="Viridis",
        )
        fig_hour.update_layout(
            paper_bgcolor="#0f172a",
            plot_bgcolor="#0f172a",
            margin=dict(l=30, r=20, t=30, b=30),
            height=300,
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig_hour, use_container_width=True)
    else:
        st.write("Awaiting data stream...")


# ==========================================
# SECTION E: RECENT ANOMALY FEED & LOGS
# ==========================================
st.markdown("---")
st.subheader("🚨 Live Anomaly Incident Log")

if not anomalies_df.empty:
    display_anomalies = anomalies_df.copy()
    display_anomalies.columns = ["ID", "Incident Timestamp", "Power (kW)", "Rolling Avg (kW)", "Logged At"]
    st.dataframe(
        display_anomalies,
        use_container_width=True,
        hide_index=True,
    )
else:
    st.success("No anomalies currently detected. Grid consumption operating within nominal parameters.")


# ==========================================
# AUTO REFRESH LOGIC
# ==========================================
if auto_refresh_enabled:
    time.sleep(refresh_rate)
    st.rerun()
