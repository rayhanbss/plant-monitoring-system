import pytz
import time
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configurations
CASSANDRA_HOST = "cassandra"
KEYSPACE = "plant_monitoring"
REFRESH_INTERVAL = 10
LOCAL_TZ = pytz.timezone('Asia/Jakarta')  # UTC+7

DATA_CONFIG = {
    "Last 1 Hour": {"resample": None, "limit": 5000},
    "Last 24 Hours": {"resample": "1min", "limit": 100000},
    "Last 7 Days": {"resample": "10min", "limit": 700000},
    "Custom Range": {"resample": "adaptive", "limit": 1000000}
}

st.set_page_config(
    page_title="Plant Monitoring Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    h1 {
        color: #27ae60;
        padding-bottom: 10px;
        border-bottom: 3px solid #27ae60;
    }
    h2, h3 {
        color: #ffffff;
        margin-top: 20px;
    }
    .st-emotion-cache-16idsys p {
        font-size: 16px;
    }
    </style>
    """, unsafe_allow_html=True)

# Cassandra Connection
@st.cache_resource
def get_session():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(KEYSPACE)
    return session

session = get_session()

# Query Helpers
@st.cache_data(ttl=300)
def get_available_devices():
    query = "SELECT DISTINCT device_id FROM sensor_data"
    rows = session.execute(query)
    devices = [row.device_id for row in rows]
    return devices if devices else ["esp32_client"]

@st.cache_data(ttl=300)
def get_available_cities():
    query = "SELECT DISTINCT city_id FROM weather_data"
    rows = session.execute(query)
    city_ids = [row.city_id for row in rows]
    
    cities = []
    for city_id in city_ids:
        query = "SELECT city_name FROM weather_data WHERE city_id = %s LIMIT 1"
        result = session.execute(query, [city_id])
        row = result.one()
        if row:
            cities.append((row.city_name, city_id))
    
    return cities if cities else [("Surakarta", 1625812)]

def fetch_sensor_data(device_id, start_time=None, end_time=None, limit=100000):
    if start_time and end_time:
        query = """
            SELECT device_id, timestamp, temperature, humidity,
                   soil_moisture, light_level, status, duration
            FROM sensor_data
            WHERE device_id = %s AND timestamp >= %s AND timestamp <= %s
            LIMIT %s
            ALLOW FILTERING
        """
        params = [device_id, start_time, end_time, limit]
    else:
        query = """
            SELECT device_id, timestamp, temperature, humidity,
                   soil_moisture, light_level, status, duration
            FROM sensor_data
            WHERE device_id = %s
            LIMIT %s
        """
        params = [device_id, limit]

    rows = session.execute(query, params)
    df = pd.DataFrame(list(rows))

    if df.empty:
        return df

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    # Convert to UTC+7 for display
    df["timestamp"] = df["timestamp"].dt.tz_convert(LOCAL_TZ)

    return df.sort_values("timestamp")

def fetch_weather_data(city_id, start_time=None, end_time=None, limit=100000):
    """
    Fetch weather data with timestamp range filtering.
    Weather data biasanya lebih jarang (per jam), jadi tidak perlu limit setinggi sensor.
    """
    if start_time and end_time:
        query = """
            SELECT city_id, timestamp, city_name, temperature,
                   weather_condition, weather_description
            FROM weather_data
            WHERE city_id = %s AND timestamp >= %s AND timestamp <= %s
            LIMIT %s
            ALLOW FILTERING
        """
        params = [int(city_id), start_time, end_time, limit]
    else:
        query = """
            SELECT city_id, timestamp, city_name, temperature,
                   weather_condition, weather_description
            FROM weather_data
            WHERE city_id = %s
            LIMIT %s
        """
        params = [int(city_id), limit]
    
    rows = session.execute(query, params)
    df = pd.DataFrame(list(rows))

    if df.empty:
        return df

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    # Convert to UTC+7 for display
    df["timestamp"] = df["timestamp"].dt.tz_convert(LOCAL_TZ)

    return df.sort_values("timestamp")


def downsample_sensor_data(df, resample_rule):
    """
    Downsample sensor data menggunakan aggregation.
    - Numeric columns: mean
    - Status: mode (most frequent)
    - Duration: sum
    """
    if df.empty or resample_rule is None:
        return df
    
    df = df.set_index("timestamp")
    
    # Aggregation rules untuk setiap kolom
    numeric_cols = ["temperature", "humidity", "soil_moisture", "light_level"]
    
    # Resample numeric columns dengan mean
    resampled = df[numeric_cols].resample(resample_rule).agg({
        "temperature": "mean",
        "humidity": "mean", 
        "soil_moisture": "mean",
        "light_level": "mean"
    })
    
    # Handle status - ambil yang paling sering muncul dalam interval
    if "status" in df.columns:
        status_resampled = df["status"].resample(resample_rule).agg(
            lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None
        )
        resampled["status"] = status_resampled
    
    # Handle duration - sum dalam interval
    if "duration" in df.columns:
        duration_resampled = df["duration"].resample(resample_rule).sum()
        resampled["duration"] = duration_resampled
    
    # Handle device_id - ambil first
    if "device_id" in df.columns:
        device_resampled = df["device_id"].resample(resample_rule).first()
        resampled["device_id"] = device_resampled
    
    resampled = resampled.dropna(how="all").reset_index()
    
    return resampled


def downsample_weather_data(df, resample_rule):
    """
    Downsample weather data menggunakan aggregation.
    - Temperature: mean
    - Weather condition/description: mode
    """
    if df.empty or resample_rule is None:
        return df
    
    df = df.set_index("timestamp")
    
    # Resample temperature dengan mean
    resampled = df[["temperature"]].resample(resample_rule).mean()
    
    # Handle categorical columns - ambil yang paling sering
    for col in ["weather_condition", "weather_description", "city_name"]:
        if col in df.columns:
            col_resampled = df[col].resample(resample_rule).agg(
                lambda x: x.mode().iloc[0] if len(x.mode()) > 0 else None
            )
            resampled[col] = col_resampled
    
    # Handle city_id - ambil first
    if "city_id" in df.columns:
        city_resampled = df["city_id"].resample(resample_rule).first()
        resampled["city_id"] = city_resampled
    
    resampled = resampled.dropna(how="all").reset_index()
    
    return resampled


def get_adaptive_resample_rule(start_time, end_time):
    """
    Menentukan resample rule berdasarkan rentang waktu.
    Tujuan: menjaga jumlah data points sekitar 1000-2000 untuk performa optimal.
    """
    duration = end_time - start_time
    total_seconds = duration.total_seconds()
    
    if total_seconds <= 3600:  # <= 1 hour
        return None  # No resampling, raw data
    elif total_seconds <= 86400:  # <= 1 day
        return "1min"  # 1 minute intervals
    elif total_seconds <= 604800:  # <= 1 week
        return "10min"  # 10 minute intervals
    elif total_seconds <= 2592000:  # <= 30 days
        return "1h"  # 1 hour intervals
    else:  # > 30 days
        return "6h"  # 6 hour intervals


def fetch_all_data_parallel(device_id, city_id, start_time, end_time, range_option="Custom Range"):
    """
    Fetch sensor and weather data in parallel, then apply downsampling.
    Strategy:
    1. Fetch semua data dalam range dari Cassandra
    2. Apply downsampling di pandas berdasarkan time range
    """
    # Tentukan config berdasarkan range
    config = DATA_CONFIG.get(range_option, DATA_CONFIG["Custom Range"])
    limit = config["limit"]
    
    # Tentukan resample rule
    if config["resample"] == "adaptive":
        resample_rule = get_adaptive_resample_rule(start_time, end_time)
    else:
        resample_rule = config["resample"]
    
    # Fetch data in parallel
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_sensor = executor.submit(fetch_sensor_data, device_id, start_time, end_time, limit)
        future_weather = executor.submit(fetch_weather_data, city_id, start_time, end_time, limit)
        
        sensor_df = future_sensor.result()
        weather_df = future_weather.result()
    
    # Apply downsampling jika diperlukan
    if resample_rule:
        sensor_df = downsample_sensor_data(sensor_df, resample_rule)
        weather_df = downsample_weather_data(weather_df, resample_rule)
    
    return sensor_df, weather_df, resample_rule

#  Sidebar - Filters
st.sidebar.markdown("### Configuration")

view_option = st.sidebar.selectbox(
    "View Mode",
    ["Dashboard", "Sensor Data Analysis", "Weather Data Analysis"],
    index=0
)

st.sidebar.markdown("---")

available_devices = get_available_devices()
device_id = st.sidebar.selectbox(
    "Select Device",
    available_devices,
    index=0
)

available_cities = get_available_cities()
city_id = st.sidebar.selectbox(
    "Select City",
    available_cities,
    format_func=lambda x: x[0],
    index=0
)[1]

st.sidebar.markdown("---")

# Different time selection based on view mode
if view_option == "Dashboard":
    # Dashboard: pilih opsi waktu (hour, day, week, range)
    range_option = st.sidebar.radio(
        "Show data for",
        ["Last 1 Hour", "Last 24 Hours", "Last 7 Days", "Custom Range"],
        horizontal=True
    )

    if range_option == "Custom Range":
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.sidebar.date_input("From", value=datetime.now().date() - timedelta(days=7), key="start")
        with col2:
            end_date = st.sidebar.date_input("To", value=datetime.now().date(), key="end")
        
        if start_date > end_date:
            st.sidebar.error("Invalid date range")
            start_date = end_date - timedelta(days=1)
        
        start_time = pytz.UTC.localize(datetime.combine(start_date, datetime.min.time()))
        end_time = pytz.UTC.localize(datetime.combine(end_date, datetime.max.time()))
    else:
        # End time adalah waktu sekarang (UTC) untuk opsi "Last X"
        end_time = datetime.now(pytz.UTC)
        
        delta_map = {
            "Last 1 Hour": timedelta(hours=1),
            "Last 24 Hours": timedelta(days=1),
            "Last 7 Days": timedelta(weeks=1)
        }
        start_time = end_time - delta_map[range_option]
else:
    # Analysis views: hanya pilih date range (hari)
    st.sidebar.markdown("**Select Date Range**")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.sidebar.date_input("From", value=datetime.now().date() - timedelta(days=7), key="analysis_start")
    with col2:
        end_date = st.sidebar.date_input("To", value=datetime.now().date(), key="analysis_end")
    
    if start_date > end_date:
        st.sidebar.error("Invalid date range")
        start_date = end_date - timedelta(days=1)
    
    start_time = pytz.UTC.localize(datetime.combine(start_date, datetime.min.time()))
    end_time = pytz.UTC.localize(datetime.combine(end_date, datetime.max.time()))
    range_option = "Custom Range"  # Use custom range config for analysis

st.sidebar.markdown("---")

auto_refresh = st.sidebar.checkbox("Auto Refresh", value=False)
if auto_refresh:
    refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 10)

# Fetch Data in Parallel with Loading Indicator
with st.spinner('Loading data...'):
    sensor_df, weather_df, resample_rule = fetch_all_data_parallel(device_id, city_id, start_time, end_time, range_option)

# RT Dashboard
st.title("Plant Monitoring System")

# Info tentang data yang ditampilkan
location_name = [city[0] for city in available_cities if city[1] == city_id][0]
data_info = f"**Device:** {device_id} | **Location:** {location_name} | **Period:** {range_option}"

# Tampilkan info downsampling jika aktif
if resample_rule:
    data_info += f" | **Resolution:** {resample_rule}"
    
# Tampilkan jumlah data points
sensor_count = len(sensor_df) if not sensor_df.empty else 0
weather_count = len(weather_df) if not weather_df.empty else 0
data_info += f" | **Data Points:** Sensor: {sensor_count:,}, Weather: {weather_count:,}"

st.markdown(data_info)
st.markdown("---")

if not sensor_df.empty:
    latest_sensor = sensor_df.iloc[-1]
else:
    latest_sensor = None

if not weather_df.empty:
    latest_weather = weather_df.iloc[-1]
else:
    latest_weather = None

# Display based on selected view
if view_option == "Dashboard":
    # Top Metrics with better styling
    st.markdown("### Current Status")
    col1, col2 = st.columns(2)

    with col1:
        if latest_sensor is not None:
            status = latest_sensor["status"]
            if status and status.lower() != "tidak disiram":
                duration = latest_sensor["duration"]
                display_status = f"{status} ({duration:.1f}s)"
            else:
                display_status = status
        else:
            display_status = "N/A"
        
        st.markdown(
            f'<div style="background-color: #d5f4e6; padding: 15px; border-radius: 10px; border-left: 5px solid #27ae60;">'
            f'<p style="margin: 0; color: #555; font-size: 14px;">Watering Condition</p>'
            f'<p style="margin: 0; color: #1e1e1e; font-size: 24px; font-weight: bold;">{display_status}</p>'
            f'</div>',
            unsafe_allow_html=True
        )

    with col2:
        weather = latest_weather["weather_condition"] if latest_weather is not None else "N/A"
        st.markdown(
            f'<div style="background-color: #d5f4e6; padding: 15px; border-radius: 10px; border-left: 5px solid #27ae60;">'
            f'<p style="margin: 0; color: #555; font-size: 14px;">Current Weather</p>'
            f'<p style="margin: 0; color: #1e1e1e; font-size: 24px; font-weight: bold;">{weather}</p>'
            f'</div>',
            unsafe_allow_html=True
        )

    st.markdown("---")
    
    # Sensor + Weather Metrics
    st.markdown("### Real-time Measurements")
    cols = st.columns(5)

    metrics = [
        ("Plant Temperature", "temperature", "°C"),
        ("Soil Moisture", "soil_moisture", "%"),
        ("Humidity", "humidity", "%"),
        ("Light Level", "light_level", "lux"),
    ]

    colors = ["#fff3cd", "#d1ecf1", "#e7e7ff", "#fff4e6"]
    border_colors = ["#ffc107", "#17a2b8", "#6f42c1", "#fd7e14"]
    
    for col, (label, key, unit), bg_color, border_color in zip(cols[:4], metrics, colors, border_colors):
        value = latest_sensor[key] if latest_sensor is not None else "N/A"
        display_value = f"{value:.1f} {unit}" if value != "N/A" else "N/A"
        col.markdown(
            f'<div style="background-color: {bg_color}; padding: 15px; border-radius: 10px; border-left: 5px solid {border_color};">'
            f'<p style="margin: 0; color: #555; font-size: 12px;">{label}</p>'
            f'<p style="margin: 0; color: #1e1e1e; font-size: 20px; font-weight: bold;">{display_value}</p>'
            f'</div>',
            unsafe_allow_html=True
        )

    weather_temp = latest_weather["temperature"] if latest_weather is not None else "N/A"
    weather_display = f"{weather_temp:.1f} °C" if weather_temp != "N/A" else "N/A"
    cols[4].markdown(
        f'<div style="background-color: #ffe6e6; padding: 15px; border-radius: 10px; border-left: 5px solid #dc3545;">'
        f'<p style="margin: 0; color: #555; font-size: 12px;">Weather Temperature</p>'
        f'<p style="margin: 0; color: #1e1e1e; font-size: 20px; font-weight: bold;">{weather_display}</p>'
        f'</div>',
        unsafe_allow_html=True
    )

    st.markdown("---")
    
    # Comparison Graph
    st.markdown("### Temperature Comparison: Plant vs Weather")

    if not sensor_df.empty and not weather_df.empty:
        temp_compare = pd.merge_asof(
            sensor_df.sort_values("timestamp"),
            weather_df.sort_values("timestamp"),
            on="timestamp",
            direction="nearest",
            suffixes=("_plant", "_weather")
        )

        fig = px.line(
            temp_compare,
            x="timestamp",
            y=["temperature_plant", "temperature_weather"],
            labels={"value": "Temperature (°C)", "variable": "Source", "timestamp": "Time"},
            color_discrete_map={
                "temperature_plant": "#2ecc71",
                "temperature_weather": "#3498db"
            }
        )
        fig.update_layout(
            hovermode='x unified',
            legend=dict(
                title="",
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            margin=dict(l=0, r=0, t=30, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Insufficient data for temperature comparison")

    # Sensor Trends
    st.markdown("### Sensor Trends Over Time")

    if not sensor_df.empty:
        trend_metrics = [
            ("Humidity", "humidity", "%", "#9b59b6"),
            ("Soil Moisture", "soil_moisture", "%", "#e67e22"),
            ("Light Level", "light_level", "lux", "#f1c40f")
        ]
        
        for label, col_name, unit, color in trend_metrics:
            fig = px.line(
                sensor_df,
                x="timestamp",
                y=col_name,
                labels={"timestamp": "Time", col_name: f"{label} ({unit})"}
            )
            fig.update_traces(line_color=color, line_width=2)
            fig.update_layout(
                title=f"{label} Trend",
                hovermode='x unified',
                margin=dict(l=0, r=0, t=40, b=0)
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No sensor data available for trend analysis")

elif view_option == "Sensor Data Analysis":
    # Sensor Data Analysis
    st.markdown("### Sensor Data Analysis")

    if not sensor_df.empty:
        # Statistics Cards
        st.markdown("#### Statistical Summary")
        avg_cols = st.columns(4)

        stats_metrics = [
            ("Temperature", "temperature", "°C"),
            ("Soil Moisture", "soil_moisture", "%"),
            ("Humidity", "humidity", "%"),
            ("Light Level", "light_level", "lux")
        ]

        for col, (label, field, unit) in zip(avg_cols, stats_metrics):
            mean_val = sensor_df[field].mean()
            min_val = sensor_df[field].min()
            max_val = sensor_df[field].max()
            
            col.metric(
                f"{label}",
                f"{mean_val:.2f} {unit}",
                help=f"Min: {min_val:.2f} {unit} | Max: {max_val:.2f} {unit} | Std: {sensor_df[field].std():.2f}"
            )

        st.markdown("---")
        
        # Detailed Charts
        st.markdown("#### Detailed Sensor Metrics")
        
        chart_configs = [
            ("Temperature Distribution", "temperature", "°C", "#e74c3c"),
            ("Soil Moisture Distribution", "soil_moisture", "%", "#3498db"),
            ("Humidity Distribution", "humidity", "%", "#2ecc71"),
            ("Light Level Distribution", "light_level", "lux", "#f39c12")
        ]
        
        for title, field, unit, color in chart_configs:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                fig_line = px.line(
                    sensor_df,
                    x="timestamp",
                    y=field,
                    labels={"timestamp": "Time", field: f"{field.replace('_', ' ').title()} ({unit})"}
                )
                fig_line.update_traces(line_color=color, line_width=2)
                fig_line.update_layout(
                    title=f"{title} Over Time",
                    hovermode='x unified',
                    margin=dict(l=0, r=0, t=40, b=0)
                )
                st.plotly_chart(fig_line, use_container_width=True)
            
            with col2:
                fig_hist = px.histogram(
                    sensor_df,
                    x=field,
                    labels={field: unit},
                    nbins=20
                )
                fig_hist.update_traces(marker_color=color)
                fig_hist.update_layout(
                    title="Distribution",
                    margin=dict(l=0, r=0, t=40, b=0),
                    showlegend=False
                )
                st.plotly_chart(fig_hist, use_container_width=True)

        st.markdown("---")
        
        # Raw Data
        with st.expander("View Raw Sensor Data"):
            st.dataframe(sensor_df, use_container_width=True)
    else:
        st.info("No sensor data available for the selected time range.")

elif view_option == "Weather Data Analysis":
    # Weather Data Analysis
    st.markdown("### Weather Data Analysis")

    if not weather_df.empty:
        # Weather Statistics
        st.markdown("#### Weather Statistics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            avg_temp = weather_df["temperature"].mean()
            st.metric(
                "Average Temperature",
                f"{avg_temp:.1f} °C",
                help=f"Min: {weather_df['temperature'].min():.1f}°C | Max: {weather_df['temperature'].max():.1f}°C"
            )
        
        with col2:
            most_common = weather_df["weather_condition"].mode()[0] if len(weather_df) > 0 else "N/A"
            st.metric("Most Common Condition", most_common)
        
        with col3:
            data_points = len(weather_df)
            st.metric("Data Points", f"{data_points:,}")

        st.markdown("---")
        
        # Weather Visualizations
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### Temperature Trend")
            fig_temp = px.line(
                weather_df,
                x="timestamp",
                y="temperature",
                labels={"timestamp": "Time", "temperature": "Temperature (°C)"}
            )
            fig_temp.update_traces(line_color="#e67e22", line_width=3)
            fig_temp.update_layout(
                hovermode='x unified',
                margin=dict(l=0, r=0, t=10, b=0)
            )
            st.plotly_chart(fig_temp, use_container_width=True)

        with col2:
            st.markdown("#### Weather Condition Distribution")
            condition_counts = weather_df["weather_condition"].value_counts().reset_index()
            condition_counts.columns = ["condition", "count"]
            
            fig_pie = px.pie(
                condition_counts,
                names="condition",
                values="count",
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig_pie.update_traces(
                textposition='inside',
                textinfo='percent+label'
            )
            fig_pie.update_layout(
                showlegend=False,
                margin=dict(l=0, r=0, t=10, b=0)
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        st.markdown("---")
        
        # Temperature Box Plot
        st.markdown("#### Temperature Distribution Analysis")
        fig_box = px.box(
            weather_df,
            y="temperature",
            labels={"temperature": "Temperature (°C)"},
            points="all"
        )
        fig_box.update_traces(marker_color="#3498db", line_color="#2c3e50")
        fig_box.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            showlegend=False
        )
        st.plotly_chart(fig_box, use_container_width=True)

        st.markdown("---")
        
        # Raw Weather Data
        with st.expander("View Raw Weather Data"):
            st.dataframe(weather_df, use_container_width=True)
    else:
        st.info("No weather data available for the selected time range.")

# AUTO REFRESH
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
