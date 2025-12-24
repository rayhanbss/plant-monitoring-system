import pytz
import time
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from statsmodels.tsa.holtwinters import Holt
import warnings
warnings.filterwarnings('ignore')

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

def train_holt_model(series, damped=True):
    """
    Train Holt's Linear Trend model with damped trend option.
    
    Args:
        series: pandas Series with numeric values (must have at least 10 observations)
        damped: whether to use damped trend (recommended for longer forecasts)
    
    Returns:
        Fitted Holt model
    """
    try:
        # Ensure we have enough data points
        if len(series) < 10:
            return None
        
        # Remove any remaining NaN values
        series = series.dropna()
        
        if len(series) < 10:
            return None
        
        model = Holt(
            series,
            exponential=False,
            damped_trend=damped,
            initialization_method='estimated'
        )
        model_fit = model.fit(optimized=True)
        return model_fit
    except Exception as e:
        st.warning(f"Model training failed: {str(e)}")
        return None


def prepare_ml_timeseries(df, feature, freq="1s"):
    """
    Prepare clean time-series for Holt model.
    
    For large datasets (>10000 points), automatically downsample for performance.
    
    Args:
        df: DataFrame with 'timestamp' column and feature column
        feature: column name to prepare
        freq: resampling frequency (e.g., '1s', '5s', '1min')
    
    Returns:
        pandas Series ready for Holt model
    """
    if df.empty or feature not in df.columns:
        return pd.Series(dtype=float)
    
    # Copy relevant columns
    ts_df = df[["timestamp", feature]].copy()
    ts_df = ts_df.dropna()
    
    if ts_df.empty:
        return pd.Series(dtype=float)
    
    # Preserve timezone info
    original_tz = None
    if ts_df["timestamp"].dt.tz is not None:
        original_tz = ts_df["timestamp"].dt.tz
    
    # Set timestamp as index
    ts_df = ts_df.set_index("timestamp")
    
    # Adaptive resampling based on data size
    data_points = len(ts_df)
    
    if data_points > 50000:
        # For very large datasets, use 1-minute aggregation
        actual_freq = "1min"
    elif data_points > 10000:
        # For large datasets, use 10-second aggregation
        actual_freq = "10s"
    else:
        actual_freq = freq
    
    # Resample and interpolate
    ts = ts_df[feature].resample(actual_freq).mean()
    ts = ts.interpolate(method="time")
    ts = ts.dropna()
    
    # Restore timezone if it was present
    if original_tz is not None and ts.index.tz is None:
        ts.index = ts.index.tz_localize(original_tz)
    
    return ts


def forecast_holt(model_fit, steps):
    """
    Generate forecast from fitted Holt model.
    
    Args:
        model_fit: Fitted Holt model
        steps: Number of steps to forecast
    
    Returns:
        pandas Series with forecasted values
    """
    if model_fit is None:
        return pd.Series(dtype=float)
    
    try:
        forecast = model_fit.forecast(steps)
        return forecast
    except Exception as e:
        st.warning(f"Forecast failed: {str(e)}")
        return pd.Series(dtype=float)


def get_holt_model_summary(model_fit):
    """
    Extract key parameters from fitted Holt model.
    
    Returns:
        dict with model parameters
    """
    if model_fit is None:
        return {}
    
    try:
        return {
            "smoothing_level": model_fit.params.get('smoothing_level', None),
            "smoothing_trend": model_fit.params.get('smoothing_trend', None),
            "damping_trend": model_fit.params.get('damping_trend', None),
            "aic": model_fit.aic if hasattr(model_fit, 'aic') else None,
            "bic": model_fit.bic if hasattr(model_fit, 'bic') else None,
            "sse": model_fit.sse if hasattr(model_fit, 'sse') else None,
        }
    except:
        return {}


def run_multi_feature_forecast(df, features, horizon_steps, freq="1s"):
    """
    Run Holt forecast for multiple features.
    
    Args:
        df: DataFrame with sensor data
        features: list of feature names to forecast
        horizon_steps: number of steps to forecast
        freq: resampling frequency
    
    Returns:
        dict with forecast results for each feature
    """
    results = {}
    
    for feature in features:
        # Prepare time series
        ts = prepare_ml_timeseries(df, feature, freq)
        
        if len(ts) < 10:
            results[feature] = {
                "success": False,
                "error": "Insufficient data points (need at least 10)",
                "historical": ts,
                "forecast": pd.Series(dtype=float),
                "model_params": {}
            }
            continue
        
        # Train model
        model_fit = train_holt_model(ts)
        
        if model_fit is None:
            results[feature] = {
                "success": False,
                "error": "Model training failed",
                "historical": ts,
                "forecast": pd.Series(dtype=float),
                "model_params": {}
            }
            continue
        
        # Generate forecast
        forecast = forecast_holt(model_fit, horizon_steps)
        
        # Create forecast index (future timestamps) - preserve timezone
        last_timestamp = ts.index[-1]
        freq_td = pd.Timedelta(ts.index[-1] - ts.index[-2]) if len(ts) > 1 else pd.Timedelta(seconds=1)
        
        # Create forecast index with same timezone as historical data
        forecast_index = pd.date_range(
            start=last_timestamp + freq_td,
            periods=horizon_steps,
            freq=freq_td,
            tz=ts.index.tz  # Preserve timezone from historical data
        )
        forecast.index = forecast_index
        
        results[feature] = {
            "success": True,
            "error": None,
            "historical": ts,
            "forecast": forecast,
            "model_params": get_holt_model_summary(model_fit),
            "last_value": ts.iloc[-1],
            "forecast_start": forecast.iloc[0] if len(forecast) > 0 else None,
            "forecast_end": forecast.iloc[-1] if len(forecast) > 0 else None,
            "trend_direction": "up" if (len(forecast) > 0 and forecast.iloc[-1] > ts.iloc[-1]) else "down"
        }
    
    return results


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

st.sidebar.markdown("## Forecast Configuration")

forecast_feature = st.sidebar.multiselect(
    "Select Features to Forecast",
    ["duration", "temperature", "humidity", "soil_moisture", "light_level"],
    default=["duration"]
)

forecast_horizon = st.sidebar.slider(
    "Forecast Horizon (seconds)",
    min_value=10,
    max_value=600,
    value=60,
    step=10
)

run_forecast = st.sidebar.button("Run Forecast")

#  Sidebar - Filters
st.sidebar.markdown("### Configuration")

view_option = st.sidebar.selectbox(
    "View Mode",
    ["Dashboard", "Sensor Data Analysis", "Weather Data Analysis", "ML Forecast"],
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

elif view_option == "ML Forecast":
    # ML Forecast View - Holt's Exponential Smoothing
    st.markdown("### ML Forecast - Holt's Exponential Smoothing")
    
    st.markdown("""
    **Holt's Linear Trend Method** (Double Exponential Smoothing) is used to forecast 
    time series data that exhibits a trend. This method uses two smoothing equations:
    - **Level equation**: captures the base value of the series
    - **Trend equation**: captures the trend (slope) of the series
    
    The damped trend variant is used to prevent forecasts from trending indefinitely.
    """)
    
    st.markdown("---")
    
    if sensor_df.empty:
        st.warning("No sensor data available for forecasting. Please check your data source.")
    else:
        # Data Overview
        st.markdown("#### Data Overview")
        data_cols = st.columns(4)
        
        data_cols[0].metric("Total Data Points", f"{len(sensor_df):,}")
        data_cols[1].metric("Time Range", f"{(sensor_df['timestamp'].max() - sensor_df['timestamp'].min()).days} days")
        data_cols[2].metric("Selected Features", len(forecast_feature))
        data_cols[3].metric("Forecast Horizon", f"{forecast_horizon} steps")
        
        st.markdown("---")
        
        # Run Forecast Button Logic
        if run_forecast and len(forecast_feature) > 0:
            with st.spinner("Training models and generating forecasts..."):
                # Determine appropriate frequency based on data
                data_points = len(sensor_df)
                if data_points > 50000:
                    resample_freq = "1min"
                    freq_label = "1 minute"
                elif data_points > 10000:
                    resample_freq = "10s"
                    freq_label = "10 seconds"
                else:
                    resample_freq = "1s"
                    freq_label = "1 second"
                
                st.info(f"Using {freq_label} resolution for {data_points:,} data points")
                
                # Run forecast for all selected features
                forecast_results = run_multi_feature_forecast(
                    sensor_df, 
                    forecast_feature, 
                    forecast_horizon,
                    freq=resample_freq
                )
                
                # Store results in session state
                st.session_state['forecast_results'] = forecast_results
                st.session_state['forecast_timestamp'] = datetime.now(LOCAL_TZ)
        
        # Display Results if available
        if 'forecast_results' in st.session_state and st.session_state['forecast_results']:
            forecast_results = st.session_state['forecast_results']
            forecast_time = st.session_state.get('forecast_timestamp', datetime.now(LOCAL_TZ))
            
            st.success(f"Forecast generated at {forecast_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Feature unit mapping
            unit_map = {
                "duration": "seconds",
                "temperature": "°C",
                "humidity": "%",
                "soil_moisture": "%",
                "light_level": "lux"
            }
            
            color_map = {
                "duration": "#27ae60",
                "temperature": "#e74c3c",
                "humidity": "#3498db",
                "soil_moisture": "#9b59b6",
                "light_level": "#f39c12"
            }
            
            # Display each feature forecast
            for feature, result in forecast_results.items():
                st.markdown(f"---")
                st.markdown(f"#### {feature.replace('_', ' ').title()} Forecast")
                
                if not result["success"]:
                    st.error(f"Forecast failed: {result['error']}")
                    continue
                
                # Metrics Row
                metrics_cols = st.columns(5)
                
                unit = unit_map.get(feature, "")
                color = color_map.get(feature, "#27ae60")
                
                last_val = result["last_value"]
                forecast_start = result["forecast_start"]
                forecast_end = result["forecast_end"]
                trend = result["trend_direction"]
                
                metrics_cols[0].metric(
                    "Last Observed Value",
                    f"{last_val:.2f} {unit}"
                )
                metrics_cols[1].metric(
                    "Forecast Start",
                    f"{forecast_start:.2f} {unit}" if forecast_start else "N/A"
                )
                metrics_cols[2].metric(
                    "Forecast End",
                    f"{forecast_end:.2f} {unit}" if forecast_end else "N/A",
                    delta=f"{(forecast_end - last_val):.2f}" if forecast_end else None,
                    delta_color="normal"
                )
                metrics_cols[3].metric(
                    "Trend Direction",
                    "Upward" if trend == "up" else "Downward"
                )
                metrics_cols[4].metric(
                    "Historical Points",
                    f"{len(result['historical']):,}"
                )
                
                # Plot: Historical + Forecast
                fig = go.Figure()
                
                # Historical data (last 500 points for visualization)
                hist_data = result["historical"].tail(500)
                fig.add_trace(go.Scatter(
                    x=hist_data.index,
                    y=hist_data.values,
                    mode='lines',
                    name='Historical',
                    line=dict(color=color, width=2)
                ))
                
                # Forecast data - connect from last historical point
                forecast_data = result["forecast"]
                
                # Create connected forecast (start from last historical point)
                last_hist_time = hist_data.index[-1]
                last_hist_value = hist_data.values[-1]
                
                # Prepend the last historical point to forecast for seamless connection
                connected_forecast_index = pd.Index([last_hist_time]).append(forecast_data.index)
                connected_forecast_values = np.concatenate([[last_hist_value], forecast_data.values])
                
                fig.add_trace(go.Scatter(
                    x=connected_forecast_index,
                    y=connected_forecast_values,
                    mode='lines+markers',
                    name='Forecast',
                    line=dict(color='#e74c3c', width=3, dash='dash'),
                    marker=dict(size=6)
                ))
                
                # Add vertical line at forecast start using add_shape (avoids timestamp annotation issue)
                forecast_start_time = hist_data.index[-1]
                fig.add_shape(
                    type="line",
                    x0=forecast_start_time,
                    x1=forecast_start_time,
                    y0=0,
                    y1=1,
                    yref="paper",
                    line=dict(color="gray", width=2, dash="dot")
                )
                
                # Add annotation separately
                fig.add_annotation(
                    x=forecast_start_time,
                    y=1,
                    yref="paper",
                    text="Forecast Start",
                    showarrow=False,
                    font=dict(size=10, color="gray"),
                    yanchor="bottom"
                )
                
                fig.update_layout(
                    title=f"{feature.replace('_', ' ').title()} - Historical vs Forecast",
                    xaxis_title="Time",
                    yaxis_title=f"{feature.replace('_', ' ').title()} ({unit})",
                    hovermode='x unified',
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    ),
                    margin=dict(l=0, r=0, t=60, b=0)
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Model Parameters (Expandable)
                with st.expander(f"Model Parameters for {feature.replace('_', ' ').title()}"):
                    params = result["model_params"]
                    if params:
                        param_cols = st.columns(3)
                        
                        param_cols[0].metric(
                            "Smoothing Level (α)",
                            f"{params.get('smoothing_level', 0):.4f}" if params.get('smoothing_level') else "N/A",
                            help="Controls weight of recent observations on level"
                        )
                        param_cols[1].metric(
                            "Smoothing Trend (β)",
                            f"{params.get('smoothing_trend', 0):.4f}" if params.get('smoothing_trend') else "N/A",
                            help="Controls weight of recent observations on trend"
                        )
                        param_cols[2].metric(
                            "Damping Factor (φ)",
                            f"{params.get('damping_trend', 0):.4f}" if params.get('damping_trend') else "N/A",
                            help="Dampens the trend over time (prevents runaway forecasts)"
                        )
                        
                        st.markdown("**Model Fit Statistics:**")
                        stats_cols = st.columns(3)
                        stats_cols[0].write(f"AIC: {params.get('aic', 'N/A'):.2f}" if params.get('aic') else "AIC: N/A")
                        stats_cols[1].write(f"BIC: {params.get('bic', 'N/A'):.2f}" if params.get('bic') else "BIC: N/A")
                        stats_cols[2].write(f"SSE: {params.get('sse', 'N/A'):.2f}" if params.get('sse') else "SSE: N/A")
                    else:
                        st.info("No model parameters available")
                
                # Forecast Data Table (Expandable)
                with st.expander(f"Forecast Data Table for {feature.replace('_', ' ').title()}"):
                    forecast_df = pd.DataFrame({
                        'Timestamp': forecast_data.index,
                        f'{feature.replace("_", " ").title()} ({unit})': forecast_data.values
                    })
                    forecast_df['Timestamp'] = forecast_df['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
                    st.dataframe(forecast_df, use_container_width=True)
            
            st.markdown("---")
            
            # Combined Forecast Comparison (if multiple features)
            if len(forecast_results) > 1:
                st.markdown("#### Combined Forecast Comparison")
                
                # Normalize values for comparison
                fig_combined = go.Figure()
                
                for feature, result in forecast_results.items():
                    if result["success"]:
                        # Normalize forecast values (0-100 scale)
                        forecast_data = result["forecast"]
                        if len(forecast_data) > 0:
                            min_val = forecast_data.min()
                            max_val = forecast_data.max()
                            if max_val - min_val > 0:
                                normalized = (forecast_data - min_val) / (max_val - min_val) * 100
                            else:
                                normalized = forecast_data * 0 + 50
                            
                            fig_combined.add_trace(go.Scatter(
                                x=forecast_data.index,
                                y=normalized.values,
                                mode='lines+markers',
                                name=feature.replace('_', ' ').title(),
                                line=dict(width=2)
                            ))
                
                fig_combined.update_layout(
                    title="Normalized Forecast Comparison (0-100 scale)",
                    xaxis_title="Time",
                    yaxis_title="Normalized Value",
                    hovermode='x unified',
                    legend=dict(
                        orientation="h",
                        yanchor="bottom",
                        y=1.02,
                        xanchor="right",
                        x=1
                    ),
                    margin=dict(l=0, r=0, t=60, b=0)
                )
                
                st.plotly_chart(fig_combined, use_container_width=True)
        
        else:
            st.info("Select features and click **Run Forecast** to generate predictions")
            
            # Show data distribution preview
            st.markdown("#### Current Data Distribution")
            
            available_features = ["duration", "temperature", "humidity", "soil_moisture", "light_level"]
            preview_cols = st.columns(len(available_features))
            
            for col, feature in zip(preview_cols, available_features):
                if feature in sensor_df.columns:
                    col.metric(
                        feature.replace('_', ' ').title(),
                        f"{sensor_df[feature].mean():.2f}",
                        help=f"Min: {sensor_df[feature].min():.2f} | Max: {sensor_df[feature].max():.2f}"
                    )

# AUTO REFRESH
if auto_refresh:
    time.sleep(refresh_interval)
    st.experimental_rerun()
