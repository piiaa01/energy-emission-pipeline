"""
Energy and Emission Monitoring Dashboard
Displays real-time energy consumption and CO₂ emissions during AI model training
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
from datetime import datetime, timedelta
import time

# Page configuration
st.set_page_config(
    page_title="Energy & Emission Monitor",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

# MongoDB Connection
@st.cache_resource
def get_mongodb_client():
    """Connect to MongoDB"""
    try:
        client = MongoClient('localhost', 27017, serverSelectionTimeoutMS=5000)
        client.server_info()  # Test connection
        return client
    except Exception as e:
        st.error(f"❌ MongoDB connection failed: {e}")
        return None


def get_collection():
    """Get MongoDB collection for training metrics"""
    client = get_mongodb_client()
    if client:
        db = client['energy_emissions']
        return db['training_metrics']
    return None


@st.cache_data(ttl=5)
def fetch_training_data():
    """Fetch all training metrics from MongoDB"""
    collection = get_collection()
    if collection is not None:
        try:
            data = list(collection.find({}, {'_id': 0}))
            if data:
                df = pd.DataFrame(data)
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                return df
            else:
                return pd.DataFrame()
        except Exception as e:
            st.error(f"Error fetching data: {e}")
            return pd.DataFrame()
    return pd.DataFrame()


def generate_sample_data():
    """Generate sample data for visualization testing"""
    import numpy as np
    users = ['alice', 'bob', 'charlie', 'diana']
    models = ['logistic_regression', 'random_forest', 'neural_network', 'xgboost']
    regions = ['US', 'EU', 'CN', 'IN']
    n_records = 100
    base_time = datetime.now() - timedelta(hours=24)

    data = {
        'timestamp': [base_time + timedelta(minutes=i * 15) for i in range(n_records)],
        'run_id': [f'run_{i // 10}' for i in range(n_records)],
        'user_id': np.random.choice(users, n_records),
        'model_name': np.random.choice(models, n_records),
        'cpu_utilization_pct': np.random.uniform(30, 90, n_records),
        'gpu_power_w': np.random.uniform(50, 300, n_records),
        'ram_used_mb': np.random.uniform(2000, 8000, n_records),
        'energy_kwh': np.random.uniform(0.1, 2.5, n_records),
        'emissions_kg': np.random.uniform(0.05, 1.2, n_records),
        'region_iso': np.random.choice(regions, n_records),
        'epoch': np.random.randint(1, 100, n_records),
        'accuracy': np.random.uniform(0.7, 0.95, n_records),
        'loss': np.random.uniform(0.1, 0.5, n_records)
    }
    return pd.DataFrame(data)


def insert_sample_data():
    """Insert sample data into MongoDB for testing"""
    collection = get_collection()
    if collection is not None:
        df = generate_sample_data()
        records = df.to_dict('records')
        collection.insert_many(records)
        st.success(f"✅ Inserted {len(records)} sample records into MongoDB")


def main():
    st.markdown('<div class="main-header">⚡ Energy & Emission Monitoring Dashboard</div>', unsafe_allow_html=True)

    # Sidebar
    st.sidebar.title("🎛️ Dashboard Controls")

    client = get_mongodb_client()
    if client:
        st.sidebar.success("✅ MongoDB Connected")
    else:
        st.sidebar.error("❌ MongoDB Disconnected")
        st.stop()

    st.sidebar.subheader("📊 Data Management")

    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    if st.sidebar.button("🧪 Insert Sample Data"):
        insert_sample_data()
        st.cache_data.clear()
        st.rerun()

    if st.sidebar.button("🗑️ Clear All Data"):
        collection = get_collection()
        if collection is not None:
            result = collection.delete_many({})
            st.sidebar.warning(f"Deleted {result.deleted_count} records")
            st.cache_data.clear()
            st.rerun()

    df = fetch_training_data()
    if df.empty:
        st.warning("⚠️ No data available. Click 'Insert Sample Data' in the sidebar to generate test data.")
        st.info("💡 In production, data will flow from: Metrics Collector → Kafka → Spark → MongoDB → Dashboard")
        st.stop()

    # Filters
    st.sidebar.subheader("🔍 Filters")
    all_users = ['All'] + sorted(df['user_id'].unique().tolist())
    selected_user = st.sidebar.selectbox("Select User", all_users)

    all_models = ['All'] + sorted(df['model_name'].unique().tolist())
    selected_model = st.sidebar.selectbox("Select Model", all_models)

    time_range = st.sidebar.selectbox(
        "Time Range",
        ["Last Hour", "Last 6 Hours", "Last 24 Hours", "Last Week", "All Time"]
    )

    filtered_df = df.copy()
    if selected_user != 'All':
        filtered_df = filtered_df[filtered_df['user_id'] == selected_user]
    if selected_model != 'All':
        filtered_df = filtered_df[filtered_df['model_name'] == selected_model]

    if 'timestamp' in filtered_df.columns and time_range != "All Time":
        now = datetime.now()
        deltas = {
            "Last Hour": timedelta(hours=1),
            "Last 6 Hours": timedelta(hours=6),
            "Last 24 Hours": timedelta(hours=24),
            "Last Week": timedelta(days=7)
        }
        cutoff = now - deltas[time_range]
        filtered_df = filtered_df[filtered_df['timestamp'] >= cutoff]

    # Overview
    st.subheader("📊 Overview Metrics")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("⚡ Total Energy", f"{filtered_df['energy_kwh'].sum():.2f} kWh", f"{len(filtered_df)} runs")
    with c2:
        total_em = filtered_df['emissions_kg'].sum()
        st.metric("🌍 Total CO₂ Emissions", f"{total_em:.2f} kg", f"~{total_em*2.2:.1f} lbs")
    with c3:
        avg_energy = filtered_df.groupby('run_id')['energy_kwh'].sum().mean()
        st.metric("📈 Avg Energy/Run", f"{avg_energy:.3f} kWh", "per training run")
    with c4:
        users = filtered_df['user_id'].nunique()
        st.metric("👥 Active Users", users, f"{filtered_df['run_id'].nunique()} runs")

    # Tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Time Series",
        "👥 Per User",
        "🏃 Per Run",
        "🖥️ Hardware Usage",
        "📊 Data Table"
    ])

    # Tab 1
    with tab1:
        st.subheader("Energy Consumption & Emissions Over Time")
        if 'timestamp' in filtered_df.columns:
            fig1 = px.line(filtered_df.sort_values('timestamp'),
                           x='timestamp', y='energy_kwh', color='user_id',
                           title='Energy Consumption Timeline')
            st.plotly_chart(fig1, use_container_width=True)

            fig2 = px.area(filtered_df.sort_values('timestamp'),
                           x='timestamp', y='emissions_kg', color='user_id',
                           title='CO₂ Emissions Timeline')
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.warning("Timestamp data not available")

    # Tab 2
    with tab2:
        st.subheader("Energy & Emissions by User")
        user_stats = filtered_df.groupby('user_id').agg({
            'energy_kwh': 'sum',
            'emissions_kg': 'sum',
            'run_id': 'nunique'
        }).reset_index()
        user_stats.columns = ['User', 'Total Energy (kWh)', 'Total Emissions (kg)', 'Number of Runs']
        user_stats = user_stats.sort_values('Total Energy (kWh)', ascending=False)

        col1, col2 = st.columns(2)
        with col1:
            fig_energy = px.bar(user_stats, x='User', y='Total Energy (kWh)',
                                color='User', title='Total Energy Consumption by User')
            st.plotly_chart(fig_energy, use_container_width=True)
        with col2:
            fig_em = px.bar(user_stats, x='User', y='Total Emissions (kg)',
                            color='User', title='Total CO₂ Emissions by User')
            st.plotly_chart(fig_em, use_container_width=True)

    # Tab 3
    with tab3:
        st.subheader("Energy & Accuracy per Run")
        run_stats = filtered_df.groupby('run_id').agg({
            'energy_kwh': 'sum',
            'accuracy': 'mean',
            'loss': 'mean'
        }).reset_index()
        fig_run = px.scatter(run_stats, x='energy_kwh', y='accuracy',
                             size='loss', title='Accuracy vs Energy per Run',
                             labels={'energy_kwh': 'Energy (kWh)', 'accuracy': 'Accuracy'})
        st.plotly_chart(fig_run, use_container_width=True)

    # Tab 4
    with tab4:
        st.subheader("Hardware Utilization Trends")
        fig_cpu = px.line(filtered_df, x='timestamp', y='cpu_utilization_pct', color='user_id',
                          title='CPU Utilization (%)')
        fig_gpu = px.line(filtered_df, x='timestamp', y='gpu_power_w', color='user_id',
                          title='GPU Power (W)')
        st.plotly_chart(fig_cpu, use_container_width=True)
        st.plotly_chart(fig_gpu, use_container_width=True)

    # Tab 5
    with tab5:
        st.subheader("Raw Data Table")
        st.dataframe(filtered_df)


if __name__ == "__main__":
    main()
