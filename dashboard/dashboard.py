"""
Energy and Emission Monitoring Dashboard
Displays real-time energy consumption and CO₂ emissions during AI model training
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone


# -----------------------------
# Page configuration
# -----------------------------
st.set_page_config(
    page_title="Energy & Emission Monitor",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -----------------------------
# Custom CSS
# -----------------------------
st.markdown(
    """
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 8px;
    }
</style>
""",
    unsafe_allow_html=True,
)

# -----------------------------
# Environment config (K8s ConfigMap friendly)
# -----------------------------
DEFAULT_MONGO_URI = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
DEFAULT_MONGO_DB = os.getenv("MONGODB_DB", "energy_emissions")

# Raw metrics (what dashboard uses for plots)
DEFAULT_METRICS_COLLECTION = os.getenv("MONGODB_METRICS_COLLECTION", "training_metrics")

# Run summaries (optional, if you have a component writing those)
DEFAULT_RUN_SUMMARY_COLLECTION = os.getenv("MONGODB_RUN_SUMMARY_COLLECTION", "run_summaries")


# -----------------------------
# MongoDB helpers
# -----------------------------
@st.cache_resource
def get_mongodb_client():
    """Connect to MongoDB (inside Docker network: host 'mongodb')."""
    try:
        client = MongoClient(DEFAULT_MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info()
        return client
    except Exception as e:
        st.error(f"❌ MongoDB connection failed: {e}")
        return None


def get_metrics_collection():
    client = get_mongodb_client()
    if client:
        db = client[DEFAULT_MONGO_DB]
        return db[DEFAULT_METRICS_COLLECTION]
    return None


def get_run_summary_collection():
    client = get_mongodb_client()
    if client:
        db = client[DEFAULT_MONGO_DB]
        return db[DEFAULT_RUN_SUMMARY_COLLECTION]
    return None


@st.cache_data(ttl=5)
def fetch_training_data():
    """Fetch all training metrics from MongoDB."""
    collection = get_metrics_collection()
    if collection is None:
        return pd.DataFrame()

    try:
        data = list(collection.find({}, {"_id": 0}))
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df
    except Exception as e:
        st.error(f"Error fetching training metrics: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=10)
def fetch_run_summaries():
    """Fetch per-run summaries from MongoDB (optional)."""
    collection = get_run_summary_collection()
    if collection is None:
        return pd.DataFrame()

    try:
        data = list(collection.find({}, {"_id": 0}))
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if "start_time" in df.columns:
            df["start_time"] = pd.to_datetime(df["start_time"], utc=True)
        if "end_time" in df.columns:
            df["end_time"] = pd.to_datetime(df["end_time"], utc=True)
        return df
    except Exception as e:
        st.error(f"Error fetching run summaries: {e}")
        return pd.DataFrame()


def clear_all_data():
    """Delete all training metrics and run summaries."""
    metrics = get_metrics_collection()
    summaries = get_run_summary_collection()
    deleted_total = 0

    if metrics is not None:
        result = metrics.delete_many({})
        deleted_total += result.deleted_count

    if summaries is not None:
        result = summaries.delete_many({})
        deleted_total += result.deleted_count

    st.sidebar.warning(f"🗑️ Deleted {deleted_total} documents from MongoDB")
    st.cache_data.clear()


# -----------------------------
# Main UI
# -----------------------------
def main():
    st.markdown(
        '<div class="main-header">⚡ Energy & Emission Monitoring Dashboard</div>',
        unsafe_allow_html=True,
    )

    # Sidebar
    st.sidebar.title("🎛️ Dashboard Controls")

    client = get_mongodb_client()
    if client:
        st.sidebar.success("✅ MongoDB Connected")
        st.sidebar.caption(
            f"DB: `{DEFAULT_MONGO_DB}` | Metrics: `{DEFAULT_METRICS_COLLECTION}` | Summaries: `{DEFAULT_RUN_SUMMARY_COLLECTION}`"
        )
    else:
        st.sidebar.error("❌ MongoDB Disconnected")
        st.stop()

    st.sidebar.subheader("📊 Data Management")

    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    if st.sidebar.button("🗑️ Clear All Data"):
        clear_all_data()
        st.rerun()

    # Load data
    df = fetch_training_data()
    runs_df = fetch_run_summaries()

    if df.empty and runs_df.empty:
        st.warning("⚠️ No data available yet.")
        st.info(
            "Start a training run with MetricsCollector to generate data.\n"
            "Pipeline: Collector → Kafka → Streaming Job → MongoDB → Dashboard."
        )
        st.stop()

    filtered_df = df.copy() if not df.empty else pd.DataFrame()

    # Filters
    if not df.empty:
        st.sidebar.subheader("🔍 Filters")

        if "user_id" in df.columns:
            all_users = ["All"] + sorted(df["user_id"].dropna().unique().tolist())
            selected_user = st.sidebar.selectbox("Select User", all_users)
            if selected_user != "All":
                filtered_df = filtered_df[filtered_df["user_id"] == selected_user]

        if "model_name" in df.columns:
            all_models = ["All"] + sorted(df["model_name"].dropna().unique().tolist())
            selected_model = st.sidebar.selectbox("Select Model", all_models)
            if selected_model != "All":
                filtered_df = filtered_df[filtered_df["model_name"] == selected_model]

        if "dataset_name" in df.columns:
            all_datasets = ["All"] + sorted(df["dataset_name"].dropna().unique().tolist())
            selected_dataset = st.sidebar.selectbox("Select Dataset", all_datasets)
            if selected_dataset != "All":
                filtered_df = filtered_df[filtered_df["dataset_name"] == selected_dataset]

        if "environment" in df.columns:
            all_envs = ["All"] + sorted(df["environment"].dropna().unique().tolist())
            selected_env = st.sidebar.selectbox("Select Environment", all_envs)
            if selected_env != "All":
                filtered_df = filtered_df[filtered_df["environment"] == selected_env]

        # Your collector currently uses "region" not "region_iso" (depending on version)
        region_col = "region_iso" if "region_iso" in df.columns else ("region" if "region" in df.columns else None)
        if region_col:
            all_regions = ["All"] + sorted(df[region_col].dropna().unique().tolist())
            selected_region = st.sidebar.selectbox("Select Region", all_regions)
            if selected_region != "All":
                filtered_df = filtered_df[filtered_df[region_col] == selected_region]

        time_range = st.sidebar.selectbox(
            "Time Range",
            ["Last Hour", "Last 6 Hours", "Last 24 Hours", "Last Week", "All Time"],
        )

        if "timestamp" in filtered_df.columns and time_range != "All Time":
            now = datetime.now(timezone.utc)

            deltas = {
                "Last Hour": timedelta(hours=1),
                "Last 6 Hours": timedelta(hours=6),
                "Last 24 Hours": timedelta(hours=24),
                "Last Week": timedelta(days=7),
            }
            cutoff = now - deltas[time_range]
            filtered_df = filtered_df[filtered_df["timestamp"] >= cutoff]


    # Overview metrics
    if not df.empty and not filtered_df.empty and "energy_kwh" in filtered_df.columns:
        st.subheader("📊 Overview Metrics")
        c1, c2, c3, c4 = st.columns(4)

        with c1:
            st.metric(
                "⚡ Total Energy",
                f"{filtered_df['energy_kwh'].sum():.6f} kWh",
                f"{len(filtered_df)} samples",
            )

        with c2:
            total_em = filtered_df["emissions_kg"].sum() if "emissions_kg" in filtered_df.columns else 0.0
            st.metric(
                "🌍 Total CO₂ Emissions",
                f"{total_em:.6f} kg",
                f"~{total_em * 2.2:.2f} lbs",
            )

        with c3:
            if "run_id" in filtered_df.columns:
                avg_energy_per_run = filtered_df.groupby("run_id")["energy_kwh"].sum().mean()
            else:
                avg_energy_per_run = 0.0
            st.metric("📈 Avg Energy/Run", f"{avg_energy_per_run:.6f} kWh", "per run")

        with c4:
            users = filtered_df["user_id"].nunique() if "user_id" in filtered_df.columns else 0
            runs = filtered_df["run_id"].nunique() if "run_id" in filtered_df.columns else 0
            st.metric("👥 Active Users", users, f"{runs} runs")

    tab_labels = [
        "📈 Time Series",
        "👥 Per User",
        "🏃 Per Run",
        "🖥️ Hardware Usage",
        "📊 Raw Data",
        "🏁 Run Overview",
    ]
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(tab_labels)

    # Tab 1
    with tab1:
        st.subheader("Energy Consumption & Emissions Over Time")
        if filtered_df.empty:
            st.info("No metrics available yet.")
        else:
            if "timestamp" in filtered_df.columns:
                sorted_df = filtered_df.sort_values("timestamp")

                if "energy_kwh" in sorted_df.columns:
                    fig1 = px.line(
                        sorted_df,
                        x="timestamp",
                        y="energy_kwh",
                        color="user_id" if "user_id" in sorted_df.columns else None,
                        title="Energy Consumption per Interval (kWh)",
                    )
                    st.plotly_chart(fig1, use_container_width=True)

                if "emissions_kg" in sorted_df.columns:
                    fig2 = px.area(
                        sorted_df,
                        x="timestamp",
                        y="emissions_kg",
                        color="user_id" if "user_id" in sorted_df.columns else None,
                        title="CO₂ Emissions per Interval (kg)",
                    )
                    st.plotly_chart(fig2, use_container_width=True)

                if "energy_kwh" in sorted_df.columns:
                    cum_df = sorted_df.copy()
                    if "run_id" in cum_df.columns:
                        cum_df = cum_df.sort_values(["run_id", "timestamp"])
                        cum_df["cum_energy_kwh"] = cum_df.groupby("run_id")["energy_kwh"].cumsum()
                        color_col = "run_id"
                    else:
                        cum_df = cum_df.sort_values("timestamp")
                        cum_df["cum_energy_kwh"] = cum_df["energy_kwh"].cumsum()
                        color_col = "user_id" if "user_id" in cum_df.columns else None

                    fig3 = px.line(
                        cum_df,
                        x="timestamp",
                        y="cum_energy_kwh",
                        color=color_col,
                        title="Cumulative Energy per Run (kWh)",
                        labels={"cum_energy_kwh": "Cumulative Energy (kWh)"},
                    )
                    st.plotly_chart(fig3, use_container_width=True)
            else:
                st.warning("Timestamp column not available in metrics.")

    # Tab 2
    with tab2:
        st.subheader("Energy & Emissions by User")
        if filtered_df.empty:
            st.info("No metrics available yet.")
        else:
            if "user_id" in filtered_df.columns and "energy_kwh" in filtered_df.columns:
                agg_cols = {"energy_kwh": "sum"}
                if "emissions_kg" in filtered_df.columns:
                    agg_cols["emissions_kg"] = "sum"
                if "run_id" in filtered_df.columns:
                    agg_cols["run_id"] = "nunique"

                user_stats = (
                    filtered_df.groupby("user_id")
                    .agg(agg_cols)
                    .reset_index()
                    .rename(
                        columns={
                            "energy_kwh": "total_energy_kwh",
                            "emissions_kg": "total_emissions_kg",
                            "run_id": "runs",
                        }
                    )
                )

                if user_stats.empty:
                    st.info("No data after filters.")
                else:
                    col1, col2 = st.columns(2)
                    with col1:
                        fig_energy = px.bar(
                            user_stats,
                            x="user_id",
                            y="total_energy_kwh",
                            color="user_id",
                            title="Total Energy Consumption by User",
                            labels={"total_energy_kwh": "Energy (kWh)", "user_id": "User"},
                        )
                        st.plotly_chart(fig_energy, use_container_width=True)

                    with col2:
                        if "total_emissions_kg" in user_stats.columns:
                            fig_em = px.bar(
                                user_stats,
                                x="user_id",
                                y="total_emissions_kg",
                                color="user_id",
                                title="Total CO₂ Emissions by User",
                                labels={"total_emissions_kg": "Emissions (kg)", "user_id": "User"},
                            )
                            st.plotly_chart(fig_em, use_container_width=True)
                        else:
                            st.info("Emissions are not recorded in metrics.")
            else:
                st.info("User and energy columns are not available in metrics.")

    # Tab 3
    with tab3:
        st.subheader("Energy & Accuracy per Run")
        if filtered_df.empty:
            st.info("No metrics available yet.")
        else:
            if "run_id" in filtered_df.columns and "energy_kwh" in filtered_df.columns:
                agg_dict = {"energy_kwh": "sum"}
                if "accuracy" in filtered_df.columns:
                    agg_dict["accuracy"] = "mean"
                if "loss" in filtered_df.columns:
                    agg_dict["loss"] = "mean"

                run_stats = filtered_df.groupby("run_id").agg(agg_dict).reset_index()

                if "accuracy" in run_stats.columns:
                    fig_run = px.scatter(
                        run_stats,
                        x="energy_kwh",
                        y="accuracy",
                        size="loss" if "loss" in run_stats.columns else None,
                        title="Accuracy vs Energy per Run",
                        labels={"energy_kwh": "Energy (kWh)", "accuracy": "Accuracy"},
                    )
                    st.plotly_chart(fig_run, use_container_width=True)
                else:
                    st.info("No accuracy values recorded yet in metrics.")
            else:
                st.info("Run and energy columns are not available in metrics.")

    # Tab 4
    with tab4:
        st.subheader("Hardware Utilization Trends")
        if filtered_df.empty:
            st.info("No metrics available yet.")
        else:
            if "cpu_utilization_pct" in filtered_df.columns and "timestamp" in filtered_df.columns:
                fig_cpu = px.line(
                    filtered_df.sort_values("timestamp"),
                    x="timestamp",
                    y="cpu_utilization_pct",
                    color="user_id" if "user_id" in filtered_df.columns else None,
                    title="CPU Utilization (%)",
                )
                st.plotly_chart(fig_cpu, use_container_width=True)
            else:
                st.info("CPU utilization is not recorded in metrics.")

            if "gpu_power_w" in filtered_df.columns and "timestamp" in filtered_df.columns:
                fig_gpu = px.line(
                    filtered_df.sort_values("timestamp"),
                    x="timestamp",
                    y="gpu_power_w",
                    color="user_id" if "user_id" in filtered_df.columns else None,
                    title="GPU Power (W)",
                )
                st.plotly_chart(fig_gpu, use_container_width=True)
            else:
                st.info("GPU power is not recorded in metrics.")

    # Tab 5
    with tab5:
        st.subheader("Raw Metrics Table")
        if filtered_df.empty:
            st.info("No metrics available yet.")
        else:
            st.dataframe(filtered_df)

            csv_metrics = filtered_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="📥 Download filtered metrics as CSV",
                data=csv_metrics,
                file_name="training_metrics_filtered.csv",
                mime="text/csv",
            )

    # Tab 6
    with tab6:
        st.subheader("Run Overview (Summaries)")
        if runs_df.empty:
            st.info("No run summaries available yet.")
        else:
            st.dataframe(runs_df, use_container_width=True)


if __name__ == "__main__":
    main()
