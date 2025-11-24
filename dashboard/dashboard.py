"""
Energy and Emission Monitoring Dashboard
Displays real-time energy consumption and CO₂ emissions during AI model training
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(
    page_title="Energy & Emission Monitor",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
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
""",
    unsafe_allow_html=True,
)


# -----------------------------
# MongoDB helpers
# -----------------------------
@st.cache_resource
def get_mongodb_client():
    """Connect to MongoDB (inside Docker network: host 'mongodb')."""
    try:
        client = MongoClient("mongodb://mongodb:27017/", serverSelectionTimeoutMS=5000)
        client.server_info()
        return client
    except Exception as e:
        st.error(f"❌ MongoDB connection failed: {e}")
        return None


def get_metrics_collection():
    client = get_mongodb_client()
    if client:
        db = client["energy_emissions"]
        return db["training_metrics"]
    return None


def get_run_summary_collection():
    client = get_mongodb_client()
    if client:
        db = client["energy_emissions"]
        return db["run_summaries"]
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
            df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception as e:
        st.error(f"Error fetching training metrics: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=10)
def fetch_run_summaries():
    """Fetch per-run summaries from MongoDB."""
    collection = get_run_summary_collection()
    if collection is None:
        return pd.DataFrame()

    try:
        data = list(collection.find({}, {"_id": 0}))
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data)
        if "start_time" in df.columns:
            df["start_time"] = pd.to_datetime(df["start_time"])
        if "end_time" in df.columns:
            df["end_time"] = pd.to_datetime(df["end_time"])
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
            "Pipeline: Collector → Kafka → Tracker → MongoDB → Dashboard."
        )
        st.stop()

    # Initialize filtered_df
    filtered_df = df.copy() if not df.empty else pd.DataFrame()

    # Filters based on metrics dataframe
    if not df.empty:
        st.sidebar.subheader("🔍 Filters")

        # User filter
        if "user_id" in df.columns:
            all_users = ["All"] + sorted(df["user_id"].dropna().unique().tolist())
            selected_user = st.sidebar.selectbox("Select User", all_users)
            if selected_user != "All":
                filtered_df = filtered_df[filtered_df["user_id"] == selected_user]

        # Model filter
        if "model_name" in df.columns:
            all_models = ["All"] + sorted(df["model_name"].dropna().unique().tolist())
            selected_model = st.sidebar.selectbox("Select Model", all_models)
            if selected_model != "All":
                filtered_df = filtered_df[filtered_df["model_name"] == selected_model]

        # Dataset filter
        if "dataset_name" in df.columns:
            all_datasets = ["All"] + sorted(df["dataset_name"].dropna().unique().tolist())
            selected_dataset = st.sidebar.selectbox("Select Dataset", all_datasets)
            if selected_dataset != "All":
                filtered_df = filtered_df[filtered_df["dataset_name"] == selected_dataset]

        # Environment filter (dev/staging/prod/local)
        if "environment" in df.columns:
            all_envs = ["All"] + sorted(df["environment"].dropna().unique().tolist())
            selected_env = st.sidebar.selectbox("Select Environment", all_envs)
            if selected_env != "All":
                filtered_df = filtered_df[filtered_df["environment"] == selected_env]

        # Region filter
        if "region_iso" in df.columns:
            all_regions = ["All"] + sorted(df["region_iso"].dropna().unique().tolist())
            selected_region = st.sidebar.selectbox("Select Region", all_regions)
            if selected_region != "All":
                filtered_df = filtered_df[filtered_df["region_iso"] == selected_region]

        # Time range filter
        time_range = st.sidebar.selectbox(
            "Time Range",
            ["Last Hour", "Last 6 Hours", "Last 24 Hours", "Last Week", "All Time"],
        )

        if "timestamp" in filtered_df.columns and time_range != "All Time":
            now = datetime.now()
            deltas = {
                "Last Hour": timedelta(hours=1),
                "Last 6 Hours": timedelta(hours=6),
                "Last 24 Hours": timedelta(hours=24),
                "Last Week": timedelta(days=7),
            }
            cutoff = now - deltas[time_range]
            filtered_df = filtered_df[filtered_df["timestamp"] >= cutoff]

    # Overview metrics (based on filtered_df if available)
    if not df.empty and not filtered_df.empty and "energy_kwh" in filtered_df.columns:
        st.subheader("📊 Overview Metrics")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric(
                "⚡ Total Energy",
                f"{filtered_df['energy_kwh'].sum():.4f} kWh",
                f"{len(filtered_df)} samples",
            )
        with c2:
            total_em = filtered_df["emissions_kg"].sum() if "emissions_kg" in filtered_df.columns else 0.0
            st.metric(
                "🌍 Total CO₂ Emissions",
                f"{total_em:.4f} kg",
                f"~{total_em * 2.2:.1f} lbs",
            )
        with c3:
            if "run_id" in filtered_df.columns:
                avg_energy_per_run = (
                    filtered_df.groupby("run_id")["energy_kwh"].sum().mean()
                    if not filtered_df.empty
                    else 0.0
                )
            else:
                avg_energy_per_run = 0.0
            st.metric(
                "📈 Avg Energy/Run",
                f"{avg_energy_per_run:.4f} kWh",
                "per training run",
            )
        with c4:
            users = filtered_df["user_id"].nunique() if "user_id" in filtered_df.columns else 0
            runs = filtered_df["run_id"].nunique() if "run_id" in filtered_df.columns else 0
            st.metric("👥 Active Users", users, f"{runs} runs")

    # Tabs
    tab_labels = [
        "📈 Time Series",
        "👥 Per User",
        "🏃 Per Run",
        "🖥️ Hardware Usage",
        "📊 Raw Data",
        "🏁 Run Overview",
    ]
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(tab_labels)

    # Tab 1: Time series (including cumulative energy)
    with tab1:
        st.subheader("Energy Consumption & Emissions Over Time")
        if df.empty or filtered_df.empty:
            st.info("No metrics available yet.")
        else:
            if "timestamp" in filtered_df.columns:
                sorted_df = filtered_df.sort_values("timestamp")

                # Energy per interval
                if "energy_kwh" in sorted_df.columns:
                    fig1 = px.line(
                        sorted_df,
                        x="timestamp",
                        y="energy_kwh",
                        color="user_id" if "user_id" in sorted_df.columns else None,
                        title="Energy Consumption per Interval (kWh)",
                    )
                    st.plotly_chart(fig1, use_container_width=True)

                # Emissions per interval
                if "emissions_kg" in sorted_df.columns:
                    fig2 = px.area(
                        sorted_df,
                        x="timestamp",
                        y="emissions_kg",
                        color="user_id" if "user_id" in sorted_df.columns else None,
                        title="CO₂ Emissions per Interval (kg)",
                    )
                    st.plotly_chart(fig2, use_container_width=True)

                # Cumulative energy over time per run
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

    # Tab 2: Per user
    with tab2:
        st.subheader("Energy & Emissions by User")
        if df.empty or filtered_df.empty:
            st.info("No metrics available yet.")
        else:
            if "user_id" in filtered_df.columns and "energy_kwh" in filtered_df.columns:
                agg_cols = {
                    "energy_kwh": "sum",
                }
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
                            labels={
                                "total_energy_kwh": "Energy (kWh)",
                                "user_id": "User",
                            },
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
                                labels={
                                    "total_emissions_kg": "Emissions (kg)",
                                    "user_id": "User",
                                },
                            )
                            st.plotly_chart(fig_em, use_container_width=True)
                        else:
                            st.info("Emissions are not recorded in metrics.")
            else:
                st.info("User and energy columns are not available in metrics.")

    # Tab 3: Per run (scatter accuracy vs energy)
    with tab3:
        st.subheader("Energy & Accuracy per Run")
        if df.empty or filtered_df.empty:
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

    # Tab 4: Hardware usage
    with tab4:
        st.subheader("Hardware Utilization Trends")
        if df.empty or filtered_df.empty:
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

    # Tab 5: Raw data + CSV export
    with tab5:
        st.subheader("Raw Metrics Table")
        if df.empty or filtered_df.empty:
            st.info("No metrics available yet.")
        else:
            st.dataframe(filtered_df)

            # CSV export
            csv_metrics = filtered_df.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="📥 Download filtered metrics as CSV",
                data=csv_metrics,
                file_name="training_metrics_filtered.csv",
                mime="text/csv",
            )

    # Tab 6: Run overview + compare + CSV export
    with tab6:
        st.subheader("Run Overview (Summaries)")
        if runs_df.empty:
            st.info("No run summaries available yet.")
        else:
            show_cols = [
                "run_id",
                "user_id",
                "model_name",
                "dataset_name",
                "framework",
                "environment",
                "region_iso",
                "status",
                "total_energy_kwh",
                "total_emissions_kg",
                "start_time",
                "end_time",
                "duration_seconds",
            ]
            existing_cols = [c for c in show_cols if c in runs_df.columns]
            st.dataframe(
                runs_df[existing_cols].sort_values(
                    "start_time", ascending=False
                ),
                use_container_width=True,
            )

            # CSV export for summaries
            csv_summaries = runs_df[existing_cols].to_csv(index=False).encode("utf-8")
            st.download_button(
                label="📥 Download run summaries as CSV",
                data=csv_summaries,
                file_name="run_summaries.csv",
                mime="text/csv",
            )

            st.markdown("### 🆚 Compare Runs")

            if "run_id" in runs_df.columns:
                run_ids = runs_df["run_id"].unique().tolist()
                default_selection = run_ids[:2] if len(run_ids) >= 2 else run_ids[:1]
                selected_runs = st.multiselect(
                    "Select runs to compare",
                    options=run_ids,
                    default=default_selection,
                )

                if selected_runs:
                    cmp_df = runs_df[runs_df["run_id"].isin(selected_runs)].copy()

                    cmp_cols = [
                        "run_id",
                        "user_id",
                        "model_name",
                        "dataset_name",
                        "framework",
                        "environment",
                        "region_iso",
                        "status",
                        "total_energy_kwh",
                        "total_emissions_kg",
                        "duration_seconds",
                    ]
                    cmp_existing = [c for c in cmp_cols if c in cmp_df.columns]
                    st.dataframe(cmp_df[cmp_existing], use_container_width=True)

                    if "total_energy_kwh" in cmp_df.columns:
                        fig_cmp_energy = px.bar(
                            cmp_df,
                            x="run_id",
                            y="total_energy_kwh",
                            color="status",
                            title="Total Energy per Selected Run",
                            labels={
                                "total_energy_kwh": "Energy (kWh)",
                                "run_id": "Run ID",
                            },
                        )
                        st.plotly_chart(fig_cmp_energy, use_container_width=True)

                    if "total_emissions_kg" in cmp_df.columns:
                        fig_cmp_em = px.bar(
                            cmp_df,
                            x="run_id",
                            y="total_emissions_kg",
                            color="status",
                            title="Total CO₂ Emissions per Selected Run",
                            labels={
                                "total_emissions_kg": "Emissions (kg)",
                                "run_id": "Run ID",
                            },
                        )
                        st.plotly_chart(fig_cmp_em, use_container_width=True)
            else:
                st.info("Run IDs are not available in summaries.")


if __name__ == "__main__":
    main()