# Pipeline for Energy and Emission Monitoring During AI Model Training

## Overview
This project aims to build a Big Data pipeline that monitors energy consumption and CO₂ emissions during AI model training. 
Instead of using external tools such as CodeCarbon, we design a custom, distributed monitoring system that collects, processes, 
and stores relevant hardware and runtime metrics in real-time.

The project aligns with the Big Data Storage and Processing course objectives — implementing a complete end-to-end data pipeline 
using Apache Kafka, Apache Spark, and a NoSQL database under a Kappa Architecture.

## Problem Definition
AI model training is computationally intensive, and energy costs are often invisible. 
Currently, there is no unified, transparent system that tracks how much energy (and resulting emissions) is used per training run, per user, or per model.

This project addresses that gap by creating a scalable data pipeline that:
- Continuously collects low-level system metrics (CPU, GPU, RAM, etc.)
- Calculates energy and CO₂ consumption in near real time
- Stores and visualizes this information for sustainability tracking and optimization

## Objectives
- Design a Kappa-based streaming pipeline for energy/emission monitoring.
- Collect relevant system, model, and user metrics during AI training.
- Process and aggregate data using Spark Structured Streaming.
- Store results in NoSQL and distributed storage.
- Visualize emissions and training efficiency across runs and users.

## Architecture Overview (Kappa Model)
```
 ┌───────────────────────────────────────────────────────┐
 │                   Metrics Collector                   │
 │  (psutil, pynvml, system data → JSON messages)        │
 └───────────────────────────────────────────────────────┘
                      │
                      ▼
 ┌───────────────────────────────────────────────────────┐
 │                Apache Kafka (Ingestion)               │
 │  Receives metric events from multiple training runs   │
 └───────────────────────────────────────────────────────┘
                      │
                      ▼
 ┌───────────────────────────────────────────────────────┐
 │         Spark Structured Streaming (Processing)       │
 │  - Parse JSON messages                                │
 │  - Aggregate energy/CO₂ over time and users           │
 │  - Join with reference data (carbon intensity)        │
 └───────────────────────────────────────────────────────┘
                      │
                      ▼
 ┌───────────────────────────────────────────────────────┐
 │            NoSQL Database (e.g., MongoDB)             │
 │  - Stores processed metrics for visualization         │
 └───────────────────────────────────────────────────────┘
                      │
                      ▼
 ┌───────────────────────────────────────────────────────┐
 │          Visualization Dashboard (Streamlit)          │
 │  - Displays CO₂ emissions per run, per user           │
 │  - Trends, energy efficiency, hotspots                │
 └───────────────────────────────────────────────────────┘
```

## What We Track — and Why
The following metrics are collected to understand hardware usage, energy draw, and contextual training details.

| Category | Example Metrics | Purpose |
|-----------|----------------|----------|
| CPU | Utilization %, Power (W), Core count | Estimate power draw from processor activity |
| GPU | Utilization %, Power (W), Memory usage | Measure major contributor to energy use |
| RAM | Used / Total memory | Understand memory load impact |
| IO / Network | Disk read/write, network traffic | Capture additional resource costs |
| System Info | Hostname, OS, Region | Context for grid intensity & configuration |
| Training Context | Run ID, User ID, Model name, Epoch | Enable per-run and per-user aggregation |
| CO₂ Intensity | g CO₂ / kWh (by region) | Convert energy to emissions |
| Timestamps | UTC time | Enable time series analysis |

## Example of Collected Data (JSON Event)
```json
{
  "timestamp": "2025-10-14T14:25:23Z",
  "run_id": "run_001",
  "user_id": "alice",
  "model_name": "resnet18",
  "cpu_utilization_pct": 73.4,
  "gpu_power_w": 142.3,
  "gpu_mem_used_mb": 2104,
  "ram_used_mb": 8650,
  "net_sent_mb": 1.4,
  "net_recv_mb": 0.8,
  "region_iso": "DE",
  "grid_carbon_intensity_g_per_kwh": 401
}
```

## Data Flow & Processing
1. **Metrics Collector**  
   A lightweight Python service collects local system stats using psutil and pynvml.  
   Each record is serialized as JSON and sent to a Kafka topic (`training.metrics`).

2. **Kafka (Ingestion Layer)**  
   Acts as a distributed buffer for metric streams coming from multiple machines or users.

3. **Spark Structured Streaming**  
   - Reads metric messages from Kafka  
   - Performs transformations: windowed aggregations, joins, and custom UDFs  
   - Calculates:
     ```python
     energy_kwh = (gpu_power_w + cpu_power_w) * delta_t / 3_600_000
     emissions_kg = energy_kwh * grid_carbon_intensity_g_per_kwh / 1000
     ```
   - Stores rolling aggregates per user and model.

4. **Storage Layer**  
   - MongoDB (Hot Data): For latest metrics and visualization.  
   - HDFS (Cold Data): For long-term retention and analytics.

5. **Visualization Layer**  
   - Streamlit dashboard displaying:
     - Energy & CO₂ over time  
     - Top energy-consuming models/users  
     - Regional carbon impact comparison  

## Data Schema (Spark)
| Column | Type | Description |
|--------|------|-------------|
| timestamp | Timestamp | Measurement time |
| run_id | String | Training run identifier |
| user_id | String | User who started training |
| cpu_utilization_pct | Double | CPU load |
| gpu_power_w | Double | GPU energy draw |
| ram_used_mb | Double | RAM usage |
| energy_kwh | Double | Estimated energy used |
| emissions_kg | Double | Estimated CO₂ emissions |
| region_iso | String | Country code |
| model_name | String | Model under training |

## Technology Stack
| Layer | Technology | Purpose |
|-------|-------------|----------|
| Ingestion | Apache Kafka | Distributed message queue |
| Processing | Apache Spark (PySpark) | Real-time stream processing |
| Storage | MongoDB / HDFS | Hot and cold data storage |
| Visualization | Streamlit | Dashboard for emissions & energy |
| Monitoring | Prometheus (optional) | System metrics tracking |
| Deployment | Kubernetes | Container orchestration |

## Current Progress
- Defined project concept and data model  
- Designed Kappa architecture  
- Drafted metrics collector (Python prototype)  
- Next steps: Implement Kafka ingestion, Spark consumer, and visualization

## Next Steps
- Implement Kafka Producer for metric streaming  
- Develop Spark Structured Streaming job for real-time aggregation  
- Connect MongoDB for fast retrieval  
- Build Streamlit dashboard for visualization  
- Evaluate scalability with multiple concurrent users and training runs  

## References
- CodeCarbon GitHub Repository: https://github.com/mlco2/codecarbon  
- Apache Spark Structured Streaming Guide: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html  
- ElectricityMap Carbon Intensity API: https://api.electricitymap.org/
