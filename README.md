# Pipeline for Energy and Emission Monitoring During AI Model Training

## Overview
This project implements an end-to-end data pipeline for collecting, processing, and visualizing energy consumption and emission metrics generated during machine learning training runs.

The pipeline simulates ML training workloads, streams metrics in realtime via kafka, processes them using Apache Spark Structured Streaming, stores results in MongoDB, and visualizes them in a Streamlit dashboard running on kubernetes.


## Problem Definition
AI model training is computationally intensive, and energy costs are often invisible. 
Currently, there is no unified, transparent system that tracks how much energy (and resulting emissions) is used per training run, per user, or per model.

This project addresses that gap by creating a scalable data pipeline that:
- Continuously collects low-level system metrics (CPU, GPU, RAM, etc.)
- Calculates energy and CO₂ consumption in near real time
- Stores and visualizes this information for sustainability tracking and optimization


## Architecture

The project follows an event-driven, stream-processing architecture deployed on Kubernetes.

### High-Level Architecture

Training Job
  -> Kafka (training.events topic)
    -> Spark Structured Streaming
      -> MongoDB
        -> Dashboard (Streamlit)

### Component Overview

Training Job:
- Simulates a machine learning training process
- Periodically emits training metrics and energy/emission data
- Publishes all events to a single Kafka topic (training.events)

Kafka:
- Acts as the central event bus
- Decouples metric production from processing
- Enables scalable and fault-tolerant data ingestion

Spark Structured Streaming:
- Consumes events from Kafka
- Parses and validates JSON data
- Applies event-time processing with watermarks
- Performs window-based and stateful aggregations
- Writes raw and aggregated results to MongoDB

MongoDB:
- Stores processed data in three collections:
  - training_metrics (raw events)
  - training_run_summary (per-run summaries)
  - aggregated_metrics (window-based aggregates)

Dashboard (Streamlit):
- Reads data from MongoDB
- Visualizes training metrics and aggregated statistics
- Provides real-time insight into energy and emission behavior


## Data Description

The pipeline processes metrics generated during machine learning training runs.

### Collected Data

The following data is collected and processed:

Training Metadata:
- run_id: Unique identifier for a training run
- user_id: Identifier of the user who started the training
- model_name: Name or type of the trained model
- dataset_name: Dataset used for training
- framework: ML framework used (e.g. sklearn)
- environment: Execution environment (e.g. local, cluster)
- region_iso: Geographic region for emission estimation

Training Metrics:
- epoch: Current training epoch
- step: Training step within an epoch
- loss: Training loss value
- accuracy: Training accuracy value

Energy and Emission Metrics:
- energy_kwh: Energy consumption per interval
- emissions_kg: CO2-equivalent emissions per interval
- cumulative_energy_kwh: Total energy consumed during the run
- cumulative_emissions_kg: Total emissions produced during the run

Timestamps:
- timestamp: Event creation time (event-time semantics)

### Why This Data Is Collected

- Training metrics allow evaluation of model performance
- Energy and emission metrics enable analysis of environmental impact
- Aggregations provide insights into efficiency over time
- Storing raw and aggregated data supports both real-time monitoring and historical analysis

### Data Storage

- Raw events are stored in MongoDB for traceability
- Aggregated data supports efficient visualization and analysis
- The schema is extensible to support additional metrics in the future



## How to Run the Project

All components of the pipeline run inside a Kubernetes cluster.

### Prerequisites

- Kubernetes cluster (e.g. Docker Desktop with Kubernetes enabled)
- kubectl configured to access the cluster
- Sufficient resources (recommended: at least 6 GB RAM)

Set the namespace:

NS=bigdata-pipeline

---

### Step 1: Deploy Infrastructure Components

Deploy ZooKeeper, Kafka, MongoDB, shared configuration, and the dashboard:

kubectl apply -n $NS -f k8s/zookeeper.yaml
kubectl apply -n $NS -f k8s/kafka.yaml
kubectl apply -n $NS -f k8s/mongodb.yaml
kubectl apply -n $NS -f k8s/configmap.yaml
kubectl apply -n $NS -f k8s/dashboard.yaml

Wait until all deployments are ready:

kubectl rollout status deployment/zookeeper -n $NS
kubectl rollout status deployment/kafka -n $NS
kubectl rollout status deployment/mongodb -n $NS
kubectl rollout status deployment/dashboard -n $NS

---

### Step 2: Ensure Kafka Topic Exists

The pipeline uses a single Kafka topic named training.events.

Create the topic (idempotent, safe to run multiple times):

kubectl run -n $NS kafka-client --restart=Never --rm -it \
  --image=confluentinc/cp-kafka:7.6.1 -- \
  bash -lc "kafka-topics --bootstrap-server kafka-svc:9092 \
    --create --if-not-exists \
    --topic training.events \
    --partitions 1 \
    --replication-factor 1"

Verify that the topic exists:

kubectl run -n $NS kafka-client --restart=Never --rm -it \
  --image=confluentinc/cp-kafka:7.6.1 -- \
  bash -lc "kafka-topics --bootstrap-server kafka-svc:9092 --list"

---

### Step 3: Start the Spark Streaming Job

kubectl delete job energy-streaming-job -n $NS --ignore-not-found
kubectl apply -n $NS -f k8s/spark-streaming-job.yaml

Check Spark logs:

kubectl logs -n $NS -l app=energy-streaming-job --tail=200

---

### Step 4: Start the Training Job

kubectl delete job training-run -n $NS --ignore-not-found
kubectl apply -n $NS -f k8s/train-job.yaml

Inspect training logs:

kubectl logs -n $NS -l app=trainer --tail=200

---

### Step 5: Verify Data in MongoDB

MONGO_POD=$(kubectl get pod -n $NS -l app=mongodb -o jsonpath='{.items[0].metadata.name}')

kubectl exec -n $NS -it $MONGO_POD -- mongosh --quiet --eval "
db=db.getSiblingDB('energy_emissions');
print('training_metrics:', db.training_metrics.countDocuments());
print('training_run_summary:', db.training_run_summary.countDocuments());
print('aggregated_metrics:', db.aggregated_metrics.countDocuments());
"

---

### Step 6: Access the Dashboard

kubectl port-forward -n $NS svc/dashboard 8501:8501

Open the dashboard in your browser:

http://localhost:8501

