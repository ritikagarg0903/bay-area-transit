# 🚎 Bay Area Transit Performance Monitor

### An End-to-End ELT Pipeline & Real-Time Analytics Dashboard

## 📖 Executive Summary

This project is a full-stack data engineering solution designed to monitor the operational performance of Bay Area transit agencies (via 511.org). It ingests real-time GTFS-RT streams, processes them into historical performance metrics using advanced SQL modeling, and serves a live operational dashboard.

**The Business Problem:**
Transit agencies generate millions of data points daily, but raw GTFS feeds are transient. Without a robust historical warehouse, it is impossible to answer critical questions like *"Is the 38-Geary route getting slower?"* or *"Where are the worst delays occurring right now?"*

**The Solution:**
I built an automated ELT pipeline that captures real-time streams every 20 minutes, transforms data hourly using dbt with built-in quality tests, and models actionable KPIs (Schedule Adherence, Route Efficiency, Live Delay Heatmaps).

---

## 🏗️ System Architecture

The pipeline follows a modern ELT pattern running on Google Cloud Platform.

1. **Extract (Python):** A custom script polls the 511.org API every **20 minutes** to fetch `TripUpdates` and `VehiclePositions` protobuf feeds.
2. 
**Load (Cloud Run & BigQuery):** Raw Protobuf data is parsed into JSON and appended to partitioned BigQuery tables (`raw_trip_updates`, `raw_vehicle_positions`) .


3. **Transform (dbt):**
* **Schedule:** Models are rebuilt **every 1 hour** to provide near real-time analytics.
* **Layers:** Staging (Deduplication)  Intermediate (Cleaning)  Marts (Business Logic).


4. **Visualize (Looker Studio):** A multi-page dashboard featuring historical trends and a real-time geospatial delay heatmap.

---

## 🧠 Advanced SQL & Data Cleaning

The core engineering value of this project lies in the **Data Transformation Layer**. Handling real-time transit data requires solving complex optimization and data quality issues.

### 1. Optimization: Incremental Materialization

**The Challenge:** Processing millions of raw JSON rows every hour became slow and costly as the dataset grew.
**The SQL Solution:** Converted the heavy transformation models to **Incremental Models**. This ensures dbt only processes new data that arrived since the last run, significantly reducing compute costs and latency.

```sql
{{ config(materialized='incremental', unique_key='event_key') }}
-- ...
{% if is_incremental() %}
  WHERE ingested_at > (SELECT max(ingested_at) FROM {{ this }})
{% endif %}

```

### 2. Complex Deduplication (Window Functions)

**The Challenge:** The API is polled every 20 minutes, but prediction windows overlap. A single bus arrival might be predicted 5 times with slightly different timestamps.
**The SQL Solution:** I used `ROW_NUMBER()` window functions to identify and keep only the *most recent* prediction for every unique trip-stop event, creating a clean "golden record" for analysis.

```sql
-- models/staging/stg_trip_updates.sql
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY trip_id, stop_id, event_type 
    ORDER BY ingested_at DESC, feed_timestamp DESC
) = 1

```

### 3. Geospatial Joining for Live Tracking

**The Challenge:** Vehicle position feeds contain Lat/Lon but no delay info. Trip update feeds contain Delay info but no coordinates.
**The SQL Solution:** I created a "Live Mart" that joins these two disparate streams on `trip_id`, allowing me to visualize *where* the delays are happening physically.

```sql
-- models/marts/fct_latest_vehicle_positions.sql
LEFT JOIN latest_delays d 
    ON p.trip_id = d.trip_id

```

### 4. Solving the "Midnight Crossing" Bug

**The Challenge:** Raw API data often produced impossible delay values (e.g., `-48,000 seconds`) when a bus scheduled for 23:50 arrived at 00:05. The simplistic timestamp subtraction treated this as "arriving yesterday."
**The SQL Solution:** I implemented a sanity filter in the intermediate layer to enforce realistic bounds, filtering out statistical noise before it hit the dashboard.

```sql
-- models/intermediate/int_trip_stop_events_deduped.sql
WHERE delay_sec IS NOT NULL
  AND delay_sec BETWEEN -1800 AND 14400  -- Filter: -30 mins early to +4 hours late

```

---

## 🛡️ Data Quality & Automation

To ensure reliability in a production environment, I implemented rigorous testing and scheduling protocols.

### Automated Testing (dbt Tests)

I defined schema tests (`schema.yml`) to catch data quality issues before they reached the dashboard.

* **Unique Keys:** Enforced uniqueness on surrogate keys (`event_key`) to prevent duplicate counting of passengers or delays.
* **Not Null Constraints:** Validated critical fields like `route_id` and `trip_id`. Rows failing these tests are automatically flagged or filtered to prevent dashboard crashes.
* **Accepted Values:** Ensured `event_type` only contained valid 'ARRIVAL' or 'DEPARTURE' statuses.

### Workflow Scheduling

* **Ingestion:** Python script runs every **20 minutes** via Cloud Run Scheduler to capture granular movement data.
* **Transformation:** dbt pipeline runs every **1 hour**. This cadence balances cost (compute) with freshness, ensuring the dashboard is never more than ~60 minutes behind live operations.

---

## 📊 Dashboard Overview

The output is a live Looker Studio Dashboard divided into three strategic sections:

### 1. Executive Scorecards

Tracks high-level system health.

* **Average Delay:** Tracks the weighted average delay across the network to establish a performance baseline.
* **Data Freshness:** A real-time monitor that turns **RED** if the pipeline lags by >120 minutes, building trust with users.

### 2. Historical Trends

* **Daily Delay Trend:** A line chart visualizing if performance is degrading Week-over-Week.
* **Hourly Intensity:** A pivot table heatmap identifying rush-hour bottlenecks (e.g., showing Route 20 has severe delays specifically at 5:00 PM).

### 3. Live Transit Delay Tracker (Geospatial Heatmap)

Instead of a crowded map of bus dots, I engineered a **Delay Heatmap**.

* **Visualization:** Uses a weighted heatmap based on the `current_delay_sec` column joined from the trip feed.
* **Insight:** Users can instantly see "Hot Zones" of congestion (Red Glow) across the city, rather than just seeing where buses are located.

---

## 🛠️ Tech Stack

* **Language:** Python 3.9 (Ingestion Script)
* **Orchestration:** Google Cloud Run Scheduler
* **Data Warehouse:** Google BigQuery
* **Transformation:** dbt (Data Build Tool) - *Core/Incremental/Snapshot/Testing*
* **BI Tool:** Looker Studio
* **Format:** GTFS-Realtime (Protobuf)

---

## 🚀 Future Improvements

* **Orchestration:** Migrate from Cloud Scheduler to **Airflow** or **Dagster** for better dependency graphs and failure retries.
* **Data Quality:** Implement **Great Expectations** to catch schema drifts from the 511.org API automatically.
* **Speed Analysis:** Use BigQuery GIS functions to calculate the average speed between specific stop segments to identify slow street corridors.