# Layout

## Tab 1: Dashboard (System Overview)

**Goal:** Immediate health check and high-level metrics.

- **Top Pane:** Cluster Status
  - **Source:** sql/worker/cluster-capacity-summary.sql
  - **Visual:** Big number cards or Gauges.
  - **Data:** Active Workers, Cluster Utilization %, Total Capacity, Avg CPU/Mem.
- **Middle Left Pane:** Task Status Distribution
  - **Source:** sql/tasks/task-status-view.sql
  - **Visual:** Bar Chart or Summary Table.
  - **Data:** Counts for Pending, Claimed, Running, Completed, Failed, etc.
- **Middle Right Pane:** Cluster Trend (1 Hour)
  - **Source:** sql/worker/cluster-utilization-trend.sql
  - **Visual:** Line Chart.
  - **Data:** Cluster utilization % over the last hour.
- **Bottom Pane:** Active Alerts
  - **Source:** sql/worker/overloaded-workers.sql & sql/worker/stale-claims-detection.sql
  - **Visual:** Alert List (Red/Yellow text).
  - **Data:** List of workers with High CPU/Mem or Stale Claims.

## Tab 2: Workers (Detailed Monitor)

**Goal:** Monitor individual worker instances, health, and configuration.

- **Left Pane:** Worker List (Selectable)
  - **Source:** sql/worker/active-workers.sql (or latest-worker-snapshot.sql)
  - **Visual:** Scrollable Table.
  - **Data:** Worker ID, Host, PID, CPU%, Mem%, Status.
- **Right Pane:** Details (Updates on Selection)
  - **Header:** Worker Identity & Uptime
    - **Source:** sql/worker/worker-uptime.sql & sql/worker/worker-queues.sql
    - **Data:** Uptime duration, Queues served, Priorities.
  - **Charts:** Recent Load (5 min)
    - **Source:** sql/worker/worker-load-5min.sql
    - **Visual:** Sparklines or small Line Charts.
    - **Data:** Tasks Running vs Claimed, CPU/Mem history.
- **Sub-Section / Toggle:** Dead Workers
  - **Source:** sql/worker/dead-workers.sql
  - **Visual:** Table (filtered view).

## Tab 3: Tasks (Breakdown & Analytics)

**Goal:** Deep dive into how tasks are distributed across the workforce.

- **Main Pane:** Worker Task Aggregation
  - **Source:** sql/tasks/aggregated-breakdown.sql
  - **Visual:** Comprehensive Data Grid.
  - **Data:** Rows per worker showing counts for Total, Pending, Claimed, Running, Completed, Failed.
  - **Detail:** Could potentially show claimed_task_ids or running_task_ids in a footer or modal when a row is focused.

## Tab 4: Maintenance (Optional/Background)

- **Source:** sql/worker/snapshot-age-distribution.sql
  - **Visual:** Histogram.
  - **Data:** Distribution of snapshot ages (useful for debugging retention).
  