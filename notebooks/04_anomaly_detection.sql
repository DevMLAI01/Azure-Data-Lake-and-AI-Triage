-- ===========================================================================
-- 04_anomaly_detection.sql
-- Phase 8 — Anomaly Detection: Z-score on Gold tables
--
-- Run in Databricks SQL Editor.
-- Save as a named Query, then set up a SQL Alert on it.
--
-- Detects:
--   1. Null rate spikes in CDR (null_rate_duration)
--   2. Call setup success rate drops in KPI
-- ===========================================================================

-- NOTE: Replace 'telecon_dev' below with your Unity Catalog catalog name.

-- ===========================================================================
-- Query 1: CDR null rate anomaly (Z-score per cell)
-- Save this as: "CDR Null Rate Anomaly Detection"
-- ===========================================================================

WITH baseline AS (
    -- 30-day rolling baseline per cell (excluding the latest day)
    SELECT
        cell_tower_id,
        AVG(null_rate_duration)    AS mean_null_rate,
        STDDEV(null_rate_duration) AS stddev_null_rate,
        AVG(total_calls)           AS mean_total_calls,
        STDDEV(total_calls)        AS stddev_total_calls
    FROM telecon_dev.gold.daily_network_health  -- TODO: replace catalog name
    WHERE event_date < CURRENT_DATE
    GROUP BY cell_tower_id
    HAVING COUNT(*) >= 3           -- need at least 3 days for meaningful baseline
),
latest AS (
    -- Most recent day's metrics
    SELECT *
    FROM telecon_dev.gold.daily_network_health  -- TODO: replace catalog name
    WHERE event_date = (SELECT MAX(event_date) FROM telecon_dev.gold.daily_network_health)
)
SELECT
    l.event_date,
    l.cell_tower_id,
    l.null_rate_duration                                          AS observed_null_rate,
    ROUND(b.mean_null_rate, 4)                                    AS baseline_mean,
    ROUND(b.stddev_null_rate, 4)                                  AS baseline_stddev,
    ROUND(
        (l.null_rate_duration - b.mean_null_rate)
        / NULLIF(b.stddev_null_rate, 0),
    2)                                                            AS z_score,
    CASE
        WHEN ABS((l.null_rate_duration - b.mean_null_rate)
             / NULLIF(b.stddev_null_rate, 0)) >= 5  THEN 'P1'
        WHEN ABS((l.null_rate_duration - b.mean_null_rate)
             / NULLIF(b.stddev_null_rate, 0)) >= 3  THEN 'P2'
        WHEN ABS((l.null_rate_duration - b.mean_null_rate)
             / NULLIF(b.stddev_null_rate, 0)) >= 2  THEN 'P3'
        ELSE 'NORMAL'
    END                                                           AS severity,
    l.total_calls,
    l.drop_rate_pct
FROM latest l
JOIN baseline b ON l.cell_tower_id = b.cell_tower_id
WHERE
    ABS(
        (l.null_rate_duration - b.mean_null_rate)
        / NULLIF(b.stddev_null_rate, 0)
    ) >= 2                         -- only return anomalous rows
ORDER BY z_score DESC;


-- ===========================================================================
-- Query 2: KPI call setup success rate drop
-- Save this as: "KPI Call Setup Rate Anomaly"
-- ===========================================================================

/*
WITH kpi_baseline AS (
    SELECT
        cell_id,
        technology,
        AVG(avg_call_setup_success_rate)    AS mean_cssr,
        STDDEV(avg_call_setup_success_rate) AS stddev_cssr
    FROM telecon_dev.gold.kpi_daily_performance  -- TODO: replace catalog name
    WHERE event_date < CURRENT_DATE
    GROUP BY cell_id, technology
    HAVING COUNT(*) >= 3
),
kpi_latest AS (
    SELECT *
    FROM telecon_dev.gold.kpi_daily_performance
    WHERE event_date = (SELECT MAX(event_date) FROM telecon_dev.gold.kpi_daily_performance)  -- TODO: replace catalog name
)
SELECT
    l.event_date,
    l.cell_id,
    l.technology,
    l.avg_call_setup_success_rate                                  AS observed_cssr,
    ROUND(b.mean_cssr, 4)                                          AS baseline_mean,
    ROUND(
        (l.avg_call_setup_success_rate - b.mean_cssr)
        / NULLIF(b.stddev_cssr, 0),
    2)                                                             AS z_score,
    CASE
        WHEN (l.avg_call_setup_success_rate - b.mean_cssr)
             / NULLIF(b.stddev_cssr, 0) <= -5  THEN 'P1'
        WHEN (l.avg_call_setup_success_rate - b.mean_cssr)
             / NULLIF(b.stddev_cssr, 0) <= -3  THEN 'P2'
        ELSE 'P3'
    END                                                            AS severity,
    l.avg_dl_throughput_mbps,
    l.peak_connected_users
FROM kpi_latest l
JOIN kpi_baseline b ON l.cell_id = b.cell_id AND l.technology = b.technology
WHERE
    (l.avg_call_setup_success_rate - b.mean_cssr)
    / NULLIF(b.stddev_cssr, 0) <= -2
ORDER BY z_score ASC;
*/

-- ===========================================================================
-- Alert setup instructions (do this in Databricks SQL UI):
--
-- 1. Run "CDR Null Rate Anomaly Detection" query above
-- 2. Click "Save" → name it "CDR Null Rate Anomaly Detection"
-- 3. Click "Alerts" in left nav → "+ New Alert"
--    Query:     CDR Null Rate Anomaly Detection
--    Trigger:   Value of [severity] IS NOT NULL  (any row returned = alert)
--    Schedule:  Daily at 08:30 UTC
--    Notify:    Email → your email address
--               (add Webhook → Azure Function URL after Phase 9)
-- ===========================================================================
