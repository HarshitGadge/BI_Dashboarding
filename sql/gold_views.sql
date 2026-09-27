-- Gold layer: dashboard-ready views over dashboard_data (one row per ticket).
-- Metric definitions: docs/metrics.md

CREATE OR REPLACE TEMP VIEW kpi_summary AS
SELECT COUNT(*)                                             AS tickets,
       ROUND(100 * AVG(sla_met), 1)                         AS sla_compliance_pct,
       ROUND(PERCENTILE_APPROX(resolution_min, 0.5), 0)     AS median_resolution_min,
       ROUND(AVG(csat_rating), 2)                           AS avg_csat,
       ROUND(100 * AVG(CASE WHEN csat_rating IS NOT NULL THEN 1 ELSE 0 END), 1) AS survey_response_pct
FROM dashboard_data;

CREATE OR REPLACE TEMP VIEW weekly_team AS
SELECT date_trunc('week', created_at) AS week_start, team,
       COUNT(*) AS tickets, ROUND(100 * AVG(sla_met), 1) AS sla_compliance_pct, ROUND(AVG(csat_rating), 2) AS avg_csat
FROM dashboard_data GROUP BY 1, 2;

CREATE OR REPLACE TEMP VIEW sla_by_channel_priority AS
SELECT channel, priority, COUNT(*) AS tickets, ROUND(100 * AVG(sla_met), 1) AS sla_compliance_pct,
       ROUND(PERCENTILE_APPROX(resolution_min, 0.5), 0) AS median_resolution_min
FROM dashboard_data GROUP BY 1, 2;

CREATE OR REPLACE TEMP VIEW weekday_load AS
SELECT date_format(created_at, 'E') AS weekday, dayofweek(created_at) AS dow,
       COUNT(*) / COUNT(DISTINCT CAST(created_at AS DATE)) AS tickets_per_day,
       ROUND(100 * AVG(sla_met), 1) AS sla_compliance_pct
FROM dashboard_data GROUP BY 1, 2;

CREATE OR REPLACE TEMP VIEW agent_scorecard AS
SELECT agent_id, agent_name, team, tenure_months, COUNT(*) AS tickets,
       ROUND(100 * AVG(sla_met), 1) AS sla_compliance_pct,
       ROUND(PERCENTILE_APPROX(resolution_min, 0.5), 0) AS median_resolution_min,
       ROUND(AVG(csat_rating), 2) AS avg_csat
FROM dashboard_data GROUP BY 1, 2, 3, 4;

CREATE OR REPLACE TEMP VIEW csat_by_sla AS
SELECT CASE WHEN sla_met = 1 THEN 'Within SLA' ELSE 'SLA breached' END AS sla_status,
       COUNT(csat_rating) AS surveys, ROUND(AVG(csat_rating), 2) AS avg_csat,
       ROUND(100 * AVG(CASE WHEN csat_rating >= 4 THEN 1.0 WHEN csat_rating IS NOT NULL THEN 0.0 END), 1) AS pct_satisfied
FROM dashboard_data GROUP BY 1;
