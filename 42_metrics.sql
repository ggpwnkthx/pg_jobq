-- 42_metrics.sql
-- Simple metrics smoke test; may fail if caller lacks permissions.
-- High-level queue overview (if caller has SELECT on the view).
SELECT *
FROM jobq.v_queue_overview;
-- Metrics function (if caller has EXECUTE on it).
SELECT *
FROM jobq.get_queue_metrics();