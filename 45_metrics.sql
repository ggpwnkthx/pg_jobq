\set ON_ERROR_STOP on

-- High-level queue overview (if caller has SELECT on the view).
SELECT *
FROM jobq.v_queue_overview;

-- Metrics function (if caller has EXECUTE on it).
SELECT *
FROM jobq.get_queue_metrics();
