WITH inflight_counts AS (
  SELECT (payload->>'workId') as work_id, count(*) as active_jobs
  FROM importer_queue
  WHERE status IN ('IMPORTING')
  GROUP BY payload->>'workId'
)
SELECT * FROM inflight_counts;
