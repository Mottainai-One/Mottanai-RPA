-- Apply once to an existing MOTTAINAI_ANALITICO database before enabling
-- the operational-to-analytics RPA schedule. Safe to run again.
-- The view only exposes a data mart whose facts and dimensions were committed
-- together with its successful job_log publication record.
BEGIN;
SET LOCAL search_path TO mottainai_analytics, public;

CREATE OR REPLACE VIEW vw_published_batch AS
SELECT
    job_id,
    cutoff_at AS source_cutoff_at,
    (cutoff_at AT TIME ZONE 'America/Sao_Paulo')::DATE AS snapshot_date,
    batch_id,
    finished_at AS published_at
FROM job_log
WHERE job_name = 'operational_to_analytics_daily'
  AND status = 'SUCCEEDED'
  AND batch_id IS NOT NULL
  AND cutoff_at IS NOT NULL
  AND metadata ->> 'publication_contract' = 'analytics_mart_v1'
ORDER BY finished_at DESC, job_id DESC
LIMIT 1;

COMMENT ON VIEW vw_published_batch IS 'publication_contract=analytics_mart_v1';

COMMIT;
