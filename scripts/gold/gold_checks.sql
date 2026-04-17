-- dim_hospital: expect 5,426 (one per facility)
-- Checks passed
SELECT 'dim_hospital' AS view_name, COUNT(*) AS row_count FROM gold_schema.dim_hospital;

-- Check for any mislabeled values
-- Check passed no mislabels
SELECT region, COUNT(*) AS n
FROM gold_schema.dim_hospital
GROUP BY region
ORDER BY n DESC;

-- Analytical row counts
-- Passed
SELECT 'vw_imaging_vs_ed_wait' AS view_name, COUNT(*) AS rows
FROM gold_schema.vw_imaging_vs_ed_wait
UNION ALL
SELECT 'vw_ownership_imaging_ed_stats', COUNT(*)
FROM gold_schema.vw_ownership_imaging_ed_stats
UNION ALL
SELECT 'vw_complications_vs_process_care', COUNT(*)
FROM gold_schema.vw_complications_vs_process_care;



-- Verifying that there are no OP-8 in imaging
SELECT
    'OP-8 excluded' AS check_name,
    COUNT(*) AS op8_usable_rows_exist_but_excluded
FROM silver_schema.cms_outpatient_imaging
WHERE measure_id = 'OP-8'
  AND is_score_usable = TRUE
  AND score IS NOT NULL;


SELECT facility_id, facility_name, avg_imaging_score, imaging_measures_used,
       avg_ed_wait_minutes, ed_measures_used
FROM gold_schema.vw_imaging_vs_ed_wait
LIMIT 3;

-- Checking avg scores for measures
SELECT facility_id, measure_id, score
FROM silver_schema.cms_outpatient_imaging
WHERE facility_id = '010001'
  AND measure_id IN ('OP-10', 'OP-13', 'OP-39')
  AND is_score_usable = TRUE AND score IS NOT NULL;

-- ED scores (should only be OP_18a-d, all <= 1440)
SELECT facility_id, measure_id, score_numeric
FROM silver_schema.cms_timely_care
WHERE facility_id = '010001'
  AND measure_id IN ('OP_18a', 'OP_18b', 'OP_18c', 'OP_18d')
  AND is_score_usable = TRUE AND score_numeric IS NOT NULL;


-- Tribal should NOT appear. Proportions should roughly match exploration:
-- Non-Profit ~64%, For-Profit ~15.7%, Government ~20%
SELECT
    hospital_ownership,
    n_hospitals,
    ROUND(100.0 * n_hospitals / SUM(n_hospitals) OVER (), 2) AS pct_of_sample,
    avg_imaging_score,
    stddev_imaging_score,
    avg_ed_wait_minutes,
    stddev_ed_wait_minutes
FROM gold_schema.vw_ownership_imaging_ed_stats;

-- Should be between 0 and 100, no NULLs
SELECT
    MIN(complication_pct_worse) AS min_pct,
    MAX(complication_pct_worse) AS max_pct,
    COUNT(*) FILTER (WHERE complication_pct_worse IS NULL) AS null_count,
    COUNT(*) FILTER (WHERE complication_pct_worse < 0 OR complication_pct_worse > 100) AS out_of_range
FROM gold_schema.vw_complications_vs_process_care;
