-- =============================================================================
-- Bronze Layer: Data Exploration & Profiling
-- =============================================================================
-- Purpose:
--     Profile all five CMS source tables loaded into the bronze layer.
--     Documents row counts, NULL patterns, categorical distributions,
--     duplicate checks, non-standard values, and date ranges.
--     All findings drive Silver layer transformation decisions.
-- Run after: bronze load procedure completes successfully.
-- =============================================================================

-- Row count validation across all tables
SELECT 'cms_hospital_general'   AS table_name, COUNT(*) AS row_count FROM bronze_schema.cms_hospital_general
UNION ALL
SELECT 'cms_timely_care',        COUNT(*) FROM bronze_schema.cms_timely_care
UNION ALL
SELECT 'cms_complications',      COUNT(*) FROM bronze_schema.cms_complications
UNION ALL
SELECT 'cms_outpatient_imaging', COUNT(*) FROM bronze_schema.cms_outpatient_imaging
UNION ALL
SELECT 'cms_infections',         COUNT(*) FROM bronze_schema.cms_infections
ORDER BY table_name;

-- =============================================================================
-- cms_hospital_general
-- =============================================================================

-- Total row count
SELECT
    COUNT(*) AS n
FROM bronze_schema.cms_hospital_general;


-- FINDING: facility_id is unique — no duplicates, safe to use as join key
SELECT
    facility_id,
    COUNT(*) AS n
FROM bronze_schema.cms_hospital_general
GROUP BY facility_id
HAVING COUNT(*) > 1;

-- FINDING: hospital_overall_rating values are 1–5 and 'Not Available'
-- ACTION REQUIRED: Cast to INT in Silver, map 'Not Available' to NULL
SELECT DISTINCT
    hospital_overall_rating
FROM bronze_schema.cms_hospital_general;

-- FINDING: 8 distinct hospital types
SELECT DISTINCT
    hospital_type
FROM bronze_schema.cms_hospital_general;

-- FINDING: 12 distinct hospital ownerships
-- ACTION REQUIRED: Consolidate to around 3 canonical groups in Silver
--                  (Government, Non-Profit, For-Profit)
SELECT DISTINCT
    hospital_ownership
FROM bronze_schema.cms_hospital_general;

-- Hospital type distribution — Acute Care and Critical Access dominate
SELECT
    hospital_type,
    COUNT(*) AS n
FROM bronze_schema.cms_hospital_general
GROUP BY hospital_type
ORDER BY n DESC;

-- Hospital ownership distribution
SELECT
    hospital_ownership,
    COUNT(*) AS n
FROM bronze_schema.cms_hospital_general
GROUP BY hospital_ownership
ORDER BY n DESC;

-- Footnote coverage across quality measure groups
SELECT
    COUNT(*)                                                                                        AS total_hospitals,
    SUM(CASE WHEN mort_group_footnote   IS NOT NULL AND mort_group_footnote   != '' THEN 1 ELSE 0 END) AS mort_footnoted,
    SUM(CASE WHEN safety_group_footnote IS NOT NULL AND safety_group_footnote != '' THEN 1 ELSE 0 END) AS safety_footnoted,
    SUM(CASE WHEN readm_group_footnote  IS NOT NULL AND readm_group_footnote  != '' THEN 1 ELSE 0 END) AS readm_footnoted,
    SUM(CASE WHEN pt_exp_group_footnote IS NOT NULL AND pt_exp_group_footnote != '' THEN 1 ELSE 0 END) AS pt_exp_footnoted,
    SUM(CASE WHEN te_group_footnote     IS NOT NULL AND te_group_footnote     != '' THEN 1 ELSE 0 END) AS te_footnoted
FROM bronze_schema.cms_hospital_general;

-- FINDING: emergency_services is 'Yes' or 'No'
-- ACTION REQUIRED: Cast to BOOLEAN in Silver
SELECT DISTINCT
    emergency_services
FROM bronze_schema.cms_hospital_general;

-- FINDING: birthing_friendly_designation uses 'Y' (not 'Yes') and NULL (not 'No')
-- ACTION REQUIRED: Standardize to TRUE/FALSE BOOLEAN in Silver.
--                  NULL should be interpreted as FALSE (not designated).
SELECT DISTINCT
    birthing_friendly_designation
FROM bronze_schema.cms_hospital_general;

-- FINDING: mort_group_footnote contains codes 5, 19, 22, 23
-- Code 5 has the most hospitals, followed by 19; remainder are small counts
SELECT
    mort_group_footnote,
    COUNT(*) AS n
FROM bronze_schema.cms_hospital_general
WHERE mort_group_footnote IS NOT NULL
  AND mort_group_footnote != ''
GROUP BY mort_group_footnote
ORDER BY n DESC;


-- =============================================================================
-- cms_timely_care
-- =============================================================================

-- Total row count
SELECT
    COUNT(*) AS n
FROM bronze_schema.cms_timely_care;

-- FINDING: Conditions present — Cataract Surgery, Colonoscopy, eCQM,
--          Emergency Department, Healthcare Personnel Vaccination, Sepsis Care
SELECT DISTINCT
    condition
FROM bronze_schema.cms_timely_care;


-- Measure count per measure_id
SELECT
    measure_id,
    COUNT(*) AS n_hospitals
FROM bronze_schema.cms_timely_care
GROUP BY measure_id
ORDER BY n_hospitals DESC;

-- FINDING: Score — 80,534 of 138,129 rows are 'Not Available' (58.3% missing)
-- ACTION REQUIRED: Cast numeric values to NUMERIC in Silver;
--                  tag NULLs with score_availability_flag
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN score = '' OR score IS NULL THEN 1 ELSE 0 END)   AS blank_or_null,
    SUM(CASE WHEN score ~ '^\d+\.?\d*$' THEN 1 ELSE 0 END)         AS numeric,
    SUM(CASE WHEN score = 'Not Available' THEN 1 ELSE 0 END)        AS not_available
FROM bronze_schema.cms_timely_care;

-- FINDING: Non-standard score values (not numeric, not 'Not Available', not blank)
-- ACTION REQUIRED: Review values below — determine if castable or flag in Silver
SELECT DISTINCT
    score,
    COUNT(*) AS n
FROM bronze_schema.cms_timely_care
WHERE score IS NOT NULL
  AND score != ''
  AND score != 'Not Available'
  AND score !~ '^\d+\.?\d*$'
GROUP BY score
ORDER BY n DESC;

-- FINDING: Footnote code distribution for timely care
-- ACTION REQUIRED: Cross-reference CMS footnote legend to drive score_availability_flag logic
SELECT
    footnote,
    COUNT(*) AS n
FROM bronze_schema.cms_timely_care
GROUP BY footnote
ORDER BY n DESC;

-- Date range check
SELECT
    MIN(start_date) AS earliest_period,
    MAX(end_date)   AS latest_period
FROM bronze_schema.cms_timely_care;

-- All distinct start dates — confirm format consistency
SELECT DISTINCT
    start_date
FROM bronze_schema.cms_timely_care;

-- All distinct end dates — confirm format consistency
SELECT DISTINCT
    end_date
FROM bronze_schema.cms_timely_care;

-- FINDING: Check for duplicate rows on (facility_id, measure_id, condition)
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY facility_id, measure_id, condition
            ORDER BY start_date
        ) AS ranking
    FROM bronze_schema.cms_timely_care
) t
WHERE ranking > 1;


SELECT DISTINCT
    score,
    COUNT(*) AS n
FROM bronze_schema.cms_timely_care
WHERE score IS NOT NULL
  AND score != ''
  AND score != 'Not Available'
  AND score !~ '^\d+\.?\d*$'
GROUP BY score
ORDER BY n DESC;

-- =============================================================================
-- cms_complications
-- =============================================================================

-- Total row count
SELECT
    COUNT(*) AS n
FROM bronze_schema.cms_complications;

-- Measure count per measure_id — all measures report 4,789 hospitals (uniform coverage)
SELECT
    measure_id,
    COUNT(*) AS n_hospitals
FROM bronze_schema.cms_complications
GROUP BY measure_id
ORDER BY n_hospitals DESC;

-- FINDING: 8 distinct values in compared_to_national.
-- Values include semantic duplicates e.g. 'No Different Than the National Rate'
-- and 'Not Different Than National Rate' which represent the same category.
-- ACTION REQUIRED: Standardize to 4 canonical values in Silver layer.
SELECT
    compared_to_national,
    COUNT(*) AS n_of_appearances
FROM bronze_schema.cms_complications
GROUP BY compared_to_national
ORDER BY n_of_appearances DESC;

-- FINDING: Score — 43,646 of 95,780 rows are 'Not Available' (45.6% missing)
-- ACTION REQUIRED: Cast numeric values to NUMERIC in Silver;
--                  tag NULLs with score_availability_flag
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN score = '' OR score IS NULL THEN 1 ELSE 0 END)   AS blank_or_null,
    SUM(CASE WHEN score ~ '^\d+\.?\d*$' THEN 1 ELSE 0 END)         AS numeric,
    SUM(CASE WHEN score = 'Not Available' THEN 1 ELSE 0 END)        AS not_available
FROM bronze_schema.cms_complications;

-- FINDING: Denominator — 41,777 of 95,780 rows are 'Not Available'
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN denominator = '' OR denominator IS NULL THEN 1 ELSE 0 END) AS blank_or_null,
    SUM(CASE WHEN denominator ~ '^\d+\.?\d*$' THEN 1 ELSE 0 END)             AS numeric,
    SUM(CASE WHEN denominator = 'Not Available' THEN 1 ELSE 0 END)            AS not_available
FROM bronze_schema.cms_complications;

-- FINDING: lower_estimate — 43,646 of 95,780 rows are 'Not Available'
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN lower_estimate = '' OR lower_estimate IS NULL THEN 1 ELSE 0 END) AS blank_or_null,
    SUM(CASE WHEN lower_estimate ~ '^\d+\.?\d*$' THEN 1 ELSE 0 END)                AS numeric,
    SUM(CASE WHEN lower_estimate = 'Not Available' THEN 1 ELSE 0 END)               AS not_available
FROM bronze_schema.cms_complications;

-- FINDING: higher_estimate — 43,646 of 95,780 rows are 'Not Available'
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN higher_estimate = '' OR higher_estimate IS NULL THEN 1 ELSE 0 END) AS blank_or_null,
    SUM(CASE WHEN higher_estimate ~ '^\d+\.?\d*$' THEN 1 ELSE 0 END)                 AS numeric,
    SUM(CASE WHEN higher_estimate = 'Not Available' THEN 1 ELSE 0 END)                AS not_available
FROM bronze_schema.cms_complications;

-- FINDING: No duplicate rows on (facility_id, measure_id) — no dedup needed in Silver
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY facility_id, measure_id ORDER BY start_date) AS ranking
    FROM bronze_schema.cms_complications
) t
WHERE ranking > 1;

-- FINDING: Footnote — NULL is the most common value; codes 1 and 28 appear in small counts
-- ACTION REQUIRED: Cross-reference CMS footnote legend to drive score_availability_flag logic
SELECT
    footnote,
    COUNT(*) AS n
FROM bronze_schema.cms_complications
GROUP BY footnote
ORDER BY n DESC;

-- Date range check
SELECT
    MIN(start_date) AS earliest_period,
    MAX(end_date)   AS latest_period
FROM bronze_schema.cms_complications;

-- All distinct start dates — confirm format consistency
SELECT DISTINCT
    start_date
FROM bronze_schema.cms_complications;

-- All distinct end dates — confirm format consistency
SELECT DISTINCT
    end_date
FROM bronze_schema.cms_complications;


-- =============================================================================
-- cms_outpatient_imaging
-- =============================================================================

-- FINDING: Each measure_id has identical row count — uniform hospital coverage
SELECT
    measure_id,
    COUNT(*) AS n_measure_id
FROM bronze_schema.cms_outpatient_imaging
GROUP BY measure_id
ORDER BY n_measure_id DESC;

-- FINDING: Score — 8,810 of 18,500 rows are 'Not Available' (47.6% missing)
-- ACTION REQUIRED: Cast numeric values to NUMERIC in Silver;
--                  tag NULLs with score_availability_flag
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN score = '' OR score IS NULL THEN 1 ELSE 0 END)   AS blank_or_null,
    SUM(CASE WHEN score ~ '^\d+\.?\d*$' THEN 1 ELSE 0 END)         AS numeric,
    SUM(CASE WHEN score = 'Not Available' THEN 1 ELSE 0 END)        AS not_available
FROM bronze_schema.cms_outpatient_imaging;

-- FINDING: Non-standard score values (not numeric, not 'Not Available', not blank)
-- ACTION REQUIRED: Review values below — determine if castable or flag in Silver
SELECT DISTINCT
    score,
    COUNT(*) AS n
FROM bronze_schema.cms_outpatient_imaging
WHERE score IS NOT NULL
  AND score != ''
  AND score != 'Not Available'
  AND score !~ '^\d+\.?\d*$'
GROUP BY score
ORDER BY n DESC;

-- FINDING: Footnote code distribution for outpatient imaging
-- ACTION REQUIRED: Cross-reference CMS footnote legend to drive score_availability_flag logic
SELECT
    footnote,
    COUNT(*) AS n
FROM bronze_schema.cms_outpatient_imaging
GROUP BY footnote
ORDER BY n DESC;

-- FINDING: No duplicate rows on (facility_id, measure_id)
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY facility_id, measure_id ORDER BY start_date) AS ranking
    FROM bronze_schema.cms_outpatient_imaging
) t
WHERE ranking > 1;

-- Date range check
SELECT
    MIN(start_date) AS earliest_period,
    MAX(end_date)   AS latest_period
FROM bronze_schema.cms_outpatient_imaging;

-- No low, medium, high, very high scores here
SELECT DISTINCT
    score,
    COUNT(*) AS n
FROM bronze_schema.cms_outpatient_imaging
WHERE score IS NOT NULL
  AND score != ''
  AND score != 'Not Available'
  AND score !~ '^\d+\.?\d*$'
GROUP BY score
ORDER BY n DESC;



-- =============================================================================
-- cms_infections
-- =============================================================================

-- FINDING: 36 measure_ids — HAI_1 through HAI_6 variants
SELECT DISTINCT
    measure_id
FROM bronze_schema.cms_infections;


-- FINDING: compared_to_national has 4 clean, consistent values — no standardization needed
--          Contrast: cms_complications had 8 values with semantic duplicates
SELECT
    compared_to_national,
    COUNT(*) AS n_of_times
FROM bronze_schema.cms_infections
GROUP BY compared_to_national
ORDER BY n_of_times DESC;

-- FINDING: Score — less than half of rows are missing (confirm exact count below)
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN score = '' OR score IS NULL THEN 1 ELSE 0 END)   AS blank_or_null,
    SUM(CASE WHEN score ~ '^\d+\.?\d*$' THEN 1 ELSE 0 END)         AS numeric,
    SUM(CASE WHEN score = 'Not Available' THEN 1 ELSE 0 END)        AS not_available
FROM bronze_schema.cms_infections;

-- Row count per facility_id
SELECT
    facility_id,
    COUNT(*) AS n_per_id
FROM bronze_schema.cms_infections
GROUP BY facility_id
ORDER BY n_per_id DESC;

-- FINDING: Facilities that do NOT have the expected 36 rows
-- These hospitals are missing measures — flag for investigation in Silver
SELECT
    facility_id,
    COUNT(*) AS n_per_id
FROM bronze_schema.cms_infections
GROUP BY facility_id
HAVING COUNT(*) != 36;

-- FINDING: No duplicate rows on (facility_id, measure_id)
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY facility_id, measure_id ORDER BY start_date) AS ranking
    FROM bronze_schema.cms_infections
) t
WHERE ranking > 1;

-- Date range check
SELECT
    MIN(start_date) AS earliest_period,
    MAX(end_date)   AS latest_period
FROM bronze_schema.cms_infections;


SELECT
    'hospital_to_timely_care' AS join_test,
    COUNT(*) AS matched_rows
FROM bronze_schema.cms_hospital_general h
INNER JOIN bronze_schema.cms_timely_care t ON h.facility_id = t.facility_id;



-- Check if all IDs are 6 characters across all tables
SELECT
    'cms_hospital_general' AS table_name,
    LENGTH(facility_id) AS id_length,
    COUNT(*) AS n
FROM bronze_schema.cms_hospital_general
GROUP BY LENGTH(facility_id)

UNION ALL

SELECT 'cms_timely_care', LENGTH(facility_id), COUNT(*)
FROM bronze_schema.cms_timely_care
GROUP BY LENGTH(facility_id)

UNION ALL

SELECT 'cms_complications', LENGTH(facility_id), COUNT(*)
FROM bronze_schema.cms_complications
GROUP BY LENGTH(facility_id)

UNION ALL

SELECT 'cms_outpatient_imaging', LENGTH(facility_id), COUNT(*)
FROM bronze_schema.cms_outpatient_imaging
GROUP BY LENGTH(facility_id)

UNION ALL

SELECT 'cms_infections', LENGTH(facility_id), COUNT(*)
FROM bronze_schema.cms_infections
GROUP BY LENGTH(facility_id)

ORDER BY table_name, id_length;

