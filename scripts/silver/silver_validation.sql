-- =============================================================================
-- Silver Layer: Validation
-- =============================================================================
-- Purpose:
--     Verify that bronze-to-silver transformations executed correctly.
--     Run these AFTER silver load completes. Every query should return
--     expected results — if not, something broke in the transformation.
-- =============================================================================


-- =============================================================================
-- ROW COUNTS & PRIMARY KEY INTEGRITY
-- =============================================================================

-- Row counts must match bronze to silver (no rows gained or lost)
SELECT 'cms_hospital_general' AS table_name,
       (SELECT COUNT(*) FROM bronze_schema.cms_hospital_general) AS bronze_count,
       (SELECT COUNT(*) FROM silver_schema.cms_hospital_general) AS silver_count,
       (SELECT COUNT(*) FROM bronze_schema.cms_hospital_general) =
       (SELECT COUNT(*) FROM silver_schema.cms_hospital_general) AS counts_match
UNION ALL
SELECT 'cms_timely_care',
       (SELECT COUNT(*) FROM bronze_schema.cms_timely_care),
       (SELECT COUNT(*) FROM silver_schema.cms_timely_care),
       (SELECT COUNT(*) FROM bronze_schema.cms_timely_care) =
       (SELECT COUNT(*) FROM silver_schema.cms_timely_care)
UNION ALL
SELECT 'cms_complications',
       (SELECT COUNT(*) FROM bronze_schema.cms_complications),
       (SELECT COUNT(*) FROM silver_schema.cms_complications),
       (SELECT COUNT(*) FROM bronze_schema.cms_complications) =
       (SELECT COUNT(*) FROM silver_schema.cms_complications)
UNION ALL
SELECT 'cms_outpatient_imaging',
       (SELECT COUNT(*) FROM bronze_schema.cms_outpatient_imaging),
       (SELECT COUNT(*) FROM silver_schema.cms_outpatient_imaging),
       (SELECT COUNT(*) FROM bronze_schema.cms_outpatient_imaging) =
       (SELECT COUNT(*) FROM silver_schema.cms_outpatient_imaging)
UNION ALL
SELECT 'cms_infections',
       (SELECT COUNT(*) FROM bronze_schema.cms_infections),
       (SELECT COUNT(*) FROM silver_schema.cms_infections),
       (SELECT COUNT(*) FROM bronze_schema.cms_infections) =
       (SELECT COUNT(*) FROM silver_schema.cms_infections);


-- No NULLs in facility_id (should return 0 for all tables)
SELECT 'cms_hospital_general' AS table_name, COUNT(*) AS null_facility_ids
FROM silver_schema.cms_hospital_general WHERE facility_id IS NULL
UNION ALL
SELECT 'cms_timely_care', COUNT(*)
FROM silver_schema.cms_timely_care WHERE facility_id IS NULL
UNION ALL
SELECT 'cms_complications', COUNT(*)
FROM silver_schema.cms_complications WHERE facility_id IS NULL
UNION ALL
SELECT 'cms_outpatient_imaging', COUNT(*)
FROM silver_schema.cms_outpatient_imaging WHERE facility_id IS NULL
UNION ALL
SELECT 'cms_infections', COUNT(*)
FROM silver_schema.cms_infections WHERE facility_id IS NULL;


-- No duplicate (facility_id, measure_id) pairs in fact tables
-- (should return 0 for all tables)
SELECT 'cms_timely_care' AS table_name, COUNT(*) AS duplicate_rows
FROM (SELECT facility_id, measure_id, COUNT(*) AS n
      FROM silver_schema.cms_timely_care
      GROUP BY facility_id, measure_id HAVING COUNT(*) > 1) d
UNION ALL
SELECT 'cms_complications', COUNT(*)
FROM (SELECT facility_id, measure_id, COUNT(*) AS n
      FROM silver_schema.cms_complications
      GROUP BY facility_id, measure_id HAVING COUNT(*) > 1) d
UNION ALL
SELECT 'cms_outpatient_imaging', COUNT(*)
FROM (SELECT facility_id, measure_id, COUNT(*) AS n
      FROM silver_schema.cms_outpatient_imaging
      GROUP BY facility_id, measure_id HAVING COUNT(*) > 1) d
UNION ALL
SELECT 'cms_infections', COUNT(*)
FROM (SELECT facility_id, measure_id, COUNT(*) AS n
      FROM silver_schema.cms_infections
      GROUP BY facility_id, measure_id HAVING COUNT(*) > 1) d;


-- Join integrity: every facility_id in fact tables exists in hospital_general
-- (should return 0)
SELECT 'cms_timely_care' AS table_name, COUNT(DISTINCT t.facility_id) AS orphan_ids
FROM silver_schema.cms_timely_care t
LEFT JOIN silver_schema.cms_hospital_general h ON t.facility_id = h.facility_id
WHERE h.facility_id IS NULL
UNION ALL
SELECT 'cms_complications', COUNT(DISTINCT c.facility_id)
FROM silver_schema.cms_complications c
LEFT JOIN silver_schema.cms_hospital_general h ON c.facility_id = h.facility_id
WHERE h.facility_id IS NULL
UNION ALL
SELECT 'cms_outpatient_imaging', COUNT(DISTINCT i.facility_id)
FROM silver_schema.cms_outpatient_imaging i
LEFT JOIN silver_schema.cms_hospital_general h ON i.facility_id = h.facility_id
WHERE h.facility_id IS NULL
UNION ALL
SELECT 'cms_infections', COUNT(DISTINCT inf.facility_id)
FROM silver_schema.cms_infections inf
LEFT JOIN silver_schema.cms_hospital_general h ON inf.facility_id = h.facility_id
WHERE h.facility_id IS NULL;


-- =============================================================================
-- HOSPITAL_GENERAL TRANSFORMATIONS
-- =============================================================================

-- Ownership mapping — should be exactly 4 groups (Government/Non-Profit/For-Profit/Tribal)
SELECT hospital_ownership, COUNT(*) AS n
FROM silver_schema.cms_hospital_general
GROUP BY hospital_ownership
ORDER BY n DESC;


-- Ownership mapping verification — confirm each of 12 originals maps to the correct group
SELECT DISTINCT hospital_ownership, hospital_ownership_details
FROM silver_schema.cms_hospital_general
ORDER BY hospital_ownership, hospital_ownership_details;


-- Boolean columns — should only contain TRUE, FALSE, or NULL
SELECT 'emergency_services' AS column_name,
       COUNT(*) FILTER (WHERE emergency_services IS NULL) AS nulls,
       COUNT(*) FILTER (WHERE emergency_services = TRUE) AS trues,
       COUNT(*) FILTER (WHERE emergency_services = FALSE) AS falses,
       COUNT(*) AS total
FROM silver_schema.cms_hospital_general
UNION ALL
SELECT 'birthing_friendly',
       COUNT(*) FILTER (WHERE birthing_friendly_designation IS NULL),
       COUNT(*) FILTER (WHERE birthing_friendly_designation = TRUE),
       COUNT(*) FILTER (WHERE birthing_friendly_designation = FALSE),
       COUNT(*)
FROM silver_schema.cms_hospital_general;


-- Overall rating should be 1-5 or NULL
SELECT hospital_overall_rating, COUNT(*) AS n
FROM silver_schema.cms_hospital_general
GROUP BY hospital_overall_rating
ORDER BY hospital_overall_rating;


-- Hospital type distribution
SELECT hospital_type, COUNT(*) AS n
FROM silver_schema.cms_hospital_general
GROUP BY hospital_type
ORDER BY n DESC;


-- Reporting status footnote mapping
-- verify each status has correct footnote codes
SELECT mort_reporting_status, mort_group_footnote, COUNT(*) AS n
FROM silver_schema.cms_hospital_general
WHERE mort_reporting_status IS NOT NULL
GROUP BY mort_reporting_status, mort_group_footnote
ORDER BY mort_reporting_status;


-- Bronze vs silver spot-check: hospital_general
-- emergency_services: text to boolean, rating: text to int, ownership: 12 to 4
SELECT
    b.facility_id,
    b.facility_name        AS bronze_name,
    b.hospital_ownership   AS bronze_ownership,
    b.hospital_overall_rating AS bronze_rating,
    b.emergency_services   AS bronze_emergency,
    s.facility_name        AS silver_name,
    s.hospital_ownership   AS silver_ownership,
    s.hospital_overall_rating AS silver_rating,
    s.emergency_services   AS silver_emergency
FROM bronze_schema.cms_hospital_general b
JOIN silver_schema.cms_hospital_general s
    ON b.facility_id = s.facility_id
WHERE b.facility_id IN ('010001', '010012', '010051');


-- =============================================================================
-- SCORE & USABILITY LOGIC
-- =============================================================================

-- compared_to_national should only be 'Better', 'No Different', 'Worse', or NULL
SELECT 'cms_complications' AS table_name, compared_to_national, COUNT(*) AS n
FROM silver_schema.cms_complications
GROUP BY compared_to_national
UNION ALL
SELECT 'cms_infections', compared_to_national, COUNT(*)
FROM silver_schema.cms_infections
GROUP BY compared_to_national
ORDER BY table_name, compared_to_national;


-- compared_to_national: bronze had 12 variants, silver should have 3 + NULL
SELECT DISTINCT compared_to_national AS value, 'bronze_complications' AS source
FROM bronze_schema.cms_complications
UNION ALL
SELECT DISTINCT compared_to_national, 'bronze_infections'
FROM bronze_schema.cms_infections
UNION ALL
SELECT DISTINCT compared_to_national, 'silver_complications'
FROM silver_schema.cms_complications
UNION ALL
SELECT DISTINCT compared_to_national, 'silver_infections'
FROM silver_schema.cms_infections
ORDER BY source, value;


-- is_score_usable consistency: if score IS NULL, is_score_usable must be FALSE
-- (should return 0 for all tables)
SELECT 'cms_timely_care' AS table_name,
       COUNT(*) AS broken_usability_flags
FROM silver_schema.cms_timely_care
WHERE score_numeric IS NULL AND score_text IS NULL AND is_score_usable = TRUE
UNION ALL
SELECT 'cms_complications', COUNT(*)
FROM silver_schema.cms_complications
WHERE score IS NULL AND is_score_usable = TRUE
UNION ALL
SELECT 'cms_outpatient_imaging', COUNT(*)
FROM silver_schema.cms_outpatient_imaging
WHERE score IS NULL AND is_score_usable = TRUE
UNION ALL
SELECT 'cms_infections', COUNT(*)
FROM silver_schema.cms_infections
WHERE score IS NULL AND is_score_usable = TRUE;


-- score_exclusion_reason consistency: if is_score_usable = FALSE and score exists,
-- there must be an exclusion reason
SELECT 'cms_timely_care' AS table_name,
       COUNT(*) AS missing_reason
FROM silver_schema.cms_timely_care
WHERE is_score_usable = FALSE
  AND (score_numeric IS NOT NULL OR score_text IS NOT NULL)
  AND score_exclusion_reason IS NULL
UNION ALL
SELECT 'cms_complications', COUNT(*)
FROM silver_schema.cms_complications
WHERE is_score_usable = FALSE AND score IS NOT NULL AND score_exclusion_reason IS NULL
UNION ALL
SELECT 'cms_outpatient_imaging', COUNT(*)
FROM silver_schema.cms_outpatient_imaging
WHERE is_score_usable = FALSE AND score IS NOT NULL AND score_exclusion_reason IS NULL
UNION ALL
SELECT 'cms_infections', COUNT(*)
FROM silver_schema.cms_infections
WHERE is_score_usable = FALSE AND score IS NOT NULL AND score_exclusion_reason IS NULL;


-- score_exclusion_reason tier priority — multi-code footnotes must get the highest-priority tier
-- Priority: No Score (1,4,5,8), Not Applicable (7,12,13), Use With Caution (2,3,11,23,28,29)
SELECT
    table_name,
    footnote,
    score_exclusion_reason,
    COUNT(*) AS n
FROM (
    SELECT 'cms_timely_care' AS table_name, footnote, score_exclusion_reason
    FROM silver_schema.cms_timely_care
    WHERE footnote LIKE '%,%' AND is_score_usable = FALSE
    UNION ALL
    SELECT 'cms_complications', footnote, score_exclusion_reason
    FROM silver_schema.cms_complications
    WHERE footnote LIKE '%,%' AND is_score_usable = FALSE
    UNION ALL
    SELECT 'cms_outpatient_imaging', footnote, score_exclusion_reason
    FROM silver_schema.cms_outpatient_imaging
    WHERE footnote LIKE '%,%' AND is_score_usable = FALSE
    UNION ALL
    SELECT 'cms_infections', footnote, score_exclusion_reason
    FROM silver_schema.cms_infections
    WHERE footnote LIKE '%,%' AND is_score_usable = FALSE
) multi_code
GROUP BY table_name, footnote, score_exclusion_reason
ORDER BY table_name, footnote;


-- timely_care score splitting — numeric and text should be mutually exclusive
-- both_populated should be 0
SELECT
    COUNT(*) FILTER (WHERE score_numeric IS NOT NULL AND score_text IS NULL)  AS numeric_only,
    COUNT(*) FILTER (WHERE score_numeric IS NULL AND score_text IS NOT NULL)  AS text_only,
    COUNT(*) FILTER (WHERE score_numeric IS NOT NULL AND score_text IS NOT NULL) AS both_populated,
    COUNT(*) FILTER (WHERE score_numeric IS NULL AND score_text IS NULL)      AS both_null
FROM silver_schema.cms_timely_care;


-- Score range check — MIN/MAX per table to catch bad casts (negatives, impossibly large values)
SELECT 'cms_timely_care' AS table_name,
       MIN(score_numeric) AS min_score, MAX(score_numeric) AS max_score
FROM silver_schema.cms_timely_care
UNION ALL
SELECT 'cms_complications',
       MIN(score), MAX(score)
FROM silver_schema.cms_complications
UNION ALL
SELECT 'cms_outpatient_imaging',
       MIN(score), MAX(score)
FROM silver_schema.cms_outpatient_imaging
UNION ALL
SELECT 'cms_infections',
       MIN(score), MAX(score)
FROM silver_schema.cms_infections;


-- Silent data loss from type casting — count bronze rows that had a real score
-- Should return 0
SELECT 'cms_timely_care' AS table_name, COUNT(*) AS lost_scores
FROM bronze_schema.cms_timely_care b
JOIN silver_schema.cms_timely_care s
    ON b.facility_id = s.facility_id AND b.measure_id = s.measure_id
WHERE b.score IS NOT NULL
  AND b.score NOT IN ('Not Available', 'N/A', '')
  AND b.score !~ '^\s*$'
  AND s.score_numeric IS NULL AND s.score_text IS NULL
UNION ALL
SELECT 'cms_complications', COUNT(*)
FROM bronze_schema.cms_complications b
JOIN silver_schema.cms_complications s
    ON b.facility_id = s.facility_id AND b.measure_id = s.measure_id
WHERE b.score IS NOT NULL
  AND b.score NOT IN ('Not Available', 'N/A', '')
  AND b.score !~ '^\s*$'
  AND s.score IS NULL
UNION ALL
SELECT 'cms_outpatient_imaging', COUNT(*)
FROM bronze_schema.cms_outpatient_imaging b
JOIN silver_schema.cms_outpatient_imaging s
    ON b.facility_id = s.facility_id AND b.measure_id = s.measure_id
WHERE b.score IS NOT NULL
  AND b.score NOT IN ('Not Available', 'N/A', '')
  AND b.score !~ '^\s*$'
  AND s.score IS NULL
UNION ALL
SELECT 'cms_infections', COUNT(*)
FROM bronze_schema.cms_infections b
JOIN silver_schema.cms_infections s
    ON b.facility_id = s.facility_id AND b.measure_id = s.measure_id
WHERE b.score IS NOT NULL
  AND b.score NOT IN ('Not Available', 'N/A', '')
  AND b.score !~ '^\s*$'
  AND s.score IS NULL;


-- Footnote array-overlap: no row should be marked is_score_usable = TRUE
-- while carrying a blacklisted footnote code (should return 0 for all tables)
SELECT 'cms_timely_care' AS table_name, COUNT(*) AS incorrect
FROM silver_schema.cms_timely_care
WHERE is_score_usable = TRUE
  AND footnote IS NOT NULL
  AND ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
    && ARRAY['1','2','3','4','5','7','8','12','13','23','28','29']
UNION ALL
SELECT 'cms_complications', COUNT(*)
FROM silver_schema.cms_complications
WHERE is_score_usable = TRUE
  AND footnote IS NOT NULL
  AND ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
    && ARRAY['1','2','3','4','5','7','8','12','13','23','28','29']
UNION ALL
SELECT 'cms_outpatient_imaging', COUNT(*)
FROM silver_schema.cms_outpatient_imaging
WHERE is_score_usable = TRUE
  AND footnote IS NOT NULL
  AND ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
    && ARRAY['1','2','3','4','5','7','8','12','13','23','28','29']
UNION ALL
SELECT 'cms_infections', COUNT(*)
FROM silver_schema.cms_infections
WHERE is_score_usable = TRUE
  AND footnote IS NOT NULL
  AND ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
    && ARRAY['1','2','3','4','5','7','8','12','13','23','28','29'];



-- =============================================================================
-- BRONZE VS SILVER SPOT-CHECKS
-- =============================================================================

-- timely_care: score conversion, numeric stays numeric, text stays text,
-- 'Not Available' = both NULL
SELECT
    b.facility_id,
    b.measure_id,
    b.measure_name,
    b.score          AS bronze_score,
    s.score_numeric,
    s.score_text,
    s.is_score_usable,
    s.score_exclusion_reason
FROM bronze_schema.cms_timely_care b
JOIN silver_schema.cms_timely_care s
    ON b.facility_id = s.facility_id
   AND b.measure_id  = s.measure_id
WHERE b.facility_id IN ('010001', '010006')
ORDER BY b.facility_id, b.measure_id;


-- complications compared_to_national standardized, score cast to NUMERIC,
-- dates parsed from text to DATE, 'Not Available' = NULL
SELECT
    b.facility_id,
    b.measure_id,
    b.compared_to_national AS bronze_compared,
    s.compared_to_national AS silver_compared,
    b.score                AS bronze_score,
    s.score                AS silver_score,
    b.start_date           AS bronze_start_date,
    s.start_date           AS silver_start_date,
    b.end_date             AS bronze_end_date,
    s.end_date             AS silver_end_date
FROM bronze_schema.cms_complications b
JOIN silver_schema.cms_complications s
    ON b.facility_id = s.facility_id
   AND b.measure_id  = s.measure_id
WHERE b.facility_id = '010005'
ORDER BY b.measure_id;


-- infections: score values and measure_suffix extraction
SELECT
    b.facility_id,
    b.measure_id,
    b.score        AS bronze_score,
    s.score        AS silver_score,
    s.measure_suffix
FROM bronze_schema.cms_infections b
JOIN silver_schema.cms_infections s
    ON b.facility_id = s.facility_id
   AND b.measure_id  = s.measure_id
WHERE b.facility_id = '010001'
ORDER BY b.measure_id;


-- =============================================================================
-- DATE AND MEASURE VALIDATION
-- =============================================================================

-- Verifying date ranges are reasonable
SELECT 'cms_timely_care' AS table_name,
       MIN(start_date) AS earliest, MAX(end_date) AS latest
FROM silver_schema.cms_timely_care
UNION ALL
SELECT 'cms_complications', MIN(start_date), MAX(end_date)
FROM silver_schema.cms_complications
UNION ALL
SELECT 'cms_outpatient_imaging', MIN(start_date), MAX(end_date)
FROM silver_schema.cms_outpatient_imaging
UNION ALL
SELECT 'cms_infections', MIN(start_date), MAX(end_date)
FROM silver_schema.cms_infections;


-- No row should have start_date > end_date
-- should return 0 for all tables
SELECT 'cms_timely_care' AS table_name, COUNT(*) AS inverted_dates
FROM silver_schema.cms_timely_care
WHERE start_date > end_date
UNION ALL
SELECT 'cms_complications', COUNT(*)
FROM silver_schema.cms_complications
WHERE start_date > end_date
UNION ALL
SELECT 'cms_outpatient_imaging', COUNT(*)
FROM silver_schema.cms_outpatient_imaging
WHERE start_date > end_date
UNION ALL
SELECT 'cms_infections', COUNT(*)
FROM silver_schema.cms_infections
WHERE start_date > end_date;


-- Infection measure_suffix extraction, should return 6 distinct values
SELECT DISTINCT measure_suffix
FROM silver_schema.cms_infections
ORDER BY measure_suffix;


-- =============================================================================
-- FOOTNOTE DISTRIBUTIONS (profiling, not pass/fail)
-- =============================================================================

-- Footnote value distributions per fact table
-- Code 5 most dominant (63996) followed by null(45780)
SELECT 'cms_timely_care' AS table_name, footnote, COUNT(*) AS n
FROM silver_schema.cms_timely_care
GROUP BY footnote
ORDER BY n DESC;

-- Null most dominant (50908) followed by 13 (16380)
SELECT 'cms_complications' AS table_name, footnote, COUNT(*) AS n
FROM silver_schema.cms_complications
GROUP BY footnote
ORDER BY n DESC;

-- Null most dominant (9536) followed ny 1 (5176)
SELECT 'cms_outpatient_imaging' AS table_name, footnote, COUNT(*) AS n
FROM silver_schema.cms_outpatient_imaging
GROUP BY footnote
ORDER BY n DESC;

-- Null most dominant (92515) followed by 13 (28728)
SELECT 'cms_infections' AS table_name, footnote, COUNT(*) AS n
FROM silver_schema.cms_infections
GROUP BY footnote
ORDER BY n DESC;


-- Multi-code footnotes (comma-separated) — verify they exist and are handled
SELECT 'cms_timely_care' AS table_name, footnote, COUNT(*) AS n
FROM silver_schema.cms_timely_care
WHERE footnote LIKE '%,%'
GROUP BY footnote ORDER BY n DESC;

SELECT 'cms_complications' AS table_name, footnote, COUNT(*) AS n
FROM silver_schema.cms_complications
WHERE footnote LIKE '%,%'
GROUP BY footnote ORDER BY n DESC;

SELECT 'cms_outpatient_imaging' AS table_name, footnote, COUNT(*) AS n
FROM silver_schema.cms_outpatient_imaging
WHERE footnote LIKE '%,%'
GROUP BY footnote ORDER BY n DESC;

SELECT 'cms_infections' AS table_name, footnote, COUNT(*) AS n
FROM silver_schema.cms_infections
WHERE footnote LIKE '%,%'
GROUP BY footnote ORDER BY n DESC;

-- hospital_general multi-code footnotes
SELECT safety_group_footnote, COUNT(*) AS n
FROM silver_schema.cms_hospital_general
WHERE safety_group_footnote LIKE '%,%'
GROUP BY safety_group_footnote
ORDER BY n DESC;


-- =============================================================================
-- DATA QUALITY INVESTIGATION
-- =============================================================================

-- How many hospitals have zero usable scores across all 4 fact tables?
-- Result: 198 hospitals
WITH all_scores AS (
    SELECT facility_id, is_score_usable FROM silver_schema.cms_timely_care
    UNION ALL
    SELECT facility_id, is_score_usable FROM silver_schema.cms_complications
    UNION ALL
    SELECT facility_id, is_score_usable FROM silver_schema.cms_outpatient_imaging
    UNION ALL
    SELECT facility_id, is_score_usable FROM silver_schema.cms_infections
),
zero_usable AS (
    SELECT facility_id
    FROM all_scores
    GROUP BY facility_id
    HAVING MAX(CASE WHEN is_score_usable THEN 1 ELSE 0 END) = 0
)
SELECT COUNT(*) AS hospitals_with_no_usable_scores
FROM zero_usable;


-- What type of hospitals have zero usable scores?
-- Mostly children's hospitals, rural emergency, critical access, and acute care
WITH all_scores AS (
    SELECT facility_id, is_score_usable FROM silver_schema.cms_timely_care
    UNION ALL
    SELECT facility_id, is_score_usable FROM silver_schema.cms_complications
    UNION ALL
    SELECT facility_id, is_score_usable FROM silver_schema.cms_outpatient_imaging
    UNION ALL
    SELECT facility_id, is_score_usable FROM silver_schema.cms_infections
),
zero_usable AS (
    SELECT facility_id
    FROM all_scores
    GROUP BY facility_id
    HAVING MAX(CASE WHEN is_score_usable THEN 1 ELSE 0 END) = 0
)
SELECT
    hg.hospital_type,
    hg.hospital_ownership,
    hg.emergency_services,
    hg.hospital_overall_rating,
    COUNT(*) AS n_hospitals
FROM zero_usable zu
JOIN silver_schema.cms_hospital_general hg
    ON zu.facility_id = hg.facility_id
GROUP BY hospital_type, hospital_ownership, emergency_services, hospital_overall_rating
ORDER BY n_hospitals DESC;


-- Acute Care hospitals with zero usable scores — two groups:
-- non-participating (opted out, including territories) and insufficient data (VA, small facilities)
WITH all_scores AS (
    SELECT facility_id, is_score_usable FROM silver_schema.cms_timely_care
    UNION ALL
    SELECT facility_id, is_score_usable FROM silver_schema.cms_complications
    UNION ALL
    SELECT facility_id, is_score_usable FROM silver_schema.cms_outpatient_imaging
    UNION ALL
    SELECT facility_id, is_score_usable FROM silver_schema.cms_infections
),
zero_usable AS (
    SELECT facility_id
    FROM all_scores
    GROUP BY facility_id
    HAVING MAX(CASE WHEN is_score_usable THEN 1 ELSE 0 END) = 0
)
SELECT
    hg.facility_id,
    hg.hospital_type,
    hg.state,
    hg.hospital_ownership,
    hg.emergency_services,
    hg.hospital_overall_rating,
    hg.mort_reporting_status,
    hg.safety_reporting_status,
    hg.readm_reporting_status,
    hg.pt_exp_reporting_status,
    hg.te_reporting_status
FROM zero_usable zu
JOIN silver_schema.cms_hospital_general hg
    ON zu.facility_id = hg.facility_id
WHERE hg.hospital_type IN ('Acute Care Hospitals', 'Acute Care - Veterans Administration')
ORDER BY hg.state, hg.facility_name;
