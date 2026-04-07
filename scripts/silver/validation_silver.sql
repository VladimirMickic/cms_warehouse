-- All of the tables match on row counts
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

-- None of the id's have NULLS
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

-- 4 Distinct ownership types
SELECT hospital_ownership, COUNT(*) AS n
FROM silver_schema.cms_hospital_general
GROUP BY hospital_ownership
ORDER BY n DESC;

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

-- The rows map out correctly from bronze to silver, emergency services corrected from text to boolean, rating cast from text to int
SELECT
    b.facility_id,

    -- BRONZE values
    b.facility_name        AS bronze_name,
    b.hospital_ownership   AS bronze_ownership,
    b.hospital_overall_rating AS bronze_rating,
    b.emergency_services   AS bronze_emergency,

    -- SILVER values
    s.facility_name        AS silver_name,
    s.hospital_ownership   AS silver_ownership,
    s.hospital_overall_rating AS silver_rating,
    s.emergency_services   AS silver_emergency

FROM bronze_schema.cms_hospital_general b
JOIN silver_schema.cms_hospital_general s
    ON b.facility_id = s.facility_id

WHERE b.facility_id IN ('010001','010012','010051');


-- Overall ratings range from 1-5 and NULLS
SELECT hospital_overall_rating, COUNT(*) AS n
FROM silver_schema.cms_hospital_general
GROUP BY hospital_overall_rating
ORDER BY hospital_overall_rating;

-- compared_to_national should only be 'Better', 'No Different', 'Worse', or NULLS
SELECT 'cms_complications' AS table_name, compared_to_national, COUNT(*) AS n
FROM silver_schema.cms_complications
GROUP BY compared_to_national
UNION ALL
SELECT 'cms_infections', compared_to_national, COUNT(*)
FROM silver_schema.cms_infections
GROUP BY compared_to_national
ORDER BY table_name, compared_to_national;


-- Checking if scores from bronze converted into score_numeric where there was a number (e.g. 208)
-- Checking if scores from bronze converted into score_text where there is a string (e.g. high)
-- Checking to see if when score is not available in bronze both score_numeric and score_text are null
-- All checks passed
SELECT
    b.facility_id,
    b.measure_id,
    b.measure_name,

    -- BRONZE score
    b.score AS bronze_score,

    -- SILVER cleaned columns
    s.score_numeric,
    s.score_text,
    s.is_score_usable,
    s.score_exclusion_reason

FROM bronze_schema.cms_timely_care b
JOIN silver_schema.cms_timely_care s
    ON b.facility_id = s.facility_id
   AND b.measure_id  = s.measure_id

WHERE b.facility_id IN ('010001','010006')
ORDER BY b.facility_id, b.measure_id;

-- is_score_usable consistency: if score IS NULL, is_score_usable must be FALSE
-- These should all return 0
SELECT 'cms_timely_care' AS table_name,
       COUNT(*) AS broken_usability_flags
FROM silver_schema.cms_timely_care
WHERE score_numeric IS NULL AND score_text IS NULL AND is_score_usable = TRUE
UNION ALL
SELECT 'cms_complications',
       COUNT(*)
FROM silver_schema.cms_complications
WHERE score IS NULL AND is_score_usable = TRUE
UNION ALL
SELECT 'cms_outpatient_imaging',
       COUNT(*)
FROM silver_schema.cms_outpatient_imaging
WHERE score IS NULL AND is_score_usable = TRUE
UNION ALL
SELECT 'cms_infections',
       COUNT(*)
FROM silver_schema.cms_infections
WHERE score IS NULL AND is_score_usable = TRUE;

-- Issue fixed, the rows with code 8 had score_available true even though the original data showed N/A
SELECT
    *
FROM silver_schema.cms_infections
WHERE score IS NULL AND is_score_usable = TRUE;


SELECT score, footnote, is_score_usable, score_exclusion_reason, score_available
FROM silver_schema.cms_infections
WHERE footnote = '8';
--AND score IS NOT NULL;

-- Had to go back today to make sure my sql handles multiple values in the footnote and to change the score in all 4 tables to account for "N/A"

-- Did compared_to_national standardize correctly? If bronze had "No Different Than the National Rate", silver should have "No Different".
-- Did score cast from text to NUMERIC?
-- Did dates parse from 'MM/DD/YYYY' text to actual DATE type?
-- All checks passed, the not available and number of cases too small get mapped to NULL in silver
SELECT
    b.facility_id,
    b.measure_id,
    b.measure_name,

    -- compared_to_national
    b.compared_to_national AS bronze_compared,
    s.compared_to_national AS silver_compared,

    -- score
    b.score AS bronze_score,
    s.score AS silver_score_numeric,

    -- dates
    b.start_date AS bronze_start_date,
    s.start_date AS silver_start_date,

    b.end_date AS bronze_end_date,
    s.end_date AS silver_end_date

FROM bronze_schema.cms_complications b
JOIN silver_schema.cms_complications s
    ON b.facility_id = s.facility_id
   AND b.measure_id  = s.measure_id

WHERE b.facility_id = '010005'   -- replace with the one you want to check
ORDER BY b.measure_id;

--  Date sanity - no start date is larger than end date
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

-- Does the same measure_id in bronze and silver have the same score value?
-- Check passed
SELECT
    b.facility_id,
    b.measure_id,

    -- bronze values
    b.score AS bronze_score,

    -- silver values
    s.score AS silver_score,
    s.measure_suffix

FROM bronze_schema.cms_infections b
JOIN silver_schema.cms_infections s
    ON b.facility_id = s.facility_id
   AND b.measure_id  = s.measure_id

WHERE b.facility_id = '010001'
ORDER BY b.measure_id;

-- Join integrity - every facility_id in fact tables exists in hospital_general, returned 0's
-- Check passed
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


-- Checking if the new hospital_ownership column is correctly mapped to hospital_ownership_details
-- Check passed
SELECT
    hospital_ownership,
    hospital_ownership_details
FROM silver_schema.cms_hospital_general
LIMIT 80;

-- 12 distinct values preserved from bronze
SELECT
    DISTINCT hospital_ownership_details
FROM silver_schema.cms_hospital_general;


-- Checking if footnotes mapped correctly
SELECT
    facility_id,
    facility_name,
    mort_reporting_status,
    mort_group_footnote
FROM silver_schema.cms_hospital_general
WHERE mort_reporting_status = 'Federal (DoD, VA)'
LIMIT 50;

-- Correct mapping
SELECT
    facility_id,
    facility_name,
    mort_reporting_status,
    mort_group_footnote
FROM silver_schema.cms_hospital_general
WHERE mort_reporting_status = 'Not Participating'
LIMIT 50;

-- Correct mapping
SELECT
    facility_id,
    facility_name,
    mort_reporting_status,
    mort_group_footnote
FROM silver_schema.cms_hospital_general
WHERE mort_reporting_status = 'Data Quality Issue'
LIMIT 50;

-- Correct mapping
SELECT
    facility_id,
    facility_name,
    mort_reporting_status,
    mort_group_footnote
FROM silver_schema.cms_hospital_general
WHERE mort_reporting_status = 'Insufficient Data'
LIMIT 50;

-- score_exclusion_reason consistency: if is_score_usable = FALSE and score exists,
-- there must be an exclusion reason (not NULL)
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


-- Checking to see if any rows have both text and numeric values
-- Check passed
SELECT
    COUNT(*) FILTER ( WHERE score_numeric IS NOT NULL AND score_text IS NULL ) AS numerics,
    COUNT(*) FILTER ( WHERE score_numeric IS NULL AND score_text IS NOT NULL ) AS text,
    COUNT(*) FILTER ( WHERE score_numeric IS NOT NULL AND score_text IS NOT NULL ) both_populated,
    COUNT(*) FILTER ( WHERE score_numeric IS NULL AND score_text IS NULL ) both_null
FROM silver_schema.cms_timely_care;



-- Is the splitting on commas correct or does the LIKE/regex miss some cases like 2 matching inside 28?
-- Check passed

SELECT 'cms_timely_care' as tables, COUNT(*) AS incorrect
FROM silver_schema.cms_timely_care
WHERE is_score_usable = TRUE
  AND footnote IS NOT NULL
  AND ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
    && ARRAY['1','2','3','4','5','7','12','13','23','28','29']
UNION ALL
SELECT 'cms_complications', COUNT(*)
FROM silver_schema.cms_complications
WHERE is_score_usable = TRUE
  AND footnote IS NOT NULL
  AND ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
    && ARRAY['1','2','3','4','5','7','12','13','23','28','29']
UNION ALL
SELECT 'cms_outpatient_imaging' , COUNT(*)
FROM silver_schema.cms_outpatient_imaging
WHERE is_score_usable = TRUE
  AND footnote IS NOT NULL
  AND ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
    && ARRAY['1','2','3','4','5','7','12','13','23','28','29']
UNION ALL
SELECT 'cms_infections' , COUNT(*)
FROM silver_schema.cms_infections
WHERE is_score_usable = TRUE
  AND footnote IS NOT NULL
  AND ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
    && ARRAY['1','2','3','4','5','7','12','13','23','28','29'];


-- Suffix extraction was a success! 6 values returned
SELECT
    DISTINCT measure_suffix
FROM silver_schema.cms_infections;


-- Compared to national had 12 variants in bronze
SELECT
     DISTINCT compared_to_national unikat, 'bronze_cms_complications' as tables
FROM bronze_schema.cms_complications
UNION ALL
SELECT
     DISTINCT compared_to_national, 'bronze_cms_infections'
FROM bronze_schema.cms_infections;


-- In silver I changed it to 3 + NULL
SELECT
     DISTINCT compared_to_national unikat, 'silver_cms_complications' as tables
FROM silver_schema.cms_complications
UNION ALL
SELECT
     DISTINCT compared_to_national, 'silver_cms_infections'
FROM silver_schema.cms_infections;


-- How many hospitals don't have a single usable score? This is when is_score_usable is handy. I found that 198 hospitals have unusable scores across the board.
WITH all_scores AS (SELECT facility_id, is_score_usable
                    FROM silver_schema.cms_timely_care
                    UNION ALL
                    SELECT facility_id, is_score_usable
                    FROM silver_schema.cms_complications
                    UNION ALL
                    SELECT facility_id, is_score_usable
                    FROM silver_schema.cms_outpatient_imaging
                    UNION ALL
                    SELECT facility_id, is_score_usable
                    FROM silver_schema.cms_infections)

, zero_usable AS(
    SELECT
        facility_id
        FROM all_scores
        GROUP BY facility_id
        HAVING MAX(CASE WHEN is_score_usable THEN 1 ELSE 0 END) = 0

)

SELECT
    COUNT(*) hospitals_with_no_usable_scores
FROM zero_usable;


-- I wanted to investigate what type of hospitals they are, rural, non-participating, critical access ones, childrens
-- They are mostly children's hospitals followed by rural emergency and critical access hospitals and acute care hospitals
-- Consider excluding these hospitals in the gold layer, they might dilute averages and create a missleading comparison againts other acute care hospitals
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


-- Acute Care Hospitals with zero usable scores fall into two groups: non-participating (opted out entirely, including territories)
-- and insufficient data (mostly VA and small facilities lacking volume). Exclude all from Gold layer analysis.
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




