
-- How many rows are usable that are in 'Emergency Department' and what are their values?
-- OP_23 (Head CT Results has the most rows that are unusable ~31% the rest are above 45%
-- OP_22/23 are percentages - OP_22 is a percentage of people who left before being seen
-- OP_23 measures the % of patients who received head CT scan results within 45 minutes of arrival.
-- Use the OP_18 measures for analysis because it has actual wait times in minutes
SELECT
    measure_id,
    measure_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN is_score_usable = TRUE THEN 1 ELSE 0 END) AS usable_rows,
    ROUND(100 * SUM(CASE WHEN is_score_usable = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) pct_usable,
    MAX(score_numeric) AS max_score,
    MIN(score_numeric) AS min_score,
    ROUND(AVG(score_numeric), 2) AS avg_score
FROM silver_schema.cms_timely_care
WHERE condition = 'Emergency Department'
GROUP BY measure_id, measure_name;


-- Checking score distribution for key ED measures
-- OP_18a-d — ED wait times in minutes. Medians range from 151 to 294 minutes depending on the sub-measure (admitted vs discharged, before vs after).
-- OP_18c and OP_18d have extreme outliers (5,476 and 3,629 minutes, likely data quality issues or tiny hospitals with one long case skewing the number).
-- OP_22 — Left before being seen. Median is 1%, most hospitals lose very few patients to walkouts. Max 23%.
-- OP_23 — Head CT results within 45 minutes for stroke. Median 74%. This is a compliance rate (%), not a time measure.
SELECT
    measure_id,
    COUNT(*) AS n,
    ROUND(MIN(score_numeric), 2) AS min_val,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY score_numeric)::numeric, 2) AS p25,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY score_numeric)::numeric, 2) AS median,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY score_numeric)::numeric, 2) AS p75,
    ROUND(MAX(score_numeric), 2) AS max_val
FROM silver_schema.cms_timely_care
WHERE condition = 'Emergency Department'
  AND is_score_usable = TRUE
  AND score_numeric IS NOT NULL
GROUP BY measure_id
ORDER BY measure_id;


-- Understanding the outpatient_imaging table scores, all of the scores are percentages and for each one lower is better
-- OP-39 (Breast Screening Recall Rates) has nothing to do with imaging overuse, it measures false positive callbacks.
-- OP-10 has the most rows, consider pairing with OP_18b from timely_care
SELECT
    measure_id,
    measure_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN is_score_usable = TRUE THEN 1 ELSE 0 END) AS usable_rows,
    ROUND(100 * SUM(CASE WHEN is_score_usable = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) pct_usable,
    MAX(score) AS max_score,
    MIN(score) AS min_score,
    ROUND(AVG(score), 2) AS avg_score
FROM silver_schema.cms_outpatient_imaging
WHERE is_score_usable = TRUE
GROUP BY measure_id, measure_name;

-- Check to see if measures are consistent across hospitals or highly uneven
-- The results show that most outpatient imaging measures have a low or moderate median score with very wide ranges, meaning performance varies a lot between hospitals.
SELECT
    measure_id,
    measure_name,
    COUNT(*) AS n,
    ROUND(MIN(score), 2) AS min_val,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY score)::numeric, 2) AS p25,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY score)::numeric, 2) AS median,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY score)::numeric, 2) AS p75,
    ROUND(MAX(score), 2) AS max_val
FROM silver_schema.cms_outpatient_imaging
WHERE is_score_usable = TRUE
  AND score IS NOT NULL
GROUP BY measure_id, measure_name
ORDER BY measure_id;


-- OP-39 outliers are NOT data quality issues — they're full-size acute care hospitals (mostly 4-5 star Non-Profits in NY/NJ/CT).
-- High recall rates likely reflect defensive medicine culture or denser screening populations, not small-sample noise.
SELECT
    i.facility_id,
    h.facility_name,
    h.state,
    h.hospital_type,
    h.hospital_ownership,
    h.hospital_overall_rating,
    i.score
FROM silver_schema.cms_outpatient_imaging i
JOIN silver_schema.cms_hospital_general h ON i.facility_id = h.facility_id
WHERE i.measure_id = 'OP-39'
  AND i.is_score_usable = TRUE
  AND i.score IS NOT NULL
ORDER BY i.score DESC
LIMIT 50;

-- Can you average process-of-care scores across conditions?
-- Process-of-care scores can't be averaged raw across conditions, they mix
-- compliance rates (sepsis 65-92%), coverage rates (vaccination 74.5%),
-- A hospital's average would be dominated by whichever measures it reports on.
-- Decision: use percentile ranking within each measure_id to normalize across scales before aggregating.

SELECT
    condition,
    measure_id,
    measure_name,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN is_score_usable = TRUE THEN 1 ELSE 0 END) AS usable_rows,
    ROUND(100.0 * SUM(CASE WHEN is_score_usable = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_usable,
    ROUND(MIN(score_numeric), 2) AS min_score,
    ROUND(AVG(score_numeric) FILTER (WHERE is_score_usable), 2) AS avg_score,
    ROUND(MAX(score_numeric), 2) AS max_score
FROM silver_schema.cms_timely_care
WHERE condition != 'Emergency Department'
GROUP BY condition, measure_id, measure_name
HAVING SUM(CASE WHEN is_score_usable = TRUE THEN 1 ELSE 0 END) > 0
ORDER BY condition, measure_id;


-- Three categories by prefix: MORT (mortality), PSI (patient safety indicators),
-- COMP (complications). Most hospitals land on "No Different" across all three.
-- Using compared_to_national instead of raw scores because raw rates have different
-- base rates per measure (mortality 12.5/1000 vs PSI 0.8/1000) — can't average them.
-- compared_to_national is already normalized by CMS, so counting "Worse" ratings
-- across measures is valid without percentile ranking.

WITH categorized AS (
    SELECT
        CASE
            WHEN measure_id LIKE 'MORT%' THEN 'Mortality'
            WHEN measure_id LIKE 'PSI%'  THEN 'Patient Safety'
            WHEN measure_id LIKE 'COMP%' THEN 'Complications'
            ELSE 'Other'
        END AS category,
        compared_to_national
    FROM silver_schema.cms_complications
    WHERE is_score_usable = TRUE
      AND compared_to_national IS NOT NULL
)
SELECT
    category,
    compared_to_national,
    COUNT(*) AS n,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY category), 2) AS pct_within_category
FROM categorized
GROUP BY category, compared_to_national
ORDER BY category, compared_to_national;



-- Checking to see which hospitals have usable ed (4004), imaging scores (3936) and which ones have both (3761) over 69% of hospitals have usable scores
WITH ed_hospitals AS (SELECT DISTINCT facility_id
                      FROM silver_schema.cms_timely_care
                      WHERE condition = 'Emergency Department'
                        AND is_score_usable = TRUE
                        AND score_numeric IS NOT NULL)

, imaging_hospitals AS(
    SELECT DISTINCT facility_id
    FROM silver_schema.cms_outpatient_imaging
    WHERE is_score_usable
    AND score IS NOT NULL
)

SELECT
    (SELECT COUNT(*) FROM ed_hospitals) usable_ed,
    (SELECT COUNT(*) FROM imaging_hospitals) usable_imaging,
    COUNT(*) usable_with_both,
    ROUND(100.0 * COUNT(*) / 5426,2) pct_of_all_hospitals
FROM ed_hospitals ed JOIN imaging_hospitals ih ON ed.facility_id = ih.facility_id;



-- Checking to see  how many usable scores hospital ownership types have
-- Q1/Q2 sample (3,761 hospitals) skews toward Non-Profit (63.8% vs 54% overall).
-- For-Profit drops from 21% to 15.7%, Government from 24.6% to 20.5%. Tribal = 1, consider excluding.

WITH ed_hospitals AS (SELECT DISTINCT facility_id
                      FROM silver_schema.cms_timely_care
                      WHERE condition = 'Emergency Department'
                        AND is_score_usable = TRUE
                        AND score_numeric IS NOT NULL)

, imaging_hospitals AS(
    SELECT DISTINCT facility_id
    FROM silver_schema.cms_outpatient_imaging
    WHERE is_score_usable
    AND score IS NOT NULL
)

SELECT
    hospital_ownership,
    (SELECT COUNT(*) FROM ed_hospitals) usable_ed,
    (SELECT COUNT(*) FROM imaging_hospitals) usable_imaging,
    COUNT(*) usable_with_both,
    ROUND(100.0 * COUNT(*) / 5426,2) pct_of_all_hospitals
FROM ed_hospitals ed JOIN imaging_hospitals ih ON ed.facility_id = ih.facility_id
left join silver_schema.cms_hospital_general hg ON ed.facility_id = hg.facility_id
GROUP BY hospital_ownership;




-- Does ownership predict star rating?
-- Ownership does appear to be related to star rating, non-profit hospitals are much more concentrated in the higher ratings (3–4 stars dominate)
-- while for-profit hospitals have a noticeably larger share of 2-3 stars
-- Government hospitals sit somewhere in the middle, with a more balanced distribution across 2–4 stars
SELECT
    hospital_ownership,
    hospital_overall_rating,
    COUNT(*) AS n_hospitals,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY hospital_ownership), 2) AS pct_within_ownership
FROM silver_schema.cms_hospital_general
WHERE hospital_overall_rating IS NOT NULL
GROUP BY hospital_ownership, hospital_overall_rating
ORDER BY hospital_ownership, hospital_overall_rating;



-- Checking how many hospitals have usable complication scores same for timely_care
-- 4,120 hospitals have usable complication data, but only 3,494 have at least 3 rated measures,
-- meaning some hospitals don’t have enough outcome data for reliable comparison.
-- 3,139 hospitals have both strong complication data (3+ measures) and ED timely_care data,
-- so any analysis combining outcomes and process measures will be based on this smaller subset.
WITH complication_hospitals_not_null AS (SELECT facility_id,
                                                COUNT(*) AS rated_measures
                                         FROM silver_schema.cms_complications
                                         WHERE is_score_usable = TRUE
                                         AND compared_to_national IS NOT NULL
                                         GROUP BY facility_id)

, timely_care_hospitals AS(
    SELECT
        DISTINCT facility_id
    FROM silver_schema.cms_timely_care
    WHERE condition = 'Emergency Department'
    AND is_score_usable = TRUE
    AND score_numeric IS NOT NULL
)

SELECT
    (SELECT COUNT(*) FROM complication_hospitals_not_null) AS hospitals_with_comp,
    (SELECT COUNT(*) FROM complication_hospitals_not_null WHERE rated_measures >= 3) AS hospitals_with_comp_3plus,
    (SELECT COUNT(*) FROM timely_care_hospitals) AS hospitals_with_process_care,
    COUNT(*) AS hospitals_with_both
FROM complication_hospitals_not_null c
INNER JOIN timely_care_hospitals p ON c.facility_id = p.facility_id
WHERE c.rated_measures >= 3;


-- Mapping states to region before further analysis
SELECT
    state,
    CASE
        WHEN state IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'Northeast'
        WHEN state IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'Midwest'
        WHEN state IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'South'
        WHEN state IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'West'
        WHEN state IN ('PR','GU','VI','AS','MP') THEN 'Territory'
        ELSE 'UNMAPPED'
    END AS region,
    COUNT(*) AS n_hospitals
FROM silver_schema.cms_hospital_general
GROUP BY state, 2
ORDER BY region, state;


-- Quality checks to see if any states are not mapped
-- Check passed got nothing
SELECT DISTINCT state
FROM silver_schema.cms_hospital_general
WHERE state NOT IN (
    'CT','ME','MA','NH','RI','VT','NJ','NY','PA',
    'IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD',
    'DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX',
    'AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA',
    'PR','GU','VI','AS','MP'
);



-- What are the worst hospitals on complications and checks what their process-of-care scores look like?
-- Checking their process-of-care scores to see if bad outcomes correlate with bad compliance.
-- Hospitals that perform worse than the national average in many complication measures still often have average process-of-care scores
-- and not always very low star ratings, which suggests poor outcomes are not always explained by weak process performance alone
WITH worse_hospitals AS (
    SELECT facility_id,
           COUNT(*) AS worse_measure_count
    FROM silver_schema.cms_complications
    WHERE is_score_usable = TRUE
      AND compared_to_national = 'Worse'
    GROUP BY facility_id
    HAVING COUNT(*) >= 3
)
SELECT
    w.facility_id,
    h.hospital_ownership,
    h.hospital_overall_rating,
    w.worse_measure_count,
    COUNT(t.measure_id) AS process_care_measures,
    ROUND(AVG(t.score_numeric), 2) AS avg_process_care_score
FROM worse_hospitals w
JOIN silver_schema.cms_hospital_general h ON w.facility_id = h.facility_id
LEFT JOIN silver_schema.cms_timely_care t
    ON w.facility_id = t.facility_id
    AND t.condition != 'Emergency Department'
    AND t.is_score_usable = TRUE
    AND t.score_numeric IS NOT NULL
GROUP BY w.facility_id, h.hospital_ownership, h.hospital_overall_rating, w.worse_measure_count
ORDER BY w.worse_measure_count DESC
LIMIT 50;



-- If any measure_id appears in multiple tables, a unified dim_measure needs a composite key
-- No measure_id appears in more than one table, so a unified dim_measure can use measure_id as a simple primary key (no composite key needed).
WITH all_measures AS (
    SELECT DISTINCT measure_id, 'timely_care' AS source_table FROM silver_schema.cms_timely_care
    UNION ALL
    SELECT DISTINCT measure_id, 'complications' FROM silver_schema.cms_complications
    UNION ALL
    SELECT DISTINCT measure_id, 'outpatient_imaging' FROM silver_schema.cms_outpatient_imaging
    UNION ALL
    SELECT DISTINCT measure_id, 'infections' FROM silver_schema.cms_infections
)
SELECT
    measure_id,
    COUNT(DISTINCT source_table) AS n_tables
FROM all_measures
GROUP BY measure_id
HAVING COUNT(DISTINCT source_table) > 1
ORDER BY measure_id;

-- Check the measure counts for each table
-- The measure counts are very different across tables (36 vs 30 vs 20 vs 4), which confirms each table represents a different type of measures rather than one shared measurement system.
SELECT 'timely_care' AS source, COUNT(DISTINCT measure_id) AS n_measures FROM silver_schema.cms_timely_care
UNION ALL
SELECT 'complications', COUNT(DISTINCT measure_id) FROM silver_schema.cms_complications
UNION ALL
SELECT 'outpatient_imaging', COUNT(DISTINCT measure_id) FROM silver_schema.cms_outpatient_imaging
UNION ALL
SELECT 'infections', COUNT(DISTINCT measure_id) FROM silver_schema.cms_infections
ORDER BY n_measures DESC;


-- Getting the final column structure for all tables
SELECT column_name, data_type, table_name
FROM information_schema.columns
WHERE table_schema = 'silver_schema'
  AND table_name IN ('cms_timely_care', 'cms_complications', 'cms_outpatient_imaging', 'cms_infections')
ORDER BY column_name, table_name;


SELECT
    measure_id,
    COUNT(*) total_rows,
    SUM(CASE WHEN score_available = TRUE THEN 1 ELSE 0 END) score_available_num,
    SUM(CASE WHEN is_score_usable = TRUE THEN 1 ELSE 0 END) usable_rows_num,
    ROUND(100.0 * SUM(CASE WHEN score_available = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) pct_available,
    ROUND(100.0 * SUM(CASE WHEN is_score_usable = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) pct_usable,
    ROUND(100.0 * (SUM(CASE WHEN score_available = TRUE THEN 1 ELSE 0 END)
        - SUM(CASE WHEN is_score_usable = TRUE THEN 1 ELSE 0 END)) / COUNT(*), 2) AS availability_usability_gap_pct
FROM silver_schema.cms_infections
GROUP BY measure_id
ORDER BY availability_usability_gap_pct DESC;


