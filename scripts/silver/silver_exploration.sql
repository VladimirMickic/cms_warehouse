-- =============================================================================
-- Silver Layer: Exploration for Gold Layer Design
-- =============================================================================
-- Purpose:
--     Understand the data patterns needed to build Gold layer tables and views.
--     Each section maps to a Gold design decision or business question.
--     Run these queries and review results before building Gold.
-- =============================================================================


-- =============================================================================
-- Section 1: Measure Inventory
-- =============================================================================

-- ED measures: 6 total. OP_18a-d are wait times in minutes.
-- OP_22 is % left-before-seen, OP_23 is head CT compliance rate — both percentages, not minutes.
-- OP_23 has the worst coverage (~31% unusable). Only OP_18 measures are valid for ED wait analysis.
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


-- Imaging measures: all percentages, lower is better.
-- OP-8 median is 36.5% vs 3-7% for the others, completely different scale, must exclude from average.
-- OP-10 has the most usable rows (3,846), best candidate for pairing with OP_18b.
-- OP-39 measures breast screening recall rates (false positive callbacks), not imaging overuse —
-- include it since it's still an efficiency measure on the same percentage scale.
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


-- Process-of-care scores can't be averaged raw across conditions: they mix
-- compliance rates (sepsis 65-92%), coverage rates (vaccination 74.5%).
-- A hospital's average would be dominated by whichever measures it reports on.
-- Sepsis measures have only 18-25% usable rows — the lowest of any condition.
-- Electronic Clinical Quality measures (HH_HYPER/HYPO/ORAE) are harm rates, not percentages.
-- OP_40 (STEMI) is likely a time measure (~46 avg), not a percentage.
-- These should be excluded from any process-of-care average or handled separately.
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


-- Three complication categories by prefix: MORT (mortality), PSI (patient safety), COMP (complications).
-- Most hospitals land on "No Different" across all three.
-- Raw scores can't be averaged, base rates differ wildly (mortality 12.5/1000 vs PSI 0.8/1000).
-- FINDING: Mortality is the only category with meaningful differentiation (5.6% Better, 3.1% Worse).
-- COMP% and PSI% both cluster at 97%+ "No Different", essentially noise for hospital differentiation.
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


-- Complications by ownership: all ownership types are 94-97% "No Different".
-- Government shows a slight skew toward "Worse" (~3.1%)
SELECT
    hg.hospital_ownership,
    c.compared_to_national,
    COUNT(*) AS n_hospitals,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(PARTITION BY hg.hospital_ownership), 2) AS pct_of_total_per_ownership
FROM silver_schema.cms_complications AS c
JOIN silver_schema.cms_hospital_general AS hg
    ON c.facility_id = hg.facility_id
WHERE c.is_score_usable = TRUE
  AND c.compared_to_national IS NOT NULL
GROUP BY hg.hospital_ownership, c.compared_to_national
ORDER BY hg.hospital_ownership, c.compared_to_national;


-- =============================================================================
-- Section 2: Sample Sizes
-- =============================================================================

-- 4,004 hospitals have usable ED scores, 3,936 have usable imaging.
-- 3,761 hospitals have BOTH — 69.3% of all hospitals.
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


-- Survivorship bias check
-- For-Profit drops from 21% to 15.7%, Government from 24.6% to 20.5%.
-- Tribal drops to 1 hospital, consider excluding from analysis.
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
LEFT JOIN silver_schema.cms_hospital_general hg ON ed.facility_id = hg.facility_id
GROUP BY hospital_ownership;


-- 4,120 hospitals have usable complication data, 3,494 have 3+ rated measures.
-- 3,139 hospitals have BOTH strong complication data AND usable ED timely_care scores.
-- The 3+ measure threshold costs around 600 hospitals but prevents noise from 0%/100% binary results.
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
    (SELECT COUNT(*) FROM timely_care_hospitals) AS hospitals_timely_care,
    COUNT(*) AS hospitals_with_both
FROM complication_hospitals_not_null c
JOIN timely_care_hospitals p ON c.facility_id = p.facility_id
WHERE c.rated_measures >= 3;


-- =============================================================================
-- Section 3: Distributions
-- =============================================================================

-- ED score percentiles: OP_18a/b are cleanest (n~3,750, IQR ~70 min, no extreme outliers).
-- OP_18c max=5,476 min (~3.8 days) and OP_18d max=3,629 min (~2.5 days) are data artifacts from tiny hospitals
-- Gold layer will cap at 1,440 minutes (24 hours) to remove these.
-- OP_22 has very tight distribution (1-2%), not useful as an analytical axis.
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


-- Imaging score percentiles: OP-10 strongest (n=3,846, IQR 3-7.7%).
-- OP-8 is on a completely different scale (median 36.5% vs ~5%) with only 612 hospitals — exclude.
-- OP-39 max of 79.5% vs median 6.7% warranted outlier investigation.
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


-- OP-39 outlier investigation: high recall rates are NOT data quality issues.
-- They're full-size acute care hospitals (mostly 4-5 star Non-Profits in NY/NJ/CT).
-- High recall rates likely reflect defensive medicine culture or denser screening populations, not small-sample noise.
-- No exclusion needed.
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


-- Ownership vs star rating: Non-Profit concentrated at 3-4 stars, For-Profit heavier at 2-3 stars,
-- Government balanced across 2-4 stars. Ownership does predict star rating
SELECT
    hospital_ownership,
    hospital_overall_rating,
    COUNT(*) AS n_hospitals,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY hospital_ownership), 2) AS pct_within_ownership
FROM silver_schema.cms_hospital_general
WHERE hospital_overall_rating IS NOT NULL
GROUP BY hospital_ownership, hospital_overall_rating
ORDER BY hospital_ownership, hospital_overall_rating;


-- =============================================================================
-- Section 4: Pre-Gold Design Decisions
-- =============================================================================

-- Region mapping, 4 regions plus 1 territory
-- Territories grouped separately, 87-100% unusable scores, excluded from analytical views.
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

-- Unmapped states check: 0 rows returned. All states covered.
SELECT DISTINCT state
FROM silver_schema.cms_hospital_general
WHERE state NOT IN (
    'CT','ME','MA','NH','RI','VT','NJ','NY','PA',
    'IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD',
    'DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX',
    'AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA',
    'PR','GU','VI','AS','MP'
);


-- Measure dimension feasibility: no measure_id appears in more than one table.
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

-- 90 distinct measures total: infections 36, timely_care 30, complications 20, imaging 4.
-- Very different counts confirm each table represents a different measurement domain.
SELECT 'timely_care' AS source, COUNT(DISTINCT measure_id) AS n_measures FROM silver_schema.cms_timely_care
UNION ALL
SELECT 'complications', COUNT(DISTINCT measure_id) FROM silver_schema.cms_complications
UNION ALL
SELECT 'outpatient_imaging', COUNT(DISTINCT measure_id) FROM silver_schema.cms_outpatient_imaging
UNION ALL
SELECT 'infections', COUNT(DISTINCT measure_id) FROM silver_schema.cms_infections
ORDER BY n_measures DESC;


-- Score availability vs usability gap: identifies measures where CMS provides a number but footnotes flag it as unreliable.
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


-- =============================================================================
-- Section 5: Worst Complication Hospitals vs Process-of-Care
-- =============================================================================

-- Among the 30 worst complication hospitals (3+ "Worse" ratings), process-of-care scores range from 42 to 84 — no clear pattern.
-- Hospital 180044 has 5 "Worse" ratings but scores 83.55 on process-of-care (high compliance, still unsafe).
--  Hospital 050145 has 4 "Worse" ratings and scores only 42.17 (low compliance AND unsafe).
-- Both patterns exist: process compliance alone doesn't guarantee safety.
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


-- =============================================================================
-- Section 6: Statistical Analysis — Correlation
-- =============================================================================

-- RESULT: n=3755  avg_imaging=6.05 | avg_ed=214.34 | r = -0.0197 | R^2 = 0.0004
-- r near zero means no linear relationship between imaging overuse and ED wait times.
-- R^2 = 0.0004% of variance explained, essentially noise.
-- FINDING: Imaging overuse and ED delays are independent problems.
-- A hospital with high CT overuse is no more likely to have long ED waits.
WITH imaging_scores AS (
    SELECT
        facility_id,
        ROUND(AVG(score), 2) AS avg_imaging_score
    FROM silver_schema.cms_outpatient_imaging
    WHERE is_score_usable = TRUE
      AND score IS NOT NULL
      AND measure_id IN ('OP-10', 'OP-13', 'OP-39')
    GROUP BY facility_id
),
ed_scores AS (
    SELECT
        facility_id,
        ROUND(AVG(score_numeric), 2) AS avg_ed_wait_minutes
    FROM silver_schema.cms_timely_care
    WHERE measure_id IN ('OP_18a', 'OP_18b', 'OP_18c', 'OP_18d')
      AND is_score_usable = TRUE
      AND score_numeric IS NOT NULL
      AND score_numeric <= 1440
    GROUP BY facility_id
)
SELECT
    COUNT(*) AS sample_size,
    ROUND(AVG(i.avg_imaging_score), 2) AS avg_imaging_score,
    ROUND(STDDEV(i.avg_imaging_score), 2) AS sd_imaging,
    ROUND(AVG(e.avg_ed_wait_minutes), 2) AS avg_ed_wait_minutes,
    ROUND(STDDEV(e.avg_ed_wait_minutes), 2) AS sd_ed_wait,
    ROUND(CORR(i.avg_imaging_score, e.avg_ed_wait_minutes)::NUMERIC, 4) AS pearson_r,
    ROUND(POWER(CORR(i.avg_imaging_score, e.avg_ed_wait_minutes), 2)::NUMERIC, 4) AS r_squared
FROM imaging_scores i
JOIN ed_scores e ON i.facility_id = e.facility_id;


-- Correlation near zero across all three ownership types (r: -0.01 to -0.04), so ownership
-- doesn't moderate the imaging/ED relationship — they're independent problems no matter who runs the hospital.
-- One thing worth noting: Non-Profits have the longest ED waits (221 min) despite the highest star ratings overall.

WITH imaging_scores AS (
    SELECT
        facility_id,
        ROUND(AVG(score), 2) AS avg_imaging_score
    FROM silver_schema.cms_outpatient_imaging
    WHERE is_score_usable = TRUE
      AND score IS NOT NULL
      AND measure_id IN ('OP-10', 'OP-13', 'OP-39')
    GROUP BY facility_id
),
ed_scores AS (
    SELECT
        facility_id,
        ROUND(AVG(score_numeric), 2) AS avg_ed_wait_minutes
    FROM silver_schema.cms_timely_care
    WHERE measure_id IN ('OP_18a', 'OP_18b', 'OP_18c', 'OP_18d')
      AND is_score_usable = TRUE
      AND score_numeric IS NOT NULL
      AND score_numeric <= 1440
    GROUP BY facility_id
)
SELECT
    hg.hospital_ownership,
    COUNT(*) AS n_hospitals,
    ROUND(AVG(i.avg_imaging_score), 2) AS avg_imaging_score,
    ROUND(STDDEV(i.avg_imaging_score), 2) AS sd_imaging,
    ROUND(AVG(e.avg_ed_wait_minutes), 2) AS avg_ed_wait_minutes,
    ROUND(STDDEV(e.avg_ed_wait_minutes), 2) AS sd_ed_wait,
    ROUND(CORR(i.avg_imaging_score, e.avg_ed_wait_minutes)::NUMERIC, 4) AS pearson_r
FROM imaging_scores i
JOIN ed_scores e ON i.facility_id = e.facility_id
JOIN silver_schema.cms_hospital_general hg ON i.facility_id = hg.facility_id
WHERE hg.hospital_ownership != 'Tribal'
GROUP BY hg.hospital_ownership
ORDER BY n_hospitals DESC;
