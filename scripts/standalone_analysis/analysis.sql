-- sql_analysis.sql
-- 4 standalone analyses beyond the Tableau dashboard, run in DataGrip.
-- Q1 and Q2 query silver directly, Q3 uses both silver and the gold view,
-- Q4 is a deep dive into a specific hospital benchmarked against its peers.


-- Q1: How does hospital ownership type affect quality ratings?
--
-- Government, non-profit, and for-profit hospitals all operate under different
-- incentive structures so I wanted to see if those differences actually show up
-- in CMS star ratings. I also broke ownership into subtypes because "government"
-- includes everything from a small county hospital to the VA system, and lumping
-- them together hides a lot.

-- Overview by ownership group
-- Non-profit hospitals dominate the sample and have the highest avg rating (3.18) with the largest share of high performers (~40%), suggesting more consistent quality outcomes.
-- Government hospitals have similar average ratings (3.13) but much lower reporting rates and higher variability, indicating uneven performance across facilities.
-- For-profit hospitals lag significantly (avg 2.66) with nearly half classified as low performers, pointing to a clear negative skew in quality ratings.
SELECT
    hospital_ownership,
    COUNT(*) AS total_hospitals,
    COUNT(hospital_overall_rating) AS hospitals_rated,
    COUNT(*) - COUNT(hospital_overall_rating) AS hospitals_unrated,
    ROUND(100.0 * COUNT(hospital_overall_rating) / COUNT(*), 1) AS pct_rated,
    ROUND(AVG(hospital_overall_rating), 2) AS avg_star_rating,
    ROUND(STDDEV(hospital_overall_rating), 2) AS stddev_rating,
    COUNT(*) FILTER (WHERE hospital_overall_rating >= 4)  AS n_high_performers,
    ROUND(100.0 * COUNT(*) FILTER (WHERE hospital_overall_rating >= 4)
        / NULLIF(COUNT(hospital_overall_rating), 0), 1)  AS pct_high_performers,
    COUNT(*) FILTER (WHERE hospital_overall_rating <= 2) AS n_low_performers,
    ROUND(100.0 * COUNT(*) FILTER (WHERE hospital_overall_rating <= 2)
        / NULLIF(COUNT(hospital_overall_rating), 0), 1) AS pct_low_performers
FROM silver_schema.cms_hospital_general
WHERE hospital_ownership != 'Tribal'
GROUP BY hospital_ownership
ORDER BY total_hospitals DESC;

-- Star rating distribution per ownership type
-- For-profit hospitals are heavily skewed toward low ratings, with ~48% in 1–2 stars and only ~23% in 4–5, confirming a clear quality imbalance.
-- Non-profits show a more favorable distribution, with the majority clustered in 3–4 stars and a much smaller share at the bottom end.
-- Government hospitals sit in between, with a relatively balanced spread but more weight in mid-tier (3 stars), suggesting average but inconsistent performance.
SELECT
    hospital_ownership,
    hospital_overall_rating  AS stars,
    COUNT(*) AS n_hospitals,
    ROUND(100.0 * COUNT(*)
        / SUM(COUNT(*)) OVER (PARTITION BY hospital_ownership), 1) AS pct_within_ownership
FROM silver_schema.cms_hospital_general
WHERE hospital_overall_rating IS NOT NULL
GROUP BY hospital_ownership, hospital_overall_rating
ORDER BY hospital_ownership, hospital_overall_rating;

-- Sub-type breakdown, do government subtypes behave differently from each other?
-- Government hospitals are highly heterogeneous: VA hospitals stand out with elite performance (avg 4.2, 77% high performers) while local/state facilities skew heavily toward low ratings.
-- For-profit splits reveal a stark contrast: physician-owned hospitals outperform (3.32 avg) while proprietary hospitals drive the overall sector’s poor results (~49% low performers).
-- Non-profits are consistently strong across all subtypes, with stable averages (~3.2) and ~40% high performers, reinforcing their overall quality advantage.
SELECT
    hospital_ownership  AS ownership_group,
    hospital_ownership_details AS ownership_subtype,
    COUNT(*) AS total_hospitals,
    COUNT(hospital_overall_rating) AS rated,
    ROUND(AVG(hospital_overall_rating), 2) AS avg_rating,
    ROUND(100.0 * COUNT(*) FILTER (WHERE hospital_overall_rating >= 4)
        / NULLIF(COUNT(hospital_overall_rating), 0), 1) AS pct_high_performers,
    ROUND(100.0 * COUNT(*) FILTER (WHERE hospital_overall_rating <= 2)
        / NULLIF(COUNT(hospital_overall_rating), 0), 1) AS pct_low_performers
FROM silver_schema.cms_hospital_general
GROUP BY hospital_ownership, hospital_ownership_details
ORDER BY hospital_ownership, total_hospitals DESC;


-- Q2: Do hospitals that report more measures actually score better?
--
-- Hospitals that report on more quality measures might have better infrastructure
-- and more resources, or they might just be bigger. I wanted to check if reporting
-- completeness correlates with star ratings or if they are independent. CMS tracks
-- how many measures each hospital reports out of the total possible across 5 groups
-- (mortality, safety, readmissions, patient experience, timely/effective care).

-- Average completeness by star rating
-- Reporting completeness is high across all star levels (~73–78%) with minimal variation, suggesting most hospitals report a similar share of measures.
-- Surprisingly, 5-star hospitals have lower average completeness (73.1%) than 2–4 star hospitals, contradicting the idea that more reporting = better outcomes.
-- Overall, no clear upward trend between completeness and ratings, indicating reporting volume alone is not a strong driver of quality scores.
WITH hospital_completeness AS (
    SELECT
        facility_id,
        hospital_overall_rating,
        hospital_ownership,
        (COALESCE(facility_mort_measures_count, 0)
         + COALESCE(facility_safety_measures_count, 0)
         + COALESCE(facility_readm_measures_count, 0)
         + COALESCE(facility_pt_exp_measures_count, 0)
         + COALESCE(facility_te_measures_count, 0)) AS facility_measures_reported,
        (COALESCE(mort_group_measures_count, 0)
         + COALESCE(safety_group_measures_count, 0)
         + COALESCE(readm_group_measures_count, 0)
         + COALESCE(pt_exp_group_measures_count, 0)
         + COALESCE(te_group_measures_count, 0)) AS group_measures_total,
        ROUND(
            100.0
            * (COALESCE(facility_mort_measures_count, 0)
               + COALESCE(facility_safety_measures_count, 0)
               + COALESCE(facility_readm_measures_count, 0)
               + COALESCE(facility_pt_exp_measures_count, 0)
               + COALESCE(facility_te_measures_count, 0))
            / NULLIF(
                COALESCE(mort_group_measures_count, 0)
                + COALESCE(safety_group_measures_count, 0)
                + COALESCE(readm_group_measures_count, 0)
                + COALESCE(pt_exp_group_measures_count, 0)
                + COALESCE(te_group_measures_count, 0)
            , 0), 1) AS reporting_completeness_pct
    FROM silver_schema.cms_hospital_general
)
SELECT
    hospital_overall_rating AS stars,
    COUNT(*)  AS n_hospitals,
    ROUND(AVG(reporting_completeness_pct), 1) AS avg_completeness_pct,
    ROUND(MIN(reporting_completeness_pct), 1) AS min_completeness_pct,
    ROUND(MAX(reporting_completeness_pct), 1) AS max_completeness_pct
FROM hospital_completeness
WHERE hospital_overall_rating IS NOT NULL
GROUP BY hospital_overall_rating
ORDER BY hospital_overall_rating;

-- Pearson correlation, reporting completeness vs star rating
-- The correlation between reporting completeness and star rating is effectively zero (r = -0.0406), indicating no meaningful linear relationship.
-- The extremely low r_squared (0.0017) confirms that reporting completeness explains almost none of the variation in hospital quality scores.
WITH hospital_completeness AS (
    SELECT
        hospital_overall_rating,
        ROUND(
            100.0
            * (COALESCE(facility_mort_measures_count, 0)
               + COALESCE(facility_safety_measures_count, 0)
               + COALESCE(facility_readm_measures_count, 0)
               + COALESCE(facility_pt_exp_measures_count, 0)
               + COALESCE(facility_te_measures_count, 0))
            / NULLIF(
                COALESCE(mort_group_measures_count, 0)
                + COALESCE(safety_group_measures_count, 0)
                + COALESCE(readm_group_measures_count, 0)
                + COALESCE(pt_exp_group_measures_count, 0)
                + COALESCE(te_group_measures_count, 0)
            , 0), 1)  AS reporting_completeness_pct
    FROM silver_schema.cms_hospital_general
    WHERE hospital_overall_rating IS NOT NULL
)
SELECT
    COUNT(*) AS n,
    ROUND(CORR(reporting_completeness_pct, hospital_overall_rating)::NUMERIC, 4) AS pearson_r,
    ROUND(POWER(CORR(reporting_completeness_pct, hospital_overall_rating), 2)::NUMERIC, 4) AS r_squared
FROM hospital_completeness;



-- most hospitals (83%) report some measure groups but not all, this mixed state is the norm, not an edge case
-- the 84 that report everything score highest (3.38 stars), which probably says more about their organizational capacity than their clinical quality
-- the 800 not-participating hospitals drop off the map entirely: no stars, no data, no way for patients to compare them

SELECT
    CASE
        WHEN mort_reporting_status    = 'Fully Rated'
         AND safety_reporting_status  = 'Fully Rated'
         AND readm_reporting_status   = 'Fully Rated'
         AND pt_exp_reporting_status  = 'Fully Rated'
         AND te_reporting_status      = 'Fully Rated' THEN 'All Groups Fully Rated'
        WHEN mort_reporting_status    = 'Not Participating'
          OR safety_reporting_status  = 'Not Participating'
          OR readm_reporting_status   = 'Not Participating'
          OR pt_exp_reporting_status  = 'Not Participating'
          OR te_reporting_status      = 'Not Participating' THEN 'At Least One Not Participating'
        WHEN mort_reporting_status    = 'Federal (DoD, VA)'
          OR safety_reporting_status  = 'Federal (DoD, VA)'
          OR readm_reporting_status   = 'Federal (DoD, VA)'
          OR pt_exp_reporting_status  = 'Federal (DoD, VA)'
          OR te_reporting_status      = 'Federal (DoD, VA)' THEN 'Federal Hospital'
        ELSE 'Mixed / Partial'
    END AS reporting_profile,
    COUNT(*) AS n_hospitals,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_total,
    ROUND(AVG(hospital_overall_rating), 2) AS avg_star_rating,
    COUNT(hospital_overall_rating) AS rated_hospitals
FROM silver_schema.cms_hospital_general
GROUP BY 1
ORDER BY n_hospitals DESC;


-- Q3: Do infections, complications, and process compliance actually go together?
--
-- Infections and complications are both "things that go wrong" in a hospital but they
-- come from different tables and different measurement systems. I wanted to know if they
-- cluster in the same hospitals or if a hospital can have great infection control and
-- still be dangerous on the complications side. If they are independent then fixing one
-- problem won't fix the other and resources need to be allocated separately.
--
-- The second part flips the question: instead of comparing two outcome measures, I compared
-- process-of-care compliance (do they follow the checklists?) against complication outcomes
-- (do patients actually do better?). If checklists guaranteed good outcomes we would see a
-- strong negative correlation. If they don't, that challenges the assumption that ticking
-- boxes = quality. I used the gold view vw_complications_vs_process_care for this part
-- since it already joins complications to process-of-care scores and filters to hospitals
-- with at least 3 rated complication measures.

-- Infections vs complications, overall correlation
-- SIR > 1.0 = more infections than the national benchmark predicts
-- complication_pct_worse = % of complication measures rated "Worse Than National"
-- r = 0.085, r_squared = 0.007, essentially no relationship. hospitals with high infection rates are not
-- systematically worse at complications, and vice versa. two separate problems, not one.
-- worth noting: avg SIR of 0.57 means this joined sample skews toward better-than-benchmark hospitals,
-- and avg complication_pct_worse of 2.43% shows most cluster at "No Different"
WITH infection_performance AS (
    SELECT
        facility_id,
        ROUND(AVG(score), 2) AS avg_sir,
        COUNT(*) AS sir_measures_used
    FROM silver_schema.cms_infections
    WHERE measure_suffix = 'SIR'
      AND is_score_usable = TRUE
      AND score IS NOT NULL
    GROUP BY facility_id
),
complication_performance AS (
    SELECT
        facility_id,
        COUNT(*) FILTER (WHERE compared_to_national IS NOT NULL)  AS comp_rated_total,
        ROUND(100.0 * COUNT(*) FILTER (WHERE compared_to_national = 'Worse')
            / NULLIF(COUNT(*) FILTER (WHERE compared_to_national IS NOT NULL), 0), 2)
            AS complication_pct_worse
    FROM silver_schema.cms_complications
    WHERE is_score_usable = TRUE
    GROUP BY facility_id
    HAVING COUNT(*) FILTER (WHERE compared_to_national IS NOT NULL) >= 3
)
SELECT
    COUNT(*) AS sample_size,
    ROUND(AVG(i.avg_sir), 3) AS avg_sir,
    ROUND(STDDEV(i.avg_sir), 3) AS sd_sir,
    ROUND(AVG(c.complication_pct_worse), 2) AS avg_complication_pct_worse,
    ROUND(STDDEV(c.complication_pct_worse), 2) AS sd_complication_pct_worse,
    ROUND(CORR(i.avg_sir, c.complication_pct_worse)::NUMERIC, 4) AS pearson_r,
    ROUND(POWER(CORR(i.avg_sir, c.complication_pct_worse), 2)::NUMERIC, 4) AS r_squared
FROM infection_performance i
INNER JOIN complication_performance c ON i.facility_id = c.facility_id;

-- Process compliance vs complications, distribution and sample size
-- median complication and mortality pct_worse are both 0, most hospitals have zero "Worse" ratings.
-- the non-zero averages are a handful of bad outliers, not widespread underperformance.
-- process compliance is the opposite: most score high (median 85.5%) but the floor hits 5%, a short tail of genuinely bad actors pulling the mean down.

SELECT
    COUNT(*) AS n_hospitals,
    ROUND(AVG(avg_process_care_score), 2) AS avg_process_compliance,
    ROUND(AVG(complication_pct_worse), 2) AS avg_complication_pct_worse,
    ROUND(AVG(mort_pct_worse), 2) AS avg_mort_pct_worse,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
        (ORDER BY avg_process_care_score)::NUMERIC, 2) AS median_process_compliance,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
        (ORDER BY complication_pct_worse)::NUMERIC, 2) AS median_complication_pct_worse,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
        (ORDER BY mort_pct_worse)::NUMERIC, 2) AS median_mort_pct_worse,
    MIN(avg_process_care_score) AS min_process_compliance,
    MAX(avg_process_care_score) AS max_process_compliance,
    MIN(complication_pct_worse) AS min_complication_pct_worse,
    MAX(complication_pct_worse) AS max_complication_pct_worse
FROM gold_schema.vw_complications_vs_process_care;

-- Does following protocols predict better outcomes?
-- short answer: no. r values are all near zero across complications, mortality, and PSI.
-- process compliance as measured here (colonoscopy care, staff vaccination) explains
-- essentially 0% of outcome variance these measures track narrow procedural boxes, not clinical quality.
-- the weak PSI signal (r = 0.08) is the only hint of a relationship, and it's barely there.

SELECT
    COUNT(*) AS n_hospitals,
    ROUND(CORR(avg_process_care_score,
               complication_pct_worse)::NUMERIC, 4) AS r_process_vs_complications,
    ROUND(CORR(avg_process_care_score,
               mort_pct_worse)::NUMERIC, 4) AS r_process_vs_mortality,
    ROUND(CORR(avg_process_care_score,
               psi_pct_worse)::NUMERIC, 4) AS r_process_vs_psi,
    ROUND(POWER(CORR(avg_process_care_score,
                     complication_pct_worse), 2)::NUMERIC, 4) AS r_squared_overall,
    ROUND(POWER(CORR(avg_process_care_score,
                     mort_pct_worse), 2)::NUMERIC, 4)  AS r_squared_mortality
FROM gold_schema.vw_complications_vs_process_care;



-- Q4: How is UPMC Presbyterian Shadyside actually doing?
--
-- A friend of mine works at this hospital and was curious how they compare to other
-- hospitals in the state. I pulled their scores across all 4 fact domains (complications,
-- ED wait times, imaging overuse, infections) and benchmarked them against PA non-profit
-- acute care peers. The last query compares their weakest measures to what 5-star hospitals
-- in the same peer group score, so she can see exactly where the gaps are.

-- Hospital profile
SELECT
    facility_id,
    facility_name,
    city,
    state,
    hospital_type,
    hospital_ownership,
    hospital_overall_rating AS stars,
    emergency_services,
    mort_reporting_status,
    safety_reporting_status,
    readm_reporting_status,
    pt_exp_reporting_status,
    te_reporting_status
FROM silver_schema.cms_hospital_general
WHERE facility_name ILIKE '%presbyterian shadyside%';

-- Complication scores, Better / No Different / Worse on every measure
-- Worse only on pressure ulcer rate and CMS Medicare PSI 90: Patient safety and adverse events composite
SELECT
    measure_id,
    measure_name,
    score,
    compared_to_national,
    is_score_usable,
    score_exclusion_reason
FROM silver_schema.cms_complications
WHERE facility_id = '390164'
ORDER BY compared_to_national NULLS LAST, measure_id;

-- ED wait times vs PA average (OP_18a-d, minutes, higher = worse)
-- UPMC runs 31-40 min slower than the PA average on 18a/18b (discharged and admitted patients) --
-- but 133-149 min faster on 18c/18d. the flip likely reflects patient mix: 18c/18d capture
-- higher-acuity pathways where a large academic center has the staff and systems to move quickly.
-- routine ED visits are where UPMC falls behind its peers.

WITH pa_avg AS (
    SELECT
        measure_id,
        ROUND(AVG(score_numeric), 1) AS pa_avg_wait
    FROM silver_schema.cms_timely_care t
    JOIN silver_schema.cms_hospital_general h USING (facility_id)
    WHERE h.state = 'PA'
      AND t.measure_id IN ('OP_18a', 'OP_18b', 'OP_18c', 'OP_18d')
      AND t.is_score_usable = TRUE
      AND t.score_numeric IS NOT NULL
    GROUP BY measure_id
)
SELECT
    u.measure_id,
    u.score_numeric AS upmc_wait_minutes,
    p.pa_avg_wait AS pa_avg_wait_minutes,
    ROUND(u.score_numeric - p.pa_avg_wait, 1) AS vs_pa_diff
FROM silver_schema.cms_timely_care u
JOIN pa_avg p USING (measure_id)
WHERE u.facility_id = '390164'
  AND u.measure_id IN ('OP_18a', 'OP_18b', 'OP_18c', 'OP_18d')
  AND u.is_score_usable = TRUE
ORDER BY u.measure_id;

-- Imaging overuse vs PA average (OP-10, OP-13, OP-39, lower = better)
-- both measures come back above the PA average, and OP-10 is the problem: 12.2% vs a state
-- average of 6.28% -- UPMC is ordering CT contrast at nearly double the rate of its peers.
-- OP-13 (cardiac imaging before low-risk surgery) is also elevated but the gap is small (1 pp).
-- OP-39 returned no row -- UPMC likely has no usable score for that measure.

WITH pa_avg AS (
    SELECT
        measure_id,
        ROUND(AVG(score), 2) AS pa_avg_score
    FROM silver_schema.cms_outpatient_imaging i
    JOIN silver_schema.cms_hospital_general h USING (facility_id)
    WHERE h.state = 'PA'
      AND i.measure_id IN ('OP-10', 'OP-13', 'OP-39')
      AND i.is_score_usable = TRUE
      AND i.score IS NOT NULL
    GROUP BY measure_id
)
SELECT
    u.measure_id,
    u.measure_name,
    u.score AS upmc_score,
    p.pa_avg_score,
    ROUND(u.score - p.pa_avg_score, 2) AS vs_pa_diff
FROM silver_schema.cms_outpatient_imaging u
JOIN pa_avg p USING (measure_id)
WHERE u.facility_id = '390164'
  AND u.measure_id IN ('OP-10', 'OP-13', 'OP-39')
  AND u.is_score_usable = TRUE
ORDER BY u.measure_id;

-- Peer ranking among PA non-profit acute care hospitals
-- UPMC Presbyterian Shadyside sits in a 33-hospital 4-star cluster, solidly above average
-- but not elite. no UPMC hospital in the state made 5 stars. the 5-star group is mostly
-- smaller regional systems (WellSpan, St. Luke's, AHN Wexford), not large academic centers.
-- the UPMC system overall is scattered: 4-stars at the flagship campuses, lots of 3-stars
-- across the network, and UPMC Altoona at 2. inconsistent for a system this size.

SELECT
    facility_id,
    facility_name,
    hospital_overall_rating AS stars,
    RANK() OVER (ORDER BY hospital_overall_rating DESC) AS rank_by_stars,
    COUNT(*) OVER () AS total_peers,
    mort_reporting_status,
    safety_reporting_status,
    readm_reporting_status
FROM silver_schema.cms_hospital_general
WHERE state = 'PA'
  AND hospital_ownership = 'Non-Profit'
  AND hospital_type = 'Acute Care Hospitals'
  AND hospital_overall_rating IS NOT NULL
ORDER BY rank_by_stars, facility_name;

-- Gap to 5-star, UPMC's weakest measures vs what PA 5-star hospitals score
-- These are the 4 measures where UPMC lags the most based on the queries above
-- two problems account for most of the gap: CT contrast ordering (OP-10) and pressure ulcers (PSI_03).
-- PSI_03 is the more alarming one -- UPMC scores 1.84 vs a 5-star average of 0.50, nearly 4x worse.
-- fixing PSI_03 would also pull down PSI_90 since pressure ulcers are a component of that composite.

WITH upmc AS (
    SELECT 'PSI_03'    AS measure_id, score AS upmc_score
    FROM silver_schema.cms_complications
    WHERE facility_id = '390164' AND measure_id = 'PSI_03'
    UNION ALL
    SELECT 'PSI_90', score
    FROM silver_schema.cms_complications
    WHERE facility_id = '390164' AND measure_id = 'PSI_90'
    UNION ALL
    SELECT 'OP-10', score
    FROM silver_schema.cms_outpatient_imaging
    WHERE facility_id = '390164' AND measure_id = 'OP-10'
    UNION ALL
    SELECT 'HAI_1_SIR', score
    FROM silver_schema.cms_infections
    WHERE facility_id = '390164' AND measure_id = 'HAI_1_SIR'
),
five_star_peers AS (
    SELECT c.measure_id, ROUND(AVG(c.score), 2) AS five_star_avg
    FROM silver_schema.cms_complications c
    JOIN silver_schema.cms_hospital_general h USING (facility_id)
    WHERE h.state = 'PA'
      AND h.hospital_ownership = 'Non-Profit'
      AND h.hospital_type = 'Acute Care Hospitals'
      AND h.hospital_overall_rating = 5
      AND c.measure_id IN ('PSI_03', 'PSI_90')
      AND c.is_score_usable = TRUE
    GROUP BY c.measure_id
    UNION ALL
    SELECT i.measure_id, ROUND(AVG(i.score), 2)
    FROM silver_schema.cms_outpatient_imaging i
    JOIN silver_schema.cms_hospital_general h USING (facility_id)
    WHERE h.state = 'PA'
      AND h.hospital_ownership = 'Non-Profit'
      AND h.hospital_type = 'Acute Care Hospitals'
      AND h.hospital_overall_rating = 5
      AND i.measure_id = 'OP-10'
      AND i.is_score_usable = TRUE
    GROUP BY i.measure_id
    UNION ALL
    SELECT inf.measure_id, ROUND(AVG(inf.score), 2)
    FROM silver_schema.cms_infections inf
    JOIN silver_schema.cms_hospital_general h USING (facility_id)
    WHERE h.state = 'PA'
      AND h.hospital_ownership = 'Non-Profit'
      AND h.hospital_type = 'Acute Care Hospitals'
      AND h.hospital_overall_rating = 5
      AND inf.measure_id = 'HAI_1_SIR'
      AND inf.is_score_usable = TRUE
    GROUP BY inf.measure_id
)
SELECT
    u.measure_id,
    ROUND(u.upmc_score, 2) AS upmc_score,
    p.five_star_avg,
    ROUND(u.upmc_score - p.five_star_avg, 2) AS gap_to_close
FROM upmc u
JOIN five_star_peers p USING (measure_id)
ORDER BY gap_to_close DESC;
