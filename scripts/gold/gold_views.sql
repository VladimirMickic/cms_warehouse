/*
===============================================================================
Gold Layer: Analytical Views
===============================================================================
Purpose:
    3 views answering the project's 3 business questions.
    Views 1-2 power the Tableau dashboard (Q1 + Q2).
    View 3 is the SQL-only Q3 analysis.

    All 3 views join to gold_schema.dim_hospital (defined in gold_setup.sql)
    to get region mapping and hospital attributes without repeating the
    region CASE statement in every view.

Business Questions:
    Q1 (Dashboard): Do hospitals that over-order imaging also have longer
        ED wait times — or are these independent problems?
    Q2 (Dashboard): Does hospital ownership type predict which hospitals
        have imaging and ED problems?
    Q3 (SQL-only): Can a hospital be operationally efficient but clinically
        unsafe? (Complications vs process-of-care)

Design decisions driven by silver exploration (query_reasoning.md):
    - ED measures: OP_18a-d only (minutes). OP_22/OP_23 are percentages.
    - ED outliers: filtered out with a 1,440-minute (24-hour) hard cap.
    - Imaging: Exclude OP-8 (n=612, median 36.5% vs ~5% for others).
    - Complications: compared_to_national not raw scores (base rates differ).
    - Minimum 3 rated complication measures per hospital.
    - Reporting dates: included as context (reporting_period_start/end) but
      not used as analytical dimensions — CMS publishes one snapshot per
      release, so all hospitals share the same reporting window per table.
===============================================================================
*/


-- =============================================================================
-- VIEW 1: vw_imaging_vs_ed_wait (Dashboard — Scatter Plot)
-- =============================================================================
-- One row per hospital with BOTH usable imaging and ED scores.
-- Imaging: OP-10, OP-13, OP-39 only (exclude OP-8: different scale, n=612).
--   Lower score = better (less inappropriate imaging).
-- ED wait: OP_18a-d only (minutes, not percentages).
--   Higher score = worse (longer wait). Scores > 24 hours filtered as artifacts.
-- INNER JOIN ensures only hospitals with BOTH domains appear.
-- Sample: ~3,761 hospitals (69.3% of 5,426). Survivorship bias toward
--   large urban hospitals documented in README.


DROP VIEW gold_schema.vw_imaging_vs_ed_wait;
CREATE OR REPLACE VIEW gold_schema.vw_imaging_vs_ed_wait AS
WITH imaging_scores AS (
    SELECT
        facility_id,
        ROUND(AVG(score), 2) AS avg_imaging_score,
        COUNT(*) AS imaging_measures_used,
        MIN(start_date) AS imaging_period_start,
        MAX(end_date) AS imaging_period_end
    FROM silver_schema.cms_outpatient_imaging
    WHERE is_score_usable = TRUE
      AND score IS NOT NULL
      AND measure_id IN ('OP-10', 'OP-13', 'OP-39')  -- exclude OP-8 (different scale)
    GROUP BY facility_id
),
ed_scores AS (
    SELECT
        facility_id,
        ROUND(AVG(score_numeric), 2) AS avg_ed_wait_minutes,
        COUNT(*) AS ed_measures_used,
        MIN(start_date) AS ed_period_start,
        MAX(end_date) AS ed_period_end
    FROM silver_schema.cms_timely_care
    WHERE measure_id IN ('OP_18a', 'OP_18b', 'OP_18c', 'OP_18d')
      AND is_score_usable = TRUE
      AND score_numeric IS NOT NULL
      AND score_numeric <= 1440  -- cap at 24 hours; beyond is a data artifact
    GROUP BY facility_id
)
SELECT
    d.facility_id,
    d.facility_name,
    d.hospital_ownership,
    d.hospital_type,
    d.state,
    d.region,
    d.hospital_overall_rating,
    i.avg_imaging_score,
    i.imaging_measures_used,
    e.avg_ed_wait_minutes,
    e.ed_measures_used,
    -- Reporting periods included for context; not analytical dimensions because
    -- CMS publishes one snapshot per release (all hospitals share the same window)
    i.imaging_period_start,
    i.imaging_period_end,
    e.ed_period_start,
    e.ed_period_end
FROM gold_schema.dim_hospital d
INNER JOIN imaging_scores i ON d.facility_id = i.facility_id
INNER JOIN ed_scores e ON d.facility_id = e.facility_id;


-- =============================================================================
-- VIEW 2: vw_ownership_imaging_ed_summary (Dashboard — Summary Bars)
-- =============================================================================
-- Aggregated by ownership type from the Q1 view.
-- Tribal excluded (1 hospital in sample — not statistically meaningful).
-- Includes STDDEV so viewers can see spread within each ownership group.
-- No dates here — this is a summary view, reporting period is in View 1.
DROP VIEW gold_schema.vw_ownership_imaging_ed_statistics;
CREATE OR REPLACE VIEW gold_schema.vw_ownership_imaging_ed_stats AS
SELECT
    hospital_ownership,
    COUNT(*) AS n_hospitals,
    ROUND(AVG(avg_imaging_score), 2) AS avg_imaging_score,
    ROUND(STDDEV(avg_imaging_score), 2) AS stddev_imaging_score,
    ROUND(AVG(avg_ed_wait_minutes), 2) AS avg_ed_wait_minutes,
    ROUND(STDDEV(avg_ed_wait_minutes), 2) AS stddev_ed_wait_minutes,
    ROUND(AVG(hospital_overall_rating), 2) AS avg_star_rating,
    ROUND(CORR(avg_imaging_score, avg_ed_wait_minutes)::numeric, 2) correlation
FROM gold_schema.vw_imaging_vs_ed_wait
WHERE hospital_ownership != 'Tribal'
GROUP BY hospital_ownership
ORDER BY n_hospitals DESC;


-- =============================================================================
-- VIEW 3: vw_complications_vs_process_care (SQL-only — Q3)
-- =============================================================================
-- Can a hospital be operationally efficient but clinically unsafe?
--
-- Complication side: broken out by measure category (MORT%, PSI%, COMP%) so
--   each can be analysed independently. Only MORT% shows meaningful variation
--   (5.6% Better / 91.3% No Different / 3.1% Worse). PSI% and COMP% are 97%+
--   "No Different" — essentially noise. Requires >= 3 rated measures overall.
--
-- Process-of-care side: filters to Colonoscopy, Vaccination, and Surgical Care
--   conditions only. These are all compliance percentages on comparable scales.
--   Excluded:
--     - 'Emergency Department' (wait times in minutes, different unit)
--     - 'Sepsis' (bundles scored 18-92%, compresses the average and has only
--       18-25% usable rows — low coverage, incompatible baseline)
--     - 'Healthcare Associated Infections' (OP_40 is likely minutes not %)
--     - 'Electronic Clinical Quality' (HH_HYPER/HYPO/ORAE are harm rates not
--       compliance; OP_40 is incompatible scale)
DROP VIEW gold_schema.vw_complications_vs_process_care;
CREATE OR REPLACE VIEW gold_schema.vw_complications_vs_process_care AS
WITH comp_performance AS (
    SELECT
        facility_id,
        -- Overall
        COUNT(*) FILTER (WHERE compared_to_national IS NOT NULL) AS comp_rated_total,
        COUNT(*) FILTER (WHERE compared_to_national = 'Better')      AS comp_better,
        COUNT(*) FILTER (WHERE compared_to_national = 'No Different') AS comp_no_different,
        COUNT(*) FILTER (WHERE compared_to_national = 'Worse')       AS comp_worse,
        ROUND(100.0 * COUNT(*) FILTER (WHERE compared_to_national = 'Worse')
            / NULLIF(COUNT(*) FILTER (WHERE compared_to_national IS NOT NULL), 0), 2)
            AS complication_pct_worse,
        -- Mortality only (MORT% — only category with meaningful differentiation)
        COUNT(*) FILTER (WHERE measure_id LIKE 'MORT%' AND compared_to_national IS NOT NULL) AS mort_rated,
        ROUND(100.0 * COUNT(*) FILTER (WHERE measure_id LIKE 'MORT%' AND compared_to_national = 'Worse')
            / NULLIF(COUNT(*) FILTER (WHERE measure_id LIKE 'MORT%' AND compared_to_national IS NOT NULL), 0), 2)
            AS mort_pct_worse,
        -- Patient Safety (PSI%)
        COUNT(*) FILTER (WHERE measure_id LIKE 'PSI%' AND compared_to_national IS NOT NULL) AS psi_rated,
        ROUND(100.0 * COUNT(*) FILTER (WHERE measure_id LIKE 'PSI%' AND compared_to_national = 'Worse')
            / NULLIF(COUNT(*) FILTER (WHERE measure_id LIKE 'PSI%' AND compared_to_national IS NOT NULL), 0), 2)
            AS psi_pct_worse,
        -- Complications (COMP%)
        COUNT(*) FILTER (WHERE measure_id LIKE 'COMP%' AND compared_to_national IS NOT NULL) AS comp_cat_rated,
        ROUND(100.0 * COUNT(*) FILTER (WHERE measure_id LIKE 'COMP%' AND compared_to_national = 'Worse')
            / NULLIF(COUNT(*) FILTER (WHERE measure_id LIKE 'COMP%' AND compared_to_national IS NOT NULL), 0), 2)
            AS comp_cat_pct_worse,
        MIN(start_date) AS comp_period_start,
        MAX(end_date)   AS comp_period_end
    FROM silver_schema.cms_complications
    WHERE is_score_usable = TRUE
    GROUP BY facility_id
),
process_care AS (
    SELECT
        facility_id,
        -- Colonoscopy, Vaccination, Surgical Care only — comparable % compliance scales
        ROUND(AVG(score_numeric), 2) AS avg_process_care_score,
        COUNT(*)                     AS process_measures_count,
        MIN(start_date) AS process_period_start,
        MAX(end_date)   AS process_period_end
    FROM silver_schema.cms_timely_care
    WHERE condition IN ('Colonoscopy', 'Vaccination', 'Surgical Care')
      AND is_score_usable = TRUE
      AND score_numeric IS NOT NULL
    GROUP BY facility_id
)
SELECT
    d.facility_id,
    d.facility_name,
    d.hospital_ownership,
    d.hospital_type,
    d.state,
    d.region,
    d.hospital_overall_rating,
    c.comp_rated_total,
    c.comp_better,
    c.comp_no_different,
    c.comp_worse,
    c.complication_pct_worse,
    c.mort_rated,
    c.mort_pct_worse,
    c.psi_rated,
    c.psi_pct_worse,
    c.comp_cat_rated,
    c.comp_cat_pct_worse,
    p.avg_process_care_score,
    p.process_measures_count,
    c.comp_period_start,
    c.comp_period_end,
    p.process_period_start,
    p.process_period_end
FROM gold_schema.dim_hospital d
INNER JOIN comp_performance c ON d.facility_id = c.facility_id
INNER JOIN process_care p ON d.facility_id = p.facility_id
WHERE c.comp_rated_total >= 3;  -- minimum for meaningful % worse

