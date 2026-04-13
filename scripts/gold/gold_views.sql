-- =============================================================================
-- gold_views.sql
-- =============================================================================
-- 3 analytical views answering the project's business questions.
-- All join to dim_hospital for region and ownership, no repeated CASE logic.
--
-- Q1: Do hospitals that over-order imaging also have longer ED waits?
-- Q2: Does ownership predict imaging/ED performance? (Summary of Q1 by group)
-- Q3: Can a hospital be operationally efficient but clinically unsafe?
--
-- Key filter decisions:
--   ED: OP_18a-d only (minutes). OP_22/23 are percentages, different unit.
--   Imaging: OP-10, OP-13, OP-39 only. OP-8 excluded (different scale).
--   ED outliers: hard cap at 1,440 min (24h) — anything beyond is an artifact.
--   Process-of-care: Colonoscopy care + Healthcare Personnel Vaccination only.
--   Complications: requires >= 3 rated measures per hospital.
-- =============================================================================


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
JOIN imaging_scores i ON d.facility_id = i.facility_id
JOIN ed_scores e ON d.facility_id = e.facility_id;


CREATE OR REPLACE VIEW gold_schema.vw_ownership_imaging_ed_summary AS
SELECT
    hospital_ownership,
    COUNT(*) AS n_hospitals,
    ROUND(AVG(avg_imaging_score), 2) AS avg_imaging_score,
    ROUND(STDDEV(avg_imaging_score), 2) AS stddev_imaging_score,
    ROUND(AVG(avg_ed_wait_minutes), 2) AS avg_ed_wait_minutes,
    ROUND(STDDEV(avg_ed_wait_minutes), 2) AS stddev_ed_wait_minutes,
    ROUND(AVG(hospital_overall_rating), 2) AS avg_star_rating
FROM gold_schema.vw_imaging_vs_ed_wait
WHERE hospital_ownership != 'Tribal'
GROUP BY hospital_ownership
ORDER BY n_hospitals DESC;


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
    WHERE condition IN ('Colonoscopy care', 'Healthcare Personnel Vaccination')
      -- 'Surgical Care' does not exist in data; 'Cataract surgery outcome' is an outcome
      -- measure not a compliance %, so excluded. Sepsis/ECQ/ED excluded as before.
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
