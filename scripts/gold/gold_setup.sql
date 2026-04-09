/*
===============================================================================
Gold Layer: Base Views
===============================================================================
Purpose:
    2 base views that sit on top of Silver and add Gold-layer logic:
    - dim_hospital: adds region mapping, keeps columns needed for analysis
    - dim_measure: unifies all measures across 4 source tables with categories

    These are views, not tables, no data duplication, no load procedure needed.
    Analytical views in gold_views.sql query silver tables directly for
    measure-level filtering (e.g., OP_18a-d only) and join to dim_hospital
    for hospital attributes.

Architecture decision:
    No unified fact table. A UNION of all
    4 tables would add NULL columns for table-specific fields and complexity
    for no benefit, each view queries exactly the silver tables it needs.

Run order:
    1. This file (creates schema + base views)
    2. gold_views.sql (creates analytical views)
===============================================================================
*/

CREATE SCHEMA IF NOT EXISTS gold_schema;


-- =============================================================================
-- 1. dim_hospital
-- =============================================================================
-- One row per hospital with attributes needed by all 3 business questions.
-- Columns included: facility_id, name, ownership, type, state, region, star rating.
-- Columns excluded: address, city, zip, county, reporting status, service flags
--   (not used by any analytical view).
-- Region: US Census Bureau 4-region classification + Territory.
--   Territories (PR, GU, VI, AS, MP) grouped separately and excluded from
--   analytical views — 87-100% unusable scores across all fact tables.

CREATE OR REPLACE VIEW gold_schema.dim_hospital AS
SELECT
    facility_id,
    facility_name,
    hospital_type,
    hospital_ownership,
    hospital_ownership_details,
    state,
    CASE
        WHEN state IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'Northeast'
        WHEN state IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'Midwest'
        WHEN state IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'South'
        WHEN state IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'West'
        WHEN state IN ('PR','GU','VI','AS','MP') THEN 'Territory'
        ELSE 'Unknown'
    END AS region,
    hospital_overall_rating
FROM silver_schema.cms_hospital_general;


-- =============================================================================
-- 2. dim_measure
-- =============================================================================
-- 90 distinct measures across 4 source tables. No measure_id collisions
-- (verified in Task 7.2). Categories derived from condition (timely_care)
-- or measure_id prefix (complications).
-- UNION (not UNION ALL) to deduplicate in case of any overlap.

CREATE OR REPLACE VIEW gold_schema.dim_measure AS

-- Timely Care measures (categorized by condition)
SELECT DISTINCT
    measure_id,
    measure_name,
    CASE
        WHEN condition = 'Emergency Department' THEN 'Emergency Department'
        ELSE 'Process of Care'
    END AS measure_category,
    'cms_timely_care' AS source_table
FROM silver_schema.cms_timely_care

UNION

-- Complication measures (categorized by measure_id prefix)
SELECT DISTINCT
    measure_id,
    measure_name,
    CASE
        WHEN measure_id LIKE 'MORT%' THEN 'Mortality'
        WHEN measure_id LIKE 'PSI%' THEN 'Patient Safety'
        WHEN measure_id LIKE 'COMP%' THEN 'Complications'
        ELSE 'Other'
    END AS measure_category,
    'cms_complications' AS source_table
FROM silver_schema.cms_complications

UNION

-- Imaging measures
SELECT DISTINCT
    measure_id,
    measure_name,
    'Imaging Efficiency' AS measure_category,
    'cms_outpatient_imaging' AS source_table
FROM silver_schema.cms_outpatient_imaging

UNION

-- Infection measures
SELECT DISTINCT
    measure_id,
    measure_name,
    'Healthcare-Associated Infections' AS measure_category,
    'cms_infections' AS source_table
FROM silver_schema.cms_infections;


