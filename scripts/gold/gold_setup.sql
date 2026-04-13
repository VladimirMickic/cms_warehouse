-- =============================================================================
-- gold_setup.sql
-- =============================================================================
-- 1 base view on top of silver. No tables, no load proc, view evaluates
-- lazily so there's no data duplication.
--
-- dim_hospital: one row per hospital, adds US Census region mapping.
--
-- Analytical views in gold_views.sql query silver directly and join to
-- dim_hospital for hospital attributes. No unified fact table, each view
-- pulls exactly the silver tables it needs, nothing more.
--
-- Run this file first, then gold_views.sql.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS gold_schema;


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




