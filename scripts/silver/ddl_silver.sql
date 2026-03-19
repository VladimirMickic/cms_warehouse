/*
===============================================================================
Silver Layer DDL: Cleaned and Transformed Tables
===============================================================================
Purpose:
    Clean and standardize Bronze data with proper data types, standardized
    categories, derived flags, and audit columns.

    Star schema design (surrogate keys, column removal, dimensional modeling)
    happens in the Gold layer.
===============================================================================
*/

CREATE SCHEMA IF NOT EXISTS silver_schema;


-- =============================================================================
-- 1. silver_schema.cms_hospital_general
-- =============================================================================
DROP TABLE IF EXISTS silver_schema.cms_hospital_general;

CREATE TABLE silver_schema.cms_hospital_general (
    facility_id                         VARCHAR(6) NOT NULL,
    facility_name                       VARCHAR(255),
    address                             VARCHAR(255),
    city                                VARCHAR(50),
    state                               VARCHAR(2),
    zip_code                            VARCHAR(15),
    county                              VARCHAR(100),
    hospital_type                       VARCHAR(100),
    hospital_ownership                  VARCHAR(50),   -- THIS is a new column where I categorize ownership to Government/Non-Profit/For-Profit/Other
    hospital_ownership_details          VARCHAR(100),  -- Original column

    -- Service flags cast from txt to boolean
    emergency_services                  BOOLEAN,
    birthing_friendly_hospital          BOOLEAN,

    hospital_overall_rating             INT,
    hospital_footnote                   VARCHAR(50),

    -- Mortality Group
    mort_group_measures_count           INT,
    facility_mort_measures_count        INT,
    mort_measures_better                INT,
    mort_measures_no_different          INT,
    mort_measures_worse                 INT,
    mort_group_footnote                 VARCHAR(10),
    mort_not_reported                   BOOLEAN,
    mort_not_participating              BOOLEAN,
    mort_dod_hospital                   BOOLEAN,
    mort_data_issue                     BOOLEAN,
    mort_reporting_status               VARCHAR(50), -- added column to see rating Fully Rated/Partially rated/Insufficient Data/Non-Participating/Federal(DoD)

    -- Safety Group
    safety_group_measures_count         INT,
    facility_safety_measures_count      INT,
    safety_measures_better              INT,
    safety_measures_no_different        INT,
    safety_measures_worse               INT,
    safety_group_footnote               VARCHAR(10),
    safety_not_reported                 BOOLEAN,
    safety_not_participating            BOOLEAN,
    safety_dod_hospital                 BOOLEAN,
    safety_data_issue                   BOOLEAN,
    safety_reporting_status             VARCHAR(50),

    -- READM Group (Readmissions)
    readm_group_measures_count          INT,
    facility_readm_measures_count       INT,
    readm_measures_better               INT,
    readm_measures_no_different         INT,
    readm_measures_worse                INT,
    readm_group_footnote                VARCHAR(10),
    readm_not_reported                  BOOLEAN,
    readm_not_participating             BOOLEAN,
    readm_data_issue                    BOOLEAN,
    readm_dod_hospital                  BOOLEAN,
    readm_reporting_status              VARCHAR(50),

    -- Patient Experience Group
    pt_exp_group_measures_count         INT,
    facility_pt_exp_measures_count      INT,
    pt_exp_group_footnote               VARCHAR(10),
    pt_exp_not_reported                 BOOLEAN,
    pt_exp_not_participating            BOOLEAN,
    pt_exp_data_issue                   BOOLEAN,
    pt_exp_dod_hospital                 BOOLEAN,
    pt_exp_reporting_status             VARCHAR(50),

    -- Timely & Effective Care Group
    te_group_measures_count             INT,
    facility_te_measures_count          INT,
    te_group_footnote                   VARCHAR(10),
    te_not_reported                     BOOLEAN,
    te_not_participating                BOOLEAN,
    te_data_issue                       BOOLEAN,
    te_dod_hospital                     BOOLEAN,
    te_reporting_status                 VARCHAR(50),

    -- Audit
    dwh_create_date                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- =============================================================================
-- 2. silver_schema.cms_timely_care
-- Source: bronze_schema.cms_timely_care (16 columns, 138,129 rows)
-- Profiling:
--   - 6 conditions, ~30 measure_ids
--   - Score: 37.7% numeric (52,018), 4% categorical text (5,577),
--           58.3% 'Not Available' (80,534), 0% NULL/empty
--   - Categorical text values: 'very high','high','medium','low','very low'
--   - Sample: needs profiling confirmation (expected integers + 'Not Available')
--   - Footnotes: multi-code comma-separated (e.g. '1, 3', '2, 3, 29')
--     Code 5 covers 63,996 rows (46%), code 2 covers 45,780 (33%)
--     ~80% of rows have a footnote; only ~20% are clean unfootnoted scores
--   - Dates: MM/DD/YYYY text, consistent format
--   - No duplicates on (facility_id, measure_id, start_date)
-- =============================================================================
DROP TABLE IF EXISTS silver_schema.cms_timely_care;

CREATE TABLE silver_schema.cms_timely_care (
    -- Identity (natural keys)
    facility_id                         VARCHAR(6) NOT NULL,
    facility_name                       VARCHAR(255),
    address                             VARCHAR(255),
    city                                VARCHAR(100),
    state                               VARCHAR(2),
    zip_code                            VARCHAR(10),
    county                              VARCHAR(100),

    -- Measures
    condition                           VARCHAR(255),
    measure_id                          VARCHAR(100),
    measure_name                        VARCHAR(255),

    -- Score split into 3 columns because of different values
    score_numeric                       NUMERIC,    -- NULL when score categorical
    score_text                          VARCHAR(50), -- NULL when numeric
    score_available                     BOOLEAN,    -- False when not available or null or empty

    -- Sample size converted to int
    sample_size                         INT,
    footnote                            VARCHAR(50),
    start_date                          DATE,
    end_date                            DATE,
    dwh_create_date                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

/*
Changes from Bronze:
  Removed:   telephone_number (not analytical)
  Split:     score (TEXT) - score_numeric (NUMERIC) + score_text (VARCHAR)
  Added:     score_available (derived boolean flag)
  Cast:      sample - sample_size (INT), start_date/end_date - DATE
  Kept:      All other columns with proper types
*/


-- =============================================================================
-- 3. silver_schema.cms_complications
-- Source: bronze_schema.cms_complications (18 columns, 95,780 rows)
-- Profiling:
--   - 20 measure_ids, each ~4,789 hospitals (uniform coverage)
--   - Score: 45.6% 'Not Available' (43,646 rows)
--   - Denominator: 43.6% 'Not Available'
--   - Lower/Higher estimates: 45.6% 'Not Available' (matches score exactly)
--   - compared_to_national: 8 distinct values with semantic duplicates
--     including 'Number of Cases Too Small' -> all map to 3 values + NULL
--   - Footnotes: NULL most common; codes 1 and 28 in small counts
--   - No duplicates on (facility_id, measure_id)
-- =============================================================================
DROP TABLE IF EXISTS silver_schema.cms_complications;

CREATE TABLE silver_schema.cms_complications (
    -- Identity (natural keys)
    facility_id                         VARCHAR(6) NOT NULL,
    facility_name                       VARCHAR(255),
    address                             VARCHAR(255),
    city                                VARCHAR(100),
    state                               VARCHAR(2),
    zip_code                            VARCHAR(10),
    county                              VARCHAR(100),

    measure_id                          VARCHAR(50),
    measure_name                        VARCHAR(255),
    compared_to_national                VARCHAR(50),
    denominator                         INT,
    score                               NUMERIC,
    lower_estimate                      NUMERIC,
    higher_estimate                     NUMERIC,
    score_available                     BOOLEAN,  -- New column to check is score available at all
    footnote                            VARCHAR(50),
    start_date                          DATE,
    end_date                            DATE,
    dwh_create_date                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

/*
Changes from Bronze:
  Removed:   telephone_number (not analytical)
  Added:     score_available (derived boolean flag)
  Cast:      score, denominator, lower_estimate, higher_estimate -> NUMERIC/INT
             start_date, end_date -> DATE
  Standardized: compared_to_national -> 3 clean values + NULL
                'Better Than the National Rate'          -> 'Better'
                'No Different Than the National Rate'    -> 'No Different'
                'Worse Than the National Rate'           -> 'Worse'
                'Number of Cases Too Small'              -> NULL
                'Not Available' / '' / NULL              -> NULL
                (8 original values -> 3 + NULL using ILIKE wildcards)
  Kept:      All other columns with proper types
*/


-- =============================================================================
-- 4. silver_schema.cms_outpatient_imaging
-- Source: bronze_schema.cms_outpatient_imaging (14 columns, 18,500 rows)
-- Profiling:
--   - 5 measure_ids, each ~3,700 hospitals (uniform coverage)
--   - Score: 47.6% 'Not Available' (8,810 rows)
--     No categorical text values -- only numeric and 'Not Available'
--   - No compared_to_national, no denominator, no confidence intervals
--   - No duplicates on (facility_id, measure_id)
-- =============================================================================
DROP TABLE IF EXISTS silver_schema.cms_outpatient_imaging;

CREATE TABLE silver_schema.cms_outpatient_imaging (
    facility_id                         VARCHAR(6) NOT NULL,
    facility_name                       VARCHAR(255),
    address                             VARCHAR(255),
    city                                VARCHAR(100),
    state                               VARCHAR(2),
    zip_code                            VARCHAR(10),
    county                              VARCHAR(100),

    measure_id                          VARCHAR(50),
    measure_name                        VARCHAR(255),
    score                               NUMERIC,   -- Lower is better
    score_available                     BOOLEAN,
    footnote                            VARCHAR(50),
    start_date                          DATE,
    end_date                            DATE,
    dwh_create_date                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

/*
Changes from Bronze:
  Removed:   telephone_number (not analytical)
  Added:     score_available (derived boolean flag)
  Cast:      score -> NUMERIC, start_date/end_date -> DATE
  Kept:      All other columns with proper types
*/


-- =============================================================================
-- 5. silver_schema.cms_infections
-- Source: bronze_schema.cms_infections (15 columns, 172,404 rows)
-- Profiling:
--   - 36 measure_ids: HAI_1 through HAI_6, each with 6 sub-measures
--     (_SIR, _CILOWER, _CIUPPER, _NUMERATOR, _ELIGCASES, _DOPC)
--   - ~36 rows per hospital (some hospitals have fewer -- missing measures)
--   - Score: all numeric but semantically different by suffix
--     _SIR = ratio around 1.0, _NUMERATOR = raw counts, _DOPC = large integers
--   - compared_to_national: different wording than complications
--     'No Different than National Benchmark' vs 'No Different Than the National Rate'
--     Both standardized to 'Better'/'No Different'/'Worse'/NULL
--   - No duplicates on (facility_id, measure_id)
-- =============================================================================
DROP TABLE IF EXISTS silver_schema.cms_infections;

CREATE TABLE silver_schema.cms_infections (
    facility_id                         VARCHAR(6) NOT NULL,
    facility_name                       VARCHAR(255),
    address                             VARCHAR(255),
    city                                VARCHAR(100),
    state                               VARCHAR(2),
    zip_code                            VARCHAR(10),
    county                              VARCHAR(100),

    -- Measure attributes
    measure_id                          VARCHAR(50),
    measure_suffix                      VARCHAR(20),  -- 'SIR', 'CILOWER', 'CIUPPER', 'NUMERATOR', 'ELIGCASES', 'DOPC'
    measure_name                        VARCHAR(255),
    compared_to_national                VARCHAR(50),
    score                               NUMERIC,
    score_available                     BOOLEAN,
    footnote                            VARCHAR(50),
    start_date                          DATE,
    end_date                            DATE,
    dwh_create_date                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

/*
Changes from Bronze:
  Removed:   telephone_number (not analytical)
  Added:     measure_suffix (parsed from measure_id: 'SIR', 'CILOWER', etc.)
             score_available (derived boolean flag)
  Cast:      score -> NUMERIC, start_date/end_date -> DATE
  Standardized: compared_to_national -> 3 clean values + NULL
                'No Different than National Benchmark' -> 'No Different'
                (note: different wording than complications table -- standardized to match)
  Kept:      All 36 sub-measure rows per hospital preserved
             All other columns with proper types
*/
