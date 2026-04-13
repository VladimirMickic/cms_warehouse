-- =============================================================================
-- silver_setup.sql
-- =============================================================================
-- Typed tables built from the bronze raw layer. Each column is cast to its
-- proper type, categories are standardized, and derived flags are added.
--
-- telephone_number is dropped across all tables — not analytical.
-- dwh_create_date is the audit timestamp we add; everything else maps 1:1
-- to a bronze column or is derived from one.
--
-- Star schema design.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS silver_schema;


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
    birthing_friendly_designation       BOOLEAN,

    hospital_overall_rating             INT,
    hospital_overall_rating_footnote    VARCHAR(50),

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


DROP TABLE IF EXISTS silver_schema.cms_timely_care;

CREATE TABLE silver_schema.cms_timely_care (
    facility_id                         VARCHAR(6) NOT NULL,
    facility_name                       VARCHAR(255),
    address                             VARCHAR(255),
    city                                VARCHAR(100),
    state                               VARCHAR(2),
    zip_code                            VARCHAR(10),
    county                              VARCHAR(100),
    condition                           VARCHAR(255),
    measure_id                          VARCHAR(100),
    measure_name                        VARCHAR(255),

    -- Score split into 3 columns because of different values
    score_numeric                       NUMERIC,    -- NULL when score categorical
    score_text                          VARCHAR(50), -- NULL when numeric
    score_available                     BOOLEAN,    -- False when not available or null or empty
    is_score_usable                     BOOLEAN,
    score_exclusion_reason              VARCHAR(25),

    sample                              INT,
    footnote                            VARCHAR(50),
    start_date                          DATE,
    end_date                            DATE,
    dwh_create_date                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS silver_schema.cms_complications;

CREATE TABLE silver_schema.cms_complications (
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
    score_available                     BOOLEAN,
    is_score_usable                     BOOLEAN,
    score_exclusion_reason              VARCHAR(25),
    footnote                            VARCHAR(50),
    start_date                          DATE,
    end_date                            DATE,
    dwh_create_date                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


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
    is_score_usable                     BOOLEAN,
    score_exclusion_reason              VARCHAR(25),
    footnote                            VARCHAR(50),
    start_date                          DATE,
    end_date                            DATE,
    dwh_create_date                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS silver_schema.cms_infections;

CREATE TABLE silver_schema.cms_infections (
    facility_id                         VARCHAR(6) NOT NULL,
    facility_name                       VARCHAR(255),
    address                             VARCHAR(255),
    city                                VARCHAR(100),
    state                               VARCHAR(2),
    zip_code                            VARCHAR(10),
    county                              VARCHAR(100),
    measure_id                          VARCHAR(50),
    measure_suffix                      VARCHAR(20),  -- 'SIR', 'CILOWER', 'CIUPPER', 'NUMERATOR', 'ELIGCASES', 'DOPC'
    measure_name                        VARCHAR(255),
    compared_to_national                VARCHAR(50),
    score                               NUMERIC,
    score_available                     BOOLEAN,
    is_score_usable                     BOOLEAN,
    score_exclusion_reason              VARCHAR(25),
    footnote                            VARCHAR(50),
    start_date                          DATE,
    end_date                            DATE,
    dwh_create_date                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
