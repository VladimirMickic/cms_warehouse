-- =============================================================================
-- bronze_setup.sql
-- =============================================================================
-- Raw landing zone for CMS Hospital Compare data. Five tables, one per
-- source CSV file. Every column is TEXT, no casting, no transforms, no
-- business logic. The point is an exact mirror of what CMS published so
-- we have something to diff against when silver starts producing odd numbers.
--
-- dwh_load_date is the only column we add ourselves. Everything else is
-- pulled straight from the CMS column headers.
-- =============================================================================

DROP TABLE IF EXISTS bronze_schema.cms_hospital_general;

CREATE TABLE bronze_schema.cms_hospital_general (
    facility_id                         TEXT,
    facility_name                       TEXT,
    address                             TEXT,
    city                                TEXT,
    state                               TEXT,
    zip_code                            TEXT,
    county                              TEXT,
    telephone_number                    TEXT,
    hospital_type                       TEXT,
    hospital_ownership                  TEXT,
    emergency_services                  TEXT,
    birthing_friendly_designation       TEXT,
    hospital_overall_rating             TEXT,
    hospital_overall_rating_footnote    TEXT,
    mort_group_measure_count            TEXT,
    facility_mort_measure_count         TEXT,
    mort_measures_better                TEXT,
    mort_measures_no_different          TEXT,
    mort_measures_worse                 TEXT,
    mort_group_footnote                 TEXT,
    safety_group_measure_count          TEXT,
    facility_safety_measure_count       TEXT,
    safety_measures_better              TEXT,
    safety_measures_no_different        TEXT,
    safety_measures_worse               TEXT,
    safety_group_footnote               TEXT,
    readm_group_measure_count           TEXT,
    facility_readm_measure_count        TEXT,
    readm_measures_better               TEXT,
    readm_measures_no_different         TEXT,
    readm_measures_worse                TEXT,
    readm_group_footnote                TEXT,
    pt_exp_group_measure_count          TEXT,
    facility_pt_exp_measure_count       TEXT,
    pt_exp_group_footnote               TEXT,
    te_group_measure_count              TEXT,
    facility_te_measure_count           TEXT,
    te_group_footnote                   TEXT,
    dwh_load_date                       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS bronze_schema.cms_timely_care;

CREATE TABLE bronze_schema.cms_timely_care (
    facility_id         TEXT,
    facility_name       TEXT,
    address             TEXT,
    city                TEXT,
    state               TEXT,
    zip_code            TEXT,
    county              TEXT,
    telephone_number    TEXT,
    condition           TEXT,
    measure_id          TEXT,
    measure_name        TEXT,
    score               TEXT,
    sample              TEXT,
    footnote            TEXT,
    start_date          TEXT,
    end_date            TEXT,
    dwh_load_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS bronze_schema.cms_complications;

CREATE TABLE bronze_schema.cms_complications (
    facility_id             TEXT,
    facility_name           TEXT,
    address                 TEXT,
    city                    TEXT,
    state                   TEXT,
    zip_code                TEXT,
    county                  TEXT,
    telephone_number        TEXT,
    measure_id              TEXT,
    measure_name            TEXT,
    compared_to_national    TEXT,
    denominator             TEXT,
    score                   TEXT,
    lower_estimate          TEXT,
    higher_estimate         TEXT,
    footnote                TEXT,
    start_date              TEXT,
    end_date                TEXT,
    dwh_load_date           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS bronze_schema.cms_outpatient_imaging;

CREATE TABLE bronze_schema.cms_outpatient_imaging (
    facility_id         TEXT,
    facility_name       TEXT,
    address             TEXT,
    city                TEXT,
    state               TEXT,
    zip_code            TEXT,
    county              TEXT,
    telephone_number    TEXT,
    measure_id          TEXT,
    measure_name        TEXT,
    score               TEXT,
    footnote            TEXT,
    start_date          TEXT,
    end_date            TEXT,
    dwh_load_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



DROP TABLE IF EXISTS bronze_schema.cms_infections;

CREATE TABLE bronze_schema.cms_infections (
    facility_id             TEXT,
    facility_name           TEXT,
    address                 TEXT,
    city                    TEXT,
    state                   TEXT,
    zip_code                TEXT,
    county                  TEXT,
    telephone_number        TEXT,
    measure_id              TEXT,
    measure_name            TEXT,
    compared_to_national    TEXT,
    score                   TEXT,
    footnote                TEXT,
    start_date              TEXT,
    end_date                TEXT,
    dwh_load_date           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
