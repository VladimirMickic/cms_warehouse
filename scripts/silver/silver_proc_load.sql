/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    Performs the ETL process to populate silver_schema tables from bronze_schema.
    Actions:
        - Truncates Silver tables
        - Inserts transformed and cleansed data from Bronze into Silver
        - Logs timing and row counts per table
        - Per-table exception handling (one failure doesn't stop the rest)

Parameters:
    None.

Usage:
    CALL silver_schema.load_silver();

Expected Row Counts:
    cms_hospital_general     ->   5,426
    cms_timely_care          -> 138,129
    cms_complications        ->  95,780
    cms_outpatient_imaging   ->  18,500
    cms_infections           -> 172,404
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver_schema.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_time    TIMESTAMP;
    v_end_time      TIMESTAMP;
    v_batch_start   TIMESTAMP;
    v_batch_end     TIMESTAMP;
    v_row_count     BIGINT;
BEGIN
    v_batch_start := clock_timestamp();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE 'Batch Start: %', v_batch_start;
    RAISE NOTICE '================================================';


    -- ----------------------------------------------------------------
    -- 1. cms_hospital_general
    -- ----------------------------------------------------------------
    BEGIN
        v_start_time := clock_timestamp();
        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE '>> Truncating Table: silver_schema.cms_hospital_general';
        TRUNCATE TABLE silver_schema.cms_hospital_general;

        RAISE NOTICE '>> Loading Data Into: silver_schema.cms_hospital_general';
        INSERT INTO silver_schema.cms_hospital_general (
            facility_id, facility_name, address, city, state, zip_code, county,
            hospital_type, hospital_ownership, hospital_ownership_details,
            emergency_services, birthing_friendly_designation,
            hospital_overall_rating, hospital_overall_rating_footnote,
            mort_group_measures_count, facility_mort_measures_count,
            mort_measures_better, mort_measures_no_different, mort_measures_worse,
            mort_group_footnote, mort_not_reported, mort_not_participating,
            mort_dod_hospital, mort_data_issue, mort_reporting_status,
            safety_group_measures_count, facility_safety_measures_count,
            safety_measures_better, safety_measures_no_different, safety_measures_worse,
            safety_group_footnote, safety_not_reported, safety_not_participating,
            safety_dod_hospital, safety_data_issue, safety_reporting_status,
            readm_group_measures_count, facility_readm_measures_count,
            readm_measures_better, readm_measures_no_different, readm_measures_worse,
            readm_group_footnote, readm_not_reported, readm_not_participating,
            readm_data_issue, readm_dod_hospital, readm_reporting_status,
            pt_exp_group_measures_count, facility_pt_exp_measures_count,
            pt_exp_group_footnote, pt_exp_not_reported, pt_exp_not_participating,
            pt_exp_data_issue, pt_exp_dod_hospital, pt_exp_reporting_status,
            te_group_measures_count, facility_te_measures_count,
            te_group_footnote, te_not_reported, te_not_participating,
            te_data_issue, te_dod_hospital, te_reporting_status
        )
        SELECT
            TRIM(facility_id) AS facility_id,
            TRIM(facility_name) AS facility_name,
            TRIM(address) AS address,
            TRIM(city) AS city,
            TRIM(UPPER(state)) AS state,
            TRIM(zip_code) AS zip_code,
            TRIM(county) AS county,
            TRIM(hospital_type) AS hospital_type,
            CASE
                WHEN hospital_ownership ILIKE '%government%' OR hospital_ownership ILIKE '%veterans%' OR
                     hospital_ownership ILIKE '%defense%' THEN 'Government'
                WHEN hospital_ownership ILIKE '%non-profit%' THEN 'Non-Profit'
                WHEN hospital_ownership = 'Proprietary' OR hospital_ownership = 'Physician' THEN 'For-Profit'
                WHEN hospital_ownership = 'Tribal' THEN 'Tribal'
                ELSE 'Other'
            END AS hospital_ownership,
            TRIM(hospital_ownership) AS hospital_ownership_details,
            CASE
                WHEN emergency_services = 'Yes' THEN TRUE
                WHEN emergency_services = 'No' THEN FALSE
                ELSE NULL
            END AS emergency_services,
            CASE
                WHEN birthing_friendly_designation = 'Y' THEN TRUE
                ELSE NULL
            END AS birthing_friendly_designation,
            CASE
                WHEN hospital_overall_rating ~ '^\d+$' THEN hospital_overall_rating::INT
                ELSE NULL
            END AS hospital_overall_rating,
            TRIM(hospital_overall_rating_footnote) AS hospital_overall_rating_footnote,
            -- Mortality Group
            CASE WHEN mort_group_measure_count ~ '^\d+$' THEN mort_group_measure_count::INT ELSE NULL END AS mort_group_measures_count,
            CASE WHEN facility_mort_measure_count ~ '^\d+$' THEN facility_mort_measure_count::INT ELSE NULL END AS facility_mort_measures_count,
            CASE WHEN mort_measures_better ~ '^\d+$' THEN mort_measures_better::INT ELSE NULL END AS mort_measures_better,
            CASE WHEN mort_measures_no_different ~ '^\d+$' THEN mort_measures_no_different::INT ELSE NULL END AS mort_measures_no_different,
            CASE WHEN mort_measures_worse ~ '^\d+$' THEN mort_measures_worse::INT ELSE NULL END AS mort_measures_worse,
            TRIM(mort_group_footnote) AS mort_group_footnote,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(mort_group_footnote, ',')) x) && ARRAY['5'] THEN TRUE ELSE FALSE END AS mort_not_reported,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(mort_group_footnote, ',')) x) && ARRAY['19'] THEN TRUE ELSE FALSE END AS mort_not_participating,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(mort_group_footnote, ',')) x) && ARRAY['22'] THEN TRUE ELSE FALSE END AS mort_dod_hospital,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(mort_group_footnote, ',')) x) && ARRAY['23'] THEN TRUE ELSE FALSE END AS mort_data_issue,

            CASE
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(mort_group_footnote, ',')) x) && ARRAY['22'] THEN 'Federal (DoD, VA)'
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(mort_group_footnote, ',')) x) && ARRAY['19'] THEN 'Not Participating'
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(mort_group_footnote, ',')) x) && ARRAY['23'] THEN 'Data Quality Issue'
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(mort_group_footnote, ',')) x) && ARRAY['5'] THEN 'Insufficient Data'
                WHEN facility_mort_measure_count IS NOT NULL AND facility_mort_measure_count != ''
                         AND facility_mort_measure_count ~ '^\d+$'
                         AND mort_group_measure_count ~ '^\d+$'
                         AND facility_mort_measure_count::INT = mort_group_measure_count::INT THEN 'Fully Rated'
                WHEN facility_mort_measure_count IS NOT NULL
                         AND facility_mort_measure_count != ''
                         AND facility_mort_measure_count ~ '^\d+$'
                         AND facility_mort_measure_count::INT > 0 THEN 'Partially Rated'
                ELSE 'Not Rated'
            END AS mort_reporting_status,
            -- Safety Group
            CASE WHEN safety_group_measure_count ~ '^\d+$' THEN safety_group_measure_count::INT ELSE NULL END AS safety_group_measures_count,
            CASE WHEN facility_safety_measure_count ~ '^\d+$' THEN facility_safety_measure_count::INT ELSE NULL END AS facility_safety_measures_count,
            CASE WHEN safety_measures_better ~ '^\d+$' THEN safety_measures_better::INT ELSE NULL END AS safety_measures_better,
            CASE WHEN safety_measures_no_different ~ '^\d+$' THEN safety_measures_no_different::INT ELSE NULL END AS safety_measures_no_different,
            CASE WHEN safety_measures_worse ~ '^\d+$' THEN safety_measures_worse::INT ELSE NULL END AS safety_measures_worse,
            TRIM(safety_group_footnote) AS safety_group_footnote,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(safety_group_footnote, ',')) x) && ARRAY['5'] THEN TRUE ELSE FALSE END AS safety_not_reported,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(safety_group_footnote, ',')) x) && ARRAY['19'] THEN TRUE ELSE FALSE END AS safety_not_participating,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(safety_group_footnote, ',')) x) && ARRAY['22'] THEN TRUE ELSE FALSE END AS safety_dod_hospital,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(safety_group_footnote, ',')) x) && ARRAY['23'] THEN TRUE ELSE FALSE END AS safety_data_issue,
            CASE
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(safety_group_footnote, ',')) x) && ARRAY['22'] THEN 'Federal (DoD, VA)'
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(safety_group_footnote, ',')) x) && ARRAY['19'] THEN 'Not Participating'
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(safety_group_footnote, ',')) x) && ARRAY['23'] THEN 'Data Quality Issue'
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(safety_group_footnote, ',')) x) && ARRAY['5'] THEN 'Insufficient Data'
                WHEN facility_safety_measure_count IS NOT NULL AND facility_safety_measure_count != ''
                         AND facility_safety_measure_count ~ '^\d+$'
                         AND safety_group_measure_count ~ '^\d+$'
                         AND facility_safety_measure_count::INT = safety_group_measure_count::INT THEN 'Fully Rated'
                WHEN facility_safety_measure_count IS NOT NULL
                         AND facility_safety_measure_count != ''
                         AND facility_safety_measure_count ~ '^\d+$'
                         AND facility_safety_measure_count::INT > 0 THEN 'Partially Rated'
                ELSE 'Not Rated'
            END AS safety_reporting_status,
            -- READM Group
            CASE WHEN readm_group_measure_count ~ '^\d+$' THEN readm_group_measure_count::INT ELSE NULL END AS readm_group_measures_count,
            CASE WHEN facility_readm_measure_count ~ '^\d+$' THEN facility_readm_measure_count::INT ELSE NULL END AS facility_readm_measures_count,
            CASE WHEN readm_measures_better ~ '^\d+$' THEN readm_measures_better::INT ELSE NULL END AS readm_measures_better,
            CASE WHEN readm_measures_no_different ~ '^\d+$' THEN readm_measures_no_different::INT ELSE NULL END AS readm_measures_no_different,
            CASE WHEN readm_measures_worse ~ '^\d+$' THEN readm_measures_worse::INT ELSE NULL END AS readm_measures_worse,
            TRIM(readm_group_footnote) AS readm_group_footnote,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(readm_group_footnote, ',')) x) && ARRAY['5'] THEN TRUE ELSE FALSE END AS readm_not_reported,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(readm_group_footnote, ',')) x) && ARRAY['19'] THEN TRUE ELSE FALSE END AS readm_not_participating,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(readm_group_footnote, ',')) x) && ARRAY['22'] THEN TRUE ELSE FALSE END AS readm_dod_hospital,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(readm_group_footnote, ',')) x) && ARRAY['23'] THEN TRUE ELSE FALSE END AS readm_data_issue,
            CASE
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(readm_group_footnote, ',')) x) && ARRAY['22'] THEN 'Federal (DoD, VA)'
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(readm_group_footnote, ',')) x) && ARRAY['19'] THEN 'Not Participating'
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(readm_group_footnote, ',')) x) && ARRAY['23'] THEN 'Data Quality Issue'
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(readm_group_footnote, ',')) x) && ARRAY['5'] THEN 'Insufficient Data'
                WHEN facility_readm_measure_count IS NOT NULL AND facility_readm_measure_count != ''
                         AND facility_readm_measure_count ~ '^\d+$'
                         AND readm_group_measure_count ~ '^\d+$'
                         AND facility_readm_measure_count::INT = readm_group_measure_count::INT THEN 'Fully Rated'
                WHEN facility_readm_measure_count IS NOT NULL
                         AND facility_readm_measure_count != ''
                         AND facility_readm_measure_count ~ '^\d+$'
                         AND facility_readm_measure_count::INT > 0 THEN 'Partially Rated'
                ELSE 'Not Rated'
            END AS readm_reporting_status,
            -- Patient Experience Group
            CASE WHEN pt_exp_group_measure_count ~ '^\d+$' THEN pt_exp_group_measure_count::INT ELSE NULL END AS pt_exp_group_measures_count,
            CASE WHEN facility_pt_exp_measure_count ~ '^\d+$' THEN facility_pt_exp_measure_count::INT ELSE NULL END AS facility_pt_exp_measures_count,
            TRIM(pt_exp_group_footnote) AS pt_exp_group_footnote,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(pt_exp_group_footnote, ',')) x) && ARRAY['5'] THEN TRUE ELSE FALSE END AS pt_exp_not_reported,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(pt_exp_group_footnote, ',')) x) && ARRAY['19'] THEN TRUE ELSE FALSE END AS pt_exp_not_participating,
            FALSE AS pt_exp_data_issue,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(pt_exp_group_footnote, ',')) x) && ARRAY['22'] THEN TRUE ELSE FALSE END AS pt_exp_dod_hospital,
            CASE
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(pt_exp_group_footnote, ',')) x) && ARRAY['22'] THEN 'Federal (DoD, VA)'
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(pt_exp_group_footnote, ',')) x) && ARRAY['19'] THEN 'Not Participating'
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(pt_exp_group_footnote, ',')) x) && ARRAY['5'] THEN 'Insufficient Data'
                WHEN facility_pt_exp_measure_count IS NOT NULL AND facility_pt_exp_measure_count != ''
                         AND facility_pt_exp_measure_count ~ '^\d+$'
                         AND pt_exp_group_measure_count ~ '^\d+$'
                         AND facility_pt_exp_measure_count::INT = pt_exp_group_measure_count::INT THEN 'Fully Rated'
                WHEN facility_pt_exp_measure_count IS NOT NULL
                         AND facility_pt_exp_measure_count != ''
                         AND facility_pt_exp_measure_count ~ '^\d+$'
                         AND facility_pt_exp_measure_count::INT > 0 THEN 'Partially Rated'
                ELSE 'Not Rated'
            END AS pt_exp_reporting_status,
            -- Timely & Effective Care Group
            CASE WHEN te_group_measure_count ~ '^\d+$' THEN te_group_measure_count::INT ELSE NULL END AS te_group_measures_count,
            CASE WHEN facility_te_measure_count ~ '^\d+$' THEN facility_te_measure_count::INT ELSE NULL END AS facility_te_measures_count,
            TRIM(te_group_footnote) AS te_group_footnote,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(te_group_footnote, ',')) x) && ARRAY['5'] THEN TRUE ELSE FALSE END AS te_not_reported,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(te_group_footnote, ',')) x) && ARRAY['19'] THEN TRUE ELSE FALSE END AS te_not_participating,
            FALSE AS te_data_issue,
            CASE WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(te_group_footnote, ',')) x) && ARRAY['22'] THEN TRUE ELSE FALSE END AS te_dod_hospital,
            CASE
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(te_group_footnote, ',')) x) && ARRAY['22'] THEN 'Federal (DoD, VA)'
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(te_group_footnote, ',')) x) && ARRAY['19'] THEN 'Not Participating'
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(te_group_footnote, ',')) x) && ARRAY['5'] THEN 'Insufficient Data'
                WHEN facility_te_measure_count IS NOT NULL AND facility_te_measure_count != ''
                         AND facility_te_measure_count ~ '^\d+$'
                         AND te_group_measure_count ~ '^\d+$'
                         AND facility_te_measure_count::INT = te_group_measure_count::INT THEN 'Fully Rated'
                WHEN facility_te_measure_count IS NOT NULL
                         AND facility_te_measure_count != ''
                         AND facility_te_measure_count ~ '^\d+$'
                         AND facility_te_measure_count::INT > 0 THEN 'Partially Rated'
                ELSE 'Not Rated'
            END AS te_reporting_status
        FROM bronze_schema.cms_hospital_general;

        SELECT COUNT(*) INTO v_row_count FROM silver_schema.cms_hospital_general;
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Rows Loaded: %', v_row_count;
        RAISE NOTICE '>> Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC(10,2);
        RAISE NOTICE '>> -------------';

    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING '!! ERROR loading cms_hospital_general: % (SQLSTATE: %)', SQLERRM, SQLSTATE;
    END;


    -- ----------------------------------------------------------------
    -- 2. cms_timely_care
    -- ----------------------------------------------------------------
    BEGIN
        v_start_time := clock_timestamp();
        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE '>> Truncating Table: silver_schema.cms_timely_care';
        TRUNCATE TABLE silver_schema.cms_timely_care;

        RAISE NOTICE '>> Loading Data Into: silver_schema.cms_timely_care';
        INSERT INTO silver_schema.cms_timely_care (
            facility_id, facility_name, address, city, state, zip_code, county,
            condition, measure_id, measure_name,
            score_numeric, score_text, score_available, is_score_usable, score_exclusion_reason,
            sample, footnote, start_date, end_date
        )
        SELECT
            TRIM(facility_id) AS facility_id,
            TRIM(facility_name) AS facility_name,
            TRIM(address) AS address,
            TRIM(city) AS city,
            TRIM(UPPER(state)) AS state,
            TRIM(zip_code) AS zip_code,
            TRIM(county) AS county,
            TRIM(condition) AS condition,
            TRIM(measure_id) AS measure_id,
            TRIM(measure_name) AS measure_name,
            CASE WHEN score ~ '^\d+\.?\d*$' THEN score::NUMERIC ELSE NULL END AS score_numeric,
            CASE WHEN score IS NOT NULL AND score != '' AND score != 'Not Available' AND score != 'N/A'
                      AND score !~ '^\d+\.?\d*$' THEN score ELSE NULL END AS score_text,
            CASE WHEN score IS NOT NULL AND score != '' AND score != 'Not Available' AND score != 'N/A'
                 THEN TRUE ELSE FALSE END AS score_available,
            CASE
                WHEN score IS NULL OR score = '' OR score = 'Not Available' OR score = 'N/A' THEN FALSE
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                     && ARRAY['1', '2', '3', '4', '5', '7', '8', '12', '13', '23', '28', '29'] THEN FALSE
                ELSE TRUE
            END AS is_score_usable,
            -- score_exclusion_reason: footnote codes checked first so multi-code rows get highest-priority tier
            -- Priority: No Score (1,4,5) > Not Applicable (7,12,13) > Use With Caution (2,3,11,23,28,29) > generic No Score
            CASE
            WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                 && ARRAY['1','4','5'] THEN 'No Score'
            WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                 && ARRAY['7','12','13'] THEN 'Not Applicable'
            WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                 && ARRAY['2', '3', '11', '23', '28', '29'] THEN 'Use With Caution'
            WHEN score IS NULL OR score = '' OR score = 'Not Available' OR score = 'N/A' THEN 'No Score'
            ELSE NULL
        END AS score_exclusion_reason,
            CASE WHEN sample ~ '^\d+$' THEN sample::INT ELSE NULL END AS sample_size,
            TRIM(footnote) AS footnote,
            CASE WHEN start_date IS NULL OR start_date = '' THEN NULL
                 WHEN start_date ~ '^\d{2}/\d{2}/\d{4}' THEN TO_DATE(start_date, 'MM/DD/YYYY')
                 ELSE NULL END AS start_date,
            CASE WHEN end_date IS NULL OR end_date = '' THEN NULL
                 WHEN end_date ~ '^\d{2}/\d{2}/\d{4}' THEN TO_DATE(end_date, 'MM/DD/YYYY')
                 ELSE NULL END AS end_date
        FROM bronze_schema.cms_timely_care;

        SELECT COUNT(*) INTO v_row_count FROM silver_schema.cms_timely_care;
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Rows Loaded: %', v_row_count;
        RAISE NOTICE '>> Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC(10,2);
        RAISE NOTICE '>> -------------';

    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING '!! ERROR loading cms_timely_care: % (SQLSTATE: %)', SQLERRM, SQLSTATE;
    END;


    -- ----------------------------------------------------------------
    -- 3. cms_complications
    -- ----------------------------------------------------------------
    BEGIN
        v_start_time := clock_timestamp();
        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE '>> Truncating Table: silver_schema.cms_complications';
        TRUNCATE TABLE silver_schema.cms_complications;

        RAISE NOTICE '>> Loading Data Into: silver_schema.cms_complications';
        INSERT INTO silver_schema.cms_complications (
            facility_id, facility_name, address, city, state, zip_code, county,
            measure_id, measure_name, compared_to_national,
            denominator, score, lower_estimate, higher_estimate,
            score_available, is_score_usable, score_exclusion_reason, footnote, start_date, end_date
        )
        SELECT
            TRIM(facility_id) AS facility_id,
            TRIM(facility_name) AS facility_name,
            TRIM(address) AS address,
            TRIM(city) AS city,
            TRIM(UPPER(state)) AS state,
            TRIM(zip_code) AS zip_code,
            TRIM(county) AS county,
            TRIM(measure_id) AS measure_id,
            TRIM(measure_name) AS measure_name,
            CASE WHEN compared_to_national ILIKE '%better%' THEN 'Better'
                 WHEN compared_to_national ILIKE '%no different%' THEN 'No Different'
                 WHEN compared_to_national ILIKE '%worse%' THEN 'Worse'
                 ELSE NULL END AS compared_to_national,
            CASE WHEN denominator ~ '^\d+$' THEN denominator::INT ELSE NULL END AS denominator,
            CASE WHEN score ~ '^\d+\.?\d*$' THEN score::NUMERIC ELSE NULL END AS score,
            CASE WHEN lower_estimate ~ '^\d+\.?\d*$' THEN lower_estimate::NUMERIC ELSE NULL END AS lower_estimate,
            CASE WHEN higher_estimate ~ '^\d+\.?\d*$' THEN higher_estimate::NUMERIC ELSE NULL END AS higher_estimate,
            CASE WHEN score IS NOT NULL AND score != '' AND score != 'Not Available' AND score != 'N/A'
                 THEN TRUE ELSE FALSE END AS score_available,
            CASE
                WHEN score IS NULL OR score = '' OR score = 'Not Available' OR score = 'N/A' THEN FALSE
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                     && ARRAY['1', '2', '3', '4', '5', '7', '8', '12', '13', '23', '28', '29'] THEN FALSE
                ELSE TRUE
            END AS is_score_usable,
            -- score_exclusion_reason: footnote codes checked first so multi-code rows get highest-priority tier
            -- Priority: No Score (1,4,5) > Not Applicable (7,12,13) > Use With Caution (2,3,11,23,28,29) > generic No Score
            CASE
            WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                 && ARRAY['1','4','5'] THEN 'No Score'
            WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                 && ARRAY['7','12','13'] THEN 'Not Applicable'
            WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                 && ARRAY['2', '3', '11', '23', '28', '29'] THEN 'Use With Caution'
            WHEN score IS NULL OR score = '' OR score = 'Not Available' OR score = 'N/A' THEN 'No Score'
            ELSE NULL
        END AS score_exclusion_reason,
            TRIM(footnote) AS footnote,
            CASE WHEN start_date IS NULL OR start_date = '' THEN NULL
                 WHEN start_date ~ '^\d{2}/\d{2}/\d{4}' THEN TO_DATE(start_date, 'MM/DD/YYYY')
                 ELSE NULL END AS start_date,
            CASE WHEN end_date IS NULL OR end_date = '' THEN NULL
                 WHEN end_date ~ '^\d{2}/\d{2}/\d{4}' THEN TO_DATE(end_date, 'MM/DD/YYYY')
                 ELSE NULL END AS end_date
        FROM bronze_schema.cms_complications;

        SELECT COUNT(*) INTO v_row_count FROM silver_schema.cms_complications;
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Rows Loaded: %', v_row_count;
        RAISE NOTICE '>> Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC(10,2);
        RAISE NOTICE '>> -------------';

    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING '!! ERROR loading cms_complications: % (SQLSTATE: %)', SQLERRM, SQLSTATE;
    END;


    -- ----------------------------------------------------------------
    -- 4. cms_outpatient_imaging
    -- ----------------------------------------------------------------
    BEGIN
        v_start_time := clock_timestamp();
        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE '>> Truncating Table: silver_schema.cms_outpatient_imaging';
        TRUNCATE TABLE silver_schema.cms_outpatient_imaging;

        RAISE NOTICE '>> Loading Data Into: silver_schema.cms_outpatient_imaging';
        INSERT INTO silver_schema.cms_outpatient_imaging (
            facility_id, facility_name, address, city, state, zip_code, county,
            measure_id, measure_name, score, score_available, is_score_usable, score_exclusion_reason,
            footnote, start_date, end_date
        )
        SELECT
            TRIM(facility_id) AS facility_id,
            TRIM(facility_name) AS facility_name,
            TRIM(address) AS address,
            TRIM(city) AS city,
            TRIM(UPPER(state)) AS state,
            TRIM(zip_code) AS zip_code,
            TRIM(county) AS county,
            TRIM(measure_id) AS measure_id,
            TRIM(measure_name) AS measure_name,
            CASE WHEN score ~ '^\d+\.?\d*$' THEN score::NUMERIC ELSE NULL END AS score,
            CASE WHEN score IS NOT NULL AND score != '' AND score != 'Not Available' AND score != 'N/A'
                 THEN TRUE ELSE FALSE END AS score_available,
            CASE
                WHEN score IS NULL OR score = '' OR score = 'Not Available' OR score = 'N/A' THEN FALSE
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                     && ARRAY['1', '2', '3', '4', '5', '7', '8', '12', '13', '23', '28', '29'] THEN FALSE
                ELSE TRUE
            END AS is_score_usable,
            -- score_exclusion_reason: footnote codes checked first so multi-code rows get highest-priority tier
            -- Priority: No Score (1,4,5) > Not Applicable (7,12,13) > Use With Caution (2,3,11,23,28,29) > generic No Score
            CASE
            WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                 && ARRAY['1','4','5'] THEN 'No Score'
            WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                 && ARRAY['7','12','13'] THEN 'Not Applicable'
            WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                 && ARRAY['2', '3', '11', '23', '28', '29'] THEN 'Use With Caution'
            WHEN score IS NULL OR score = '' OR score = 'Not Available' OR score = 'N/A' THEN 'No Score'
            ELSE NULL
        END AS score_exclusion_reason,
            TRIM(footnote) AS footnote,
            CASE WHEN start_date IS NULL OR start_date = '' THEN NULL
                 WHEN start_date ~ '^\d{2}/\d{2}/\d{4}' THEN TO_DATE(start_date, 'MM/DD/YYYY')
                 ELSE NULL END AS start_date,
            CASE WHEN end_date IS NULL OR end_date = '' THEN NULL
                 WHEN end_date ~ '^\d{2}/\d{2}/\d{4}' THEN TO_DATE(end_date, 'MM/DD/YYYY')
                 ELSE NULL END AS end_date
        FROM bronze_schema.cms_outpatient_imaging;

        SELECT COUNT(*) INTO v_row_count FROM silver_schema.cms_outpatient_imaging;
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Rows Loaded: %', v_row_count;
        RAISE NOTICE '>> Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC(10,2);
        RAISE NOTICE '>> -------------';

    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING '!! ERROR loading cms_outpatient_imaging: % (SQLSTATE: %)', SQLERRM, SQLSTATE;
    END;


    -- ----------------------------------------------------------------
    -- 5. cms_infections
    -- ----------------------------------------------------------------
    BEGIN
        v_start_time := clock_timestamp();
        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE '>> Truncating Table: silver_schema.cms_infections';
        TRUNCATE TABLE silver_schema.cms_infections;

        RAISE NOTICE '>> Loading Data Into: silver_schema.cms_infections';
        INSERT INTO silver_schema.cms_infections (
            facility_id, facility_name, address, city, state, zip_code, county,
            measure_id, measure_suffix, measure_name,
            compared_to_national, score, score_available, is_score_usable, score_exclusion_reason,
            footnote, start_date, end_date
        )
        SELECT
            TRIM(facility_id) AS facility_id,
            TRIM(facility_name) AS facility_name,
            TRIM(address) AS address,
            TRIM(city) AS city,
            TRIM(UPPER(state)) AS state,
            TRIM(zip_code) AS zip_code,
            TRIM(county) AS county,
            TRIM(measure_id) AS measure_id,
            TRIM(SUBSTRING(measure_id FROM '[^_]+$')) AS measure_suffix,
            TRIM(measure_name) AS measure_name,
            CASE WHEN compared_to_national ILIKE '%better%' THEN 'Better'
                 WHEN compared_to_national ILIKE '%no different%' THEN 'No Different'
                 WHEN compared_to_national ILIKE '%worse%' THEN 'Worse'
                 ELSE NULL END AS compared_to_national,
            CASE WHEN score ~ '^\d+\.?\d*$' THEN score::NUMERIC ELSE NULL END AS score,
            CASE WHEN score IS NOT NULL AND score != '' AND score != 'Not Available' AND score != 'N/A'
                 THEN TRUE ELSE FALSE END AS score_available,
            CASE
                WHEN score IS NULL OR score = '' OR score = 'Not Available' OR score = 'N/A' THEN FALSE
                WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                     && ARRAY['1', '2', '3', '4', '5', '7', '8', '12', '13', '23', '28', '29'] THEN FALSE
                ELSE TRUE
            END AS is_score_usable,
            -- score_exclusion_reason: footnote codes checked first so multi-code rows get highest-priority tier
            -- Priority: Zero Infections (8) > No Score (1,4,5) > Not Applicable (7,12,13) > Use With Caution (2,3,11,23,28,29) > generic No Score
            CASE
            WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                 && ARRAY['8'] THEN 'No Score - Zero Infections'
            WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                 && ARRAY['1','4','5'] THEN 'No Score'
            WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                 && ARRAY['7','12','13'] THEN 'Not Applicable'
            WHEN ARRAY(SELECT TRIM(x) FROM unnest(string_to_array(footnote, ',')) x)
                 && ARRAY['2', '3', '11', '23', '28', '29'] THEN 'Use With Caution'
            WHEN score IS NULL OR score = '' OR score = 'Not Available' OR score = 'N/A' THEN 'No Score'
            ELSE NULL
        END AS score_exclusion_reason,
            TRIM(footnote) AS footnote,
            CASE WHEN start_date IS NULL OR start_date = '' THEN NULL
                 WHEN start_date ~ '^\d{2}/\d{2}/\d{4}' THEN TO_DATE(start_date, 'MM/DD/YYYY')
                 ELSE NULL END AS start_date,
            CASE WHEN end_date IS NULL OR end_date = '' THEN NULL
                 WHEN end_date ~ '^\d{2}/\d{2}/\d{4}' THEN TO_DATE(end_date, 'MM/DD/YYYY')
                 ELSE NULL END AS end_date
        FROM bronze_schema.cms_infections;

        SELECT COUNT(*) INTO v_row_count FROM silver_schema.cms_infections;
        v_end_time := clock_timestamp();
        RAISE NOTICE '>> Rows Loaded: %', v_row_count;
        RAISE NOTICE '>> Duration: % seconds', EXTRACT(EPOCH FROM (v_end_time - v_start_time))::NUMERIC(10,2);
        RAISE NOTICE '>> -------------';

    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING '!! ERROR loading cms_infections: % (SQLSTATE: %)', SQLERRM, SQLSTATE;
    END;


    -- ----------------------------------------------------------------
    -- Batch Summary
    -- ----------------------------------------------------------------
    v_batch_end := clock_timestamp();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer Completed';
    RAISE NOTICE 'Batch End: %', v_batch_end;
    RAISE NOTICE 'Total Duration: % seconds', EXTRACT(EPOCH FROM (v_batch_end - v_batch_start))::NUMERIC(10,2);
    RAISE NOTICE '================================================';

END;
$$;
