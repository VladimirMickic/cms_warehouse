/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    Creates tables in the 'bronze_schema' schema, dropping existing tables
    if they already exist. All columns are TEXT to avoid import errors.
    Type casting and cleaning happens in Silver.
 
Data Source:
    Centers for Medicare & Medicaid Services (CMS) Hospital Compare
    https://data.cms.gov/provider-data/topics/hospitals
===============================================================================
*/

CALL bronze_schema.load_bronze();
CREATE OR REPLACE PROCEDURE bronze_schema.load_bronze()
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
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE 'Batch Start: %', v_batch_start;
    RAISE NOTICE '================================================';


    -- ----------------------------------------------------------------
    -- 1. cms_hospital_general
    -- ----------------------------------------------------------------
    BEGIN
        v_start_time := clock_timestamp();
        RAISE NOTICE '------------------------------------------------';
        RAISE NOTICE '>> Truncating Table: bronze_schema.cms_hospital_general';
        TRUNCATE TABLE bronze_schema.cms_hospital_general;

        RAISE NOTICE '>> Loading Data Into: bronze_schema.cms_hospital_general';
        COPY bronze_schema.cms_hospital_general (
            facility_id, facility_name, address, city, state, zip_code,
            county, telephone_number, hospital_type, hospital_ownership,
            emergency_services, birthing_friendly_designation,
            hospital_overall_rating, hospital_overall_rating_footnote,
            mort_group_measure_count, facility_mort_measure_count,
            mort_measures_better, mort_measures_no_different, mort_measures_worse,
            mort_group_footnote,
            safety_group_measure_count, facility_safety_measure_count,
            safety_measures_better, safety_measures_no_different, safety_measures_worse,
            safety_group_footnote,
            readm_group_measure_count, facility_readm_measure_count,
            readm_measures_better, readm_measures_no_different, readm_measures_worse,
            readm_group_footnote,
            pt_exp_group_measure_count, facility_pt_exp_measure_count,
            pt_exp_group_footnote,
            te_group_measure_count, facility_te_measure_count,
            te_group_footnote
        )
        FROM '/tmp/Hospital_General_Information.csv'
        WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

        SELECT COUNT(*) INTO v_row_count FROM bronze_schema.cms_hospital_general;
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
        RAISE NOTICE '>> Truncating Table: bronze_schema.cms_timely_care';
        TRUNCATE TABLE bronze_schema.cms_timely_care;

        RAISE NOTICE '>> Loading Data Into: bronze_schema.cms_timely_care';
        COPY bronze_schema.cms_timely_care (
            facility_id, facility_name, address, city, state, zip_code,
            county, telephone_number, condition, measure_id, measure_name,
            score, sample, footnote, start_date, end_date
        )
        FROM '/tmp/Timely_and_Effective_Care-Hospital.csv'
        WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

        SELECT COUNT(*) INTO v_row_count FROM bronze_schema.cms_timely_care;
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
        RAISE NOTICE '>> Truncating Table: bronze_schema.cms_complications';
        TRUNCATE TABLE bronze_schema.cms_complications;

        RAISE NOTICE '>> Loading Data Into: bronze_schema.cms_complications';
        COPY bronze_schema.cms_complications (
            facility_id, facility_name, address, city, state, zip_code,
            county, telephone_number, measure_id, measure_name,
            compared_to_national, denominator, score,
            lower_estimate, higher_estimate, footnote,
            start_date, end_date
        )
        FROM '/tmp/Complications_and_Deaths-Hospital.csv'
        WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

        SELECT COUNT(*) INTO v_row_count FROM bronze_schema.cms_complications;
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
        RAISE NOTICE '>> Truncating Table: bronze_schema.cms_outpatient_imaging';
        TRUNCATE TABLE bronze_schema.cms_outpatient_imaging;

        RAISE NOTICE '>> Loading Data Into: bronze_schema.cms_outpatient_imaging';
        COPY bronze_schema.cms_outpatient_imaging (
            facility_id, facility_name, address, city, state, zip_code,
            county, telephone_number, measure_id, measure_name,
            score, footnote, start_date, end_date
        )
        FROM '/tmp/Outpatient_Imaging_Efficiency-Hospital.csv'
        WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

        SELECT COUNT(*) INTO v_row_count FROM bronze_schema.cms_outpatient_imaging;
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
        RAISE NOTICE '>> Truncating Table: bronze_schema.cms_infections';
        TRUNCATE TABLE bronze_schema.cms_infections;

        RAISE NOTICE '>> Loading Data Into: bronze_schema.cms_infections';
        COPY bronze_schema.cms_infections (
            facility_id, facility_name, address, city, state, zip_code,
            county, telephone_number, measure_id, measure_name,
            compared_to_national, score, footnote,
            start_date, end_date
        )
        FROM '/tmp/Healthcare_Associated_Infections-Hospital.csv'
        WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

        SELECT COUNT(*) INTO v_row_count FROM bronze_schema.cms_infections;
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
    RAISE NOTICE 'Loading Bronze Layer Completed';
    RAISE NOTICE 'Batch End: %', v_batch_end;
    RAISE NOTICE 'Total Duration: % seconds', EXTRACT(EPOCH FROM (v_batch_end - v_batch_start))::NUMERIC(10,2);
    RAISE NOTICE '================================================';

END;
$$;


/*
===============================================================================
Row Count Verification
===============================================================================
Run this AFTER calling:   CALL bronze_schema.load_bronze();

Expected:
    cms_hospital_general     ->   5,426
    cms_timely_care          -> 138,129
    cms_complications        ->  95,780
    cms_outpatient_imaging   ->  18,500
    cms_infections           -> 172,404
===============================================================================
*/

SELECT 'cms_hospital_general'   AS table_name, COUNT(*) AS row_count FROM bronze_schema.cms_hospital_general
UNION ALL
SELECT 'cms_timely_care',        COUNT(*) FROM bronze_schema.cms_timely_care
UNION ALL
SELECT 'cms_complications',      COUNT(*) FROM bronze_schema.cms_complications
UNION ALL
SELECT 'cms_outpatient_imaging', COUNT(*) FROM bronze_schema.cms_outpatient_imaging
UNION ALL
SELECT 'cms_infections',         COUNT(*) FROM bronze_schema.cms_infections
ORDER BY table_name;
