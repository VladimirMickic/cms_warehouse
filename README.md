# cms_warehouse
# CMS Hospital Quality Data Warehouse

**Business Question:** What patterns exist in US hospital quality outcomes, and how do they vary by ownership type, hospital type, and region?

**Sub-questions driving the analysis:**
1. Do hospitals that over-order imaging also have longer ED wait times, or are these independent problems?
2. How does hospital ownership type relate to imaging efficiency and ED throughput?
3. Which quality measure categories show the most variation across hospitals — and where should improvement efforts focus?
4. Do hospitals with high complication rates also score poorly on process-of-care measures, or can a hospital be operationally efficient but clinically unsafe?

**Tech Stack:** PostgreSQL · DataGrip · Tableau · Claude AI (documented prompting)

**Architecture:** Medallion (Bronze → Silver → Gold) with star schema Gold layer

**Data Source:** [data.cms.gov](https://data.cms.gov/provider-data/topics/hospitals) — official US government CMS public data, not Kaggle

---

## Data Architecture

The project follows Medallion Architecture across three layers:

**Bronze Layer** — Raw data loaded as-is from five CMS CSV files into `bronze_schema`. All columns stored as TEXT. No transformations. Purpose: traceability and debugging. Load method: full load (truncate and insert) via stored procedure with per-table exception handling and timing logs.

**Silver Layer** — Cleaned and standardized data in `silver_schema`. Transformations include type casting, NULL handling, value standardization, derived boolean flags, and ownership consolidation. Load method: full load (truncate and insert) via stored procedure. No data modeling changes — tables mirror Bronze structure with enforced data types and added derived columns.

**Gold Layer** — Business-ready star schema in `gold_schema`. Dimension and fact views optimized for Tableau and analytical queries. This is what the dashboard connects to. *(Status: design complete, build pending)*

---

## Data Sources

All files downloaded from data.cms.gov. Five core CSVs joined on Facility ID (CCN — CMS Certification Number), a 6-character zero-padded string.

| File | Rows | Role | Score Meaning |
|------|------|------|---------------|
| Hospital_General_Information.csv | 5,426 | **Main dimension** — one row per hospital | Overall star rating (1–5 composite). Higher = better |
| Timely_and_Effective_Care.csv | 138,129 | Fact — process of care measures | Mixed units: minutes (ED wait), percentages (vaccination rates), text categories (low/medium/high). Direction varies by measure |
| Complications_and_Deaths.csv | 95,780 | Fact — mortality and safety outcomes | Risk-adjusted rates (%). Lower = better. Includes confidence intervals and national comparison |
| Outpatient_Imaging_Efficiency.csv | 18,500 | Fact — imaging appropriateness | Percentage of potentially unnecessary scans. Lower = better (always) |
| Healthcare_Associated_Infections.csv | 172,404 | Fact — infection rates | Standardized Infection Ratio (SIR). 1.0 = national benchmark. Below 1.0 = better |

**Important:** Score does not mean the same thing across tables. You cannot average scores from different tables — they use different units, scales, and directions. Each measure_id defines its own unit and whether higher or lower is better.

---

## Project Status

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 1 — Setup | ✅ Complete | Database, schemas, folder structure, CSV download |
| Phase 2 — Bronze | ✅ Complete | 5 tables loaded, profiling done, findings documented |
| Phase 3 — Silver | ✅ Complete | Stored procedure built and tested, all 5 tables transformed |
| Phase 4 — Gold | 🔲 Pending | Star schema design complete, SQL views not yet written |
| Phase 5 — Analytics | 🔲 Pending | Business questions defined, queries not yet written |
| Phase 6 — Tableau | 🔲 Pending | 3-tab dashboard planned, not yet built |
| Phase 7 — Documentation | 🟡 In Progress | Bronze findings written, Silver notes partial, README in progress |

---

## Bronze Layer — Profiling Findings

Profiled all 5 CMS datasets. No duplicates found in any table. All tables join successfully on Facility ID. Seven data quality issues identified for Silver layer resolution.

### Source Tables

**cms_hospital_general (5,426 rows)** — Main dimension table, one row per facility. Contains geographic data, demographics (hospital type and ownership), overall quality rating (1–5 stars), service flags (emergency services, birthing friendly designation), grouped measure counts per quality category (mortality, safety, readmission, patient experience, timely care), performance summaries per category (better/worse/same as national), and footnote codes explaining incomplete data.

**cms_timely_care (138,129 rows)** — Process-of-care fact table. Performance metrics for care timeliness: ER wait times, surgical procedures, vaccination rates. One row per hospital × measure × condition. Includes score, sample size (number of patients in the calculation), and footnote codes.

**cms_complications (95,780 rows)** — Outcome fact table. Mortality and complication rates compared to national benchmarks. One row per hospital × measure. Has confidence intervals (lower_estimate, higher_estimate) and compared_to_national categories. Needed standardization due to semantic duplicates.

**cms_outpatient_imaging (18,500 rows)** — Imaging efficiency fact table. Measures appropriate use of CT scans and MRIs. Cleanest source table — score column contains only numeric values or "Not Available." Lower scores indicate more efficient imaging use.

**cms_infections (172,404 rows)** — Infection rate fact table. Healthcare-associated infection rates using 36 HAI measures (6 infection categories × 6 sub-measures). Scores are Standardized Infection Ratios where 1.0 = national benchmark. Some hospitals missing expected measures.

### Issues Found

**1. Score column has both numbers and text categories**
The score column in cms_timely_care contains numeric values mixed with qualitative categories. Found 3,846 rows (~3%) with text: low (1,672), medium (917), very high (704), high (553).
*Silver fix:* Split into three columns — score_numeric (NUMERIC), score_text (VARCHAR for qualitative), score_available (BOOLEAN).

**2. 42–58% of scores are "Not Available"**
Not random — reflects hospital characteristics (too specialized, low volume, data suppression for privacy).
- cms_timely_care: 58.3% (80,534 / 138,129)
- cms_outpatient_imaging: 47.6% (8,810 / 18,500)
- cms_complications: 45.6% (43,646 / 95,780)
- cms_infections: 42.88% (73,930 / 172,404)

*Silver fix:* Keep all rows. Add score_available boolean flag. Use footnote codes to categorize WHY data is missing.

**3. Semantic duplicates in compared_to_national**
Eight text values representing four categories. Inconsistent capitalization and wording ("No Different Than" vs "Not Different Than" vs "No Different than").
*Silver fix:* CASE statements to collapse to four canonical values: Better, No Different, Worse, NULL.

**4. Hospital ownership too granular**
12 distinct values when 4 groups suffice for analysis. Government (4 subtypes), Voluntary non-profit (3 subtypes), Proprietary, Physician, Tribal, others.
*Silver fix:* Consolidate to Government, Non-Profit, For-Profit, Tribal, Other. Keep original value in hospital_ownership_details column for audit trail.

**5. Inconsistent boolean representations**
emergency_services uses Yes/No strings. birthing_friendly_designation uses Y/NULL. In this context NULL means not designated (opt-in certification), not missing data.
*Silver fix:* Cast to proper BOOLEAN. emergency_services = 'Yes' → TRUE. birthing_friendly_designation = 'Y' → TRUE, else FALSE.

**6. Facility ID confirmed as join key**
6-character zero-padded strings. 100% cross-table join success. Note: Excel strips leading zeros — used PostgreSQL for all data work. Python export script available to restore zero-padding if needed.

**7. Some hospitals missing infection measures**
Expected 36 measures per hospital (6 HAI categories × 6 sub-measures). Some hospitals report fewer. Causes: specialized hospitals that don't perform certain procedures, or reporting gaps.
*Silver fix:* Do not impute zeros (would artificially lower averages and bias toward incomplete reporters). Plan to add measure_completeness_pct to Gold layer dim_hospital.

### CMS Footnote Codes Reference

CMS uses numeric codes to explain why scores are missing or flagged. Multiple codes can appear together (e.g., "5,23"). Full mapping from CMS Data Dictionary (Appendix E):

| Code | Meaning | Impact on Analysis |
|------|---------|-------------------|
| 1 | Too few cases to report | Score unreliable or suppressed for privacy |
| 2 | Data based on sample, not full population | Score valid but methodology differs |
| 3 | Results based on shorter time period than required | Score valid but less complete |
| 4 | Data suppressed by CMS for inaccuracies | Score untrustworthy — exclude from analysis |
| 5 | Results not available for reporting period | Hospital didn't submit or had no claims |
| 7 | No cases met the criteria for this measure | Legitimate zero — different from missing |
| 8 | Lower confidence interval limit cannot be calculated (zero infections) | Specific to infection measures — score (SIR) remains valid |
| 11 | Discrepancies in data collection process | Flag for caution |
| 12 | Measure does not apply to this hospital | Hospital doesn't perform the relevant service |
| 13 | Results cannot be calculated | Predicted infections < 1, or MRSA/C.diff above cut-point |
| 19 | Hospital does not participate in IQR/OQR programs | Non-participating — no quality data available |
| 22 | DoD hospital — star ratings not calculated | Military hospitals excluded from star rating system |
| 23 | Hospital reported discrepancies in claims data | Data quality concern flagged by the hospital itself |
| 28 | CMS approved Extraordinary Circumstances Exception | Results may be impacted by approved exception |
| 29 | Partial performance period due to CMS-approved exception | Less data than standard but CMS-approved reason |

**Codes classified as unusable for cross-hospital comparison: 1, 3, 5, 7, 12, 13, 23, 28, 29.** Code 2 (sampled) and code 8 (CI-only impact) are retained as usable.

---

## Silver Layer — Transformations Applied

All transformations implemented in `silver_schema.load_silver()` stored procedure. Per-table exception handling (one failure doesn't abort the batch). RAISE NOTICE logging with clock_timestamp() timing and row counts.

### cms_hospital_general (5,426 rows)
- TRIM on all text fields, UPPER(state)
- Ownership consolidated: Government, Non-Profit, For-Profit, Tribal, Other. Original preserved in hospital_ownership_details
- emergency_services and birthing_friendly_designation cast to BOOLEAN
- hospital_overall_rating cast to INT via regex validation (`~ '^\d+$'`), non-numeric → NULL
- All grouped measure counts (mort, safety, readm, pt_exp, te) cast from TEXT to INT with regex validation
- Footnote codes 5, 19, 22, 23 decoded into boolean flags per measure group (e.g., mort_not_reported, mort_not_participating, mort_dod_hospital, mort_data_issue)
- Derived reporting_status column per group: Fully Rated, Partially Rated, Not Rated, Insufficient Data, Not Participating, Federal (DoD/VA), Data Quality Issue

### cms_timely_care (138,129 rows)
- TRIM on all text fields, UPPER(state)
- Score split into three columns: score_numeric (NUMERIC via regex), score_text (non-numeric qualitative values like "low"/"medium"/"high"), score_available (BOOLEAN)
- `is_score_usable` BOOLEAN flag added — FALSE when score is missing OR footnote contains suppression/exception codes (1, 3, 5, 7, 12, 13, 23, 28, 29)
- Sample cast to INT via regex
- Dates parsed from MM/DD/YYYY text to DATE type with NULL handling for empty strings
- Footnote preserved as original string

### cms_complications (95,780 rows)
- TRIM on all text fields, UPPER(state)
- compared_to_national standardized: ILIKE matching collapsed to Better, No Different, Worse, NULL
- score, lower_estimate, higher_estimate cast to NUMERIC via regex
- denominator cast to INT via regex
- score_available BOOLEAN flag added
- `is_score_usable` BOOLEAN flag added — FALSE when score is missing OR footnote contains suppression/exception codes (1, 3, 5, 7, 12, 13, 23, 28, 29)
- Dates parsed to DATE type
- Footnote preserved as original string

### cms_outpatient_imaging (18,500 rows)
- TRIM on all text fields, UPPER(state)
- Score cast to NUMERIC via regex, non-numeric → NULL
- score_available BOOLEAN flag added
- `is_score_usable` BOOLEAN flag added — FALSE when score is missing OR footnote contains suppression/exception codes (1, 3, 5, 7, 12, 13, 23, 28, 29)
- Dates parsed to DATE type
- Footnote preserved as original string

### cms_infections (172,404 rows)
- TRIM on all text fields, UPPER(state)
- compared_to_national standardized (same logic as complications)
- Score cast to NUMERIC via regex
- score_available BOOLEAN flag added
- `is_score_usable` BOOLEAN flag added — FALSE when score is missing OR footnote contains suppression/exception codes (1, 3, 5, 7, 12, 13, 23, 28, 29)
- measure_suffix derived from measure_id (extracts portion after last underscore)
- Dates parsed to DATE type
- Footnote preserved as original string

### Score Usability Classification

All four fact tables include an `is_score_usable` BOOLEAN column that goes beyond `score_available`. While `score_available` indicates whether a numeric value exists, `is_score_usable` indicates whether that value is trustworthy for cross-hospital comparison. Footnote codes were profiled across all tables, multi-value combinations (e.g., "3, 29") were parsed using `string_to_array` with array overlap, and each code was classified against the CMS Data Dictionary. Codes 1, 3, 5, 7, 12, 13, 23, 28, 29 are classified as unusable — covering suppressed data, shortened reporting periods, CMS-approved exceptions, and hospital-reported discrepancies. Code 2 (sampled data) and code 8 (zero infections affecting only the confidence interval lower bound, not the score itself) are classified as usable. This distinction matters most in the infections table, where ~3,800 rows have valid scores flagged as unusable due to partial reporting periods or extraordinary circumstances exceptions.

---

## Gold Layer — Design (Pending Build)

Star schema with three dimension tables and one central fact table, implemented as PostgreSQL views on the Silver layer.

**gold.dim_hospital** — One row per hospital. Surrogate key (hospital_key), provider_id, name, city, state, zip, county, hospital_type, consolidated ownership, emergency services flag, birthing friendly flag, overall star rating. Primary slicing dimension for all dashboard filters.

**gold.dim_measure** — One row per unique quality measure across all source tables. Surrogate key (measure_key), measure_id, measure_name, derived category (Mortality, Safety, Readmission, Patient Experience, Timely Care, Imaging Efficiency, Infection). Enables grouping and filtering of 50+ individual CMS measures.

**gold.fact_quality_scores** — Central fact table. One row per hospital × measure × reporting period. Foreign keys to dim_hospital and dim_measure. Contains score (NUMERIC), score_available flag, is_score_usable flag, compared_to_national (where available), denominator (where available). The grain: what score did this hospital get on this measure.

**dim_date decision:** The current dataset represents a single CMS reporting cycle. A date dimension is included in the architecture to support future multi-period loads, but kept lean (date_key, full_date, year, quarter) rather than padded with irrelevant attributes like day-of-week or holiday flags.

---

## Data Limitations and Caveats

These should be communicated alongside any findings:

**Survivorship bias** — Only hospitals that participate in CMS reporting appear in this data. Hospitals with poor outcomes may suppress measures. The dataset shows a filtered view of reality.

**No true size metric** — CMS does not publish bed count or patient volume in these files. Hospital type (Critical Access = ≤25 beds) and case denominators serve as proxies. True size-adjusted analysis would require supplementing with the CMS Provider of Services file or AHA hospital survey data.

**47% of hospitals are unrated** — Nearly half have "Not Available" for overall star rating. Unrated hospitals are retained in the warehouse (not filtered out). Dashboard KPIs show rated vs. total counts so stakeholders understand data completeness.

**Scores are not comparable across tables** — Mortality rates (%), infection ratios (SIR with 1.0 benchmark), imaging percentages (lower = better), ED wait times (minutes), and vaccination rates (higher = better) all use different units and directions. Cross-table averaging is invalid.

**Medicare population only** — CMS data reflects Medicare beneficiaries (primarily 65+). Hospital quality for younger or privately insured patients may differ.

**CMS suppression logic** — Hospitals with small case counts have data suppressed for statistical reliability and patient privacy (footnote code 1). This disproportionately affects small rural and specialty hospitals, creating a systematic gap in the data.

Rural data suppression is systemic — Profiling is_score_usable rates by state across all four fact tables reveals a consistent pattern: states with high Critical Access hospital density (MT, SD, WY, KS, ND, NE) show 65–80% unusable scores, while dense urban states (NJ, CT, RI, MD) show 15–30%. This 2–5x gap persists across timely care, complications, imaging, and infections tables, confirming that CMS suppression disproportionately affects rural healthcare systems. State-level comparisons in the dashboard should be interpreted with this asymmetry in mind.

---

## Project Structure

```
cms-hospital-quality/
├── data/
│   ├── raw/                    # Original CMS CSVs (unchanged)
│   └── docs/                   # CMS data dictionary, footnote crosswalk
├── sql/
│   ├── bronze/
│   │   ├── 01_ddl_bronze.sql
│   │   └── 02_proc_load_bronze.sql
│   ├── silver/
│   │   ├── 01_ddl_silver.sql
│   │   └── 02_proc_load_silver.sql
│   ├── gold/                   # (pending)
│   │   ├── 01_dim_hospital.sql
│   │   ├── 02_dim_measure.sql
│   │   ├── 03_dim_date.sql
│   │   └── 04_fact_quality_scores.sql
│   └── analytics/              # (pending)
├── docs/
│   ├── bronze_findings.md
│   ├── silver_notes.md
│   └── data_catalog.md         # (pending)
├── tableau/                    # (pending)
├── README.md
└── .gitignore
```

---

## How to Run Locally

**Prerequisites:** PostgreSQL 14+, DataGrip or psql

```sql
-- 1. Create database and schemas
CREATE DATABASE cms_hospital_quality;
\c cms_hospital_quality
CREATE SCHEMA bronze_schema;
CREATE SCHEMA silver_schema;
CREATE SCHEMA gold_schema;

-- 2. Run Bronze DDL (creates tables with all TEXT columns)
\i sql/bronze/01_ddl_bronze.sql

-- 3. Load Bronze (COPY from CSVs)
CALL bronze_schema.load_bronze();

-- 4. Run Silver DDL (creates tables with proper types + derived columns)
\i sql/silver/01_ddl_silver.sql

-- 5. Load Silver (transforms Bronze → Silver)
CALL silver_schema.load_silver();

-- 6. Run Gold views (pending)
-- 7. Connect Tableau to gold_schema
```
