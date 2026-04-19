# Hospital Quality Analysis — CMS Hospital Compare

**Built with:** PostgreSQL 14 · SQL (CTEs, Window Functions, Stored Procedures, Array Operations) · Tableau

**Author:** Vladimir Mickic · [LinkedIn](https://www.linkedin.com/in/vladimir-mickic/)

**Do hospitals that over-order imaging also have longer ER waits?** Does ownership type predict it? And can a hospital follow every protocol and still be unsafe?

Three questions, 5,400+ hospitals, a bronze-silver-gold pipeline in PostgreSQL to find out.

I started with two intuitive hypotheses: that imaging overuse and ED delays would cluster in the same hospitals (both feel like symptoms of the same problem), and that ownership type would predict which hospitals show that pattern.

Both were wrong. The two are statistically independent (r = -0.020). Ownership tells you where your ED baseline sits, Non-Profits run 20 minutes slower than Government hospitals, but has nothing to do with whether those problems co-occur. The real finding was structural: Non-Profits have the highest star ratings but the slowest emergency departments, clinical quality and operational speed are measuring different things.

## Dashboard

> **[View on Tableau Public →](https://public.tableau.com/app/profile/vladimir.mickic/viz/Hospitals_17760513311400/Dashboard43?publish=yes)**
>
> **[View Project Presentation →](Hospital_Presentation.pdf)**
>
> **[View Data Flow Diagram →](DataFlow.png)**



## Architecture

```
hospital.xlsx → CSV sheets → Bronze (raw TEXT) → Silver (typed + cleaned) → Gold (analytical views)
```

**Bronze** — Raw CMS data loaded as is into 5 tables. Every column is TEXT to avoid import errors. No transforms, no cleaning. This layer exists so there's always an untouched copy to diff against when Silver produces unexpected numbers. Loaded via stored procedure with per-table exception handling and row-count logging.

**Silver** — Type casting, NULL standardization (empty strings + 'Not Available' + NULL all collapse to SQL NULL), and derived analytical columns. The hardest problem here was footnote handling: CMS footnote codes arrive as comma-separated strings like `'3, 13'` that indicate why a score is suppressed. Exact-match checking breaks on multi-code values (matching `'1'` would miss `'1, 5'` or false-match inside `'13'`), so all footnote logic uses PostgreSQL array overlap (`&&` with `string_to_array` and `TRIM`). Footnote codes feed a tiered priority system that classifies every row into `is_score_usable` (boolean) and `score_exclusion_reason` — footnote codes are evaluated before NULL scores, because a row with both needs the footnote-based classification, not the generic 'No Score' label. Ownership is consolidated from 12 CMS values to 4 groups with the original preserved in `hospital_ownership_details`.

**Gold** — Three analytical views, each joining only the Silver tables it needs (no monolithic fact table). `dim_hospital` provides region mapping via US Census Bureau definitions. Views are implemented as standard PostgreSQL views (not materialized) — the query planner pushes filters down into Silver, so the full row set is never materialized unnecessarily.

## Row Counts

| Table | Rows | Domain |
|---|---|---|
| cms_hospital_general | 5,426 | One row per facility — ratings, ownership, location |
| cms_timely_care | 138,129 | ED waits, vaccination rates, surgical timing |
| cms_complications | 95,780 | Mortality and complication rates vs national benchmarks |
| cms_outpatient_imaging | 18,500 | CT/MRI ordering efficiency |
| cms_infections | 172,404 | HAI rates: CLABSI, CAUTI, SSI, MRSA, C.diff |

## Key Transformations

**Ownership:** 12 raw CMS values mapped to 4 groups (Government, Non-Profit, For-Profit, Tribal). Tribal stays separate — ~30 hospitals with a different regulatory framework. Original value kept in `hospital_ownership_details`.

**Score usability:** CMS footnote codes indicate when a score is suppressed, unreliable, or based on too few cases. I parsed these into `is_score_usable` (boolean) and `score_exclusion_reason` with a tiered priority system, footnote codes are evaluated before checking for NULL scores, because multi-code footnotes like '3, 13' need to resolve to the right tier.

**Score splitting (timely_care):** The score column mixes numeric values, categorical text ('very high', 'low'), and 'Not Available' in the same field. Split into `score_numeric`, `score_text`, and `score_available` so each type can be handled correctly.

**compared_to_national:** 8 raw wording variants across complications and infections (e.g., "Better Than the National Rate" vs "Better than National Benchmark") collapsed to 3 values + NULL using ILIKE wildcards.

## Findings

### Q1: Imaging overuse vs ED wait times

3,587 hospitals had both usable imaging and ED scores. The imaging score averages three outpatient imaging measures on a "lower = better" percentage scale: CT contrast overuse (OP-10), cardiac imaging before low-risk surgery (OP-13), and mammography recall rates (OP-39). Pearson r = -0.020, R-squared = 0.0004. No relationship. A hospital with high CT overuse is no more likely to have a long ED wait. They are separate problems driven by separate causes. OP-8 averages 36.9% vs 3–8% for the other three measures — on a completely different scale. Including it would dominate the composite and make the score meaningless.



### Q2: Does ownership explain it?

Ownership doesn't moderate the imaging/ED relationship — r stays near zero in every group. But it does predict absolute ED wait levels:

| Ownership | n | Avg ED Wait | Avg Imaging | r | Avg Stars |
|---|---|---|---|---|---|
| Non-Profit | 2,303 | 221 min | 6.00% | -0.010 | 3.18 |
| Government | 720 | 201 min | 6.44% | -0.019 | 2.81 |
| For-Profit | 565 | 204 min | 5.75% | -0.037 | 2.64 |

Non-Profits have the highest star ratings but the slowest EDs. Star ratings measure clinical quality. ED throughput measures operational speed. They are not the same thing, and Non-Profits appear to optimize for the former.

### Q3: Can a hospital be compliant but still unsafe?

Yes. Among the 30 hospitals with the most "Worse" complication ratings, process-of-care scores range from 42 to 84. One hospital had 5 "Worse" ratings with a compliance score of 83.55 — following every protocol but still producing bad outcomes. Checklist compliance alone does not predict clinical safety.

## Further Analysis (`standalone_analysis/analysis.sql`)

**Ownership vs quality ratings.** Government hospitals are not a monolith: VA hospitals average 4.2 stars with 77% high performers, while local/state government hospitals sit at 2.7–2.8. Lumping them together hides a massive gap. Non-Profits are consistently strong across all subtypes. For-Profit averages are the lowest (48% low performers), except physician-owned hospitals which outperform at 3.32.

**Reporting completeness vs star ratings.** Do hospitals that report on more measures also rate higher? Pearson r = -0.041 (R² = 0.0017). Transparency and quality are statistically independent in this data.

**Infections vs complications vs process compliance.** Infections and complications are both adverse outcomes but come from different measurement systems. Quadrant analysis (median split on both dimensions) identifies hospitals that follow every checklist but still have elevated complication rates. They exist, and the group is not small.

**UPMC Presbyterian Shadyside deep dive.** Benchmarked against PA non-profit acute care peers across all four fact domains. Gap-to-5-star analysis compares UPMC's four weakest measures (PSI_03, PSI_90, OP-10, HAI_1_SIR) against the 12 five-star hospitals in the peer group. Two actionable problems: pressure ulcer protocols and CT contrast ordering.

## Data Quality Patterns

- **Rural data suppression is systemic.** Critical Access hospitals (≤25 beds) rarely generate enough volume to report. MT, SD, WY show 65–80% unusable scores vs 15–30% for NJ, CT, RI. This is baked into the CMS data structure — not a pipeline issue.
- **58% of timely_care scores are "Not Available"** — the highest missing rate of any table. ED measures are the cleanest subset within it.
- **Only 11.9% of hospitals have usable scores across all 6 HAI infection measures.** Most hospitals are missing at least one.
- **47% of hospitals have no star rating at all.** Unrated hospitals skew toward Critical Access and specialty types.
- **US territories** (PR, GU, VI, AS, MP) run 87–100% unusable. Included in `dim_hospital` but excluded from analytical views.

## Bugs Found and Fixed

Building the pipeline turned up several data-handling bugs that would have produced wrong numbers downstream.

**Footnote matching used exact strings instead of arrays.** Boolean flags on `hospital_general` checked `footnote = '5'`, which fails when CMS writes `'5, 23'`. Switched to array overlap (`&&` + `string_to_array`), matching the pattern already used in the fact tables.

**Score exclusion priority was backwards.** The CASE statement checked for NULL scores first, so rows with a NULL score AND a meaningful footnote code (like '3, 13') got labeled 'No Score' instead of the correct footnote-based tier. Reordered so footnote codes are evaluated first.

**Zero-infection hospitals weren't flagged.** Footnote code 8 (zero infections) was missing from the `is_score_usable` blacklist. All code 8 rows have NULL scores — CMS doesn't compute a ratio when there's nothing to ratio — but the pipeline was treating them as if a score might exist.

**`birthing_friendly_designation` conflated "unknown" with "no."** Mapped 'Y' to TRUE, everything else (including NULL) to FALSE. Fixed to use NULL for unknowns.

## Limitations

- **Survivorship bias.** The Q1/Q2 sample (3,587 hospitals) skews toward larger urban facilities. Smaller hospitals drop out because they don't report on both domains. Non-Profit representation rises from 54% to 64% in the joined sample.
- **Rural suppression.** States with high Critical Access density show 2–5x higher data suppression rates. Rural hospital quality is structurally underrepresented in CMS data.
- **Single snapshot.** All data comes from one CMS release cycle (January 2026). No trend analysis is possible.
- **Process-of-care averaging.** Q3 averages compliance scores across conditions with different baselines. Filtered to comparable scales (Colonoscopy, Vaccination, Surgical Care) but some variation remains.

## How to Run

**Requires:** PostgreSQL 14+

1. Export each sheet from `data.cms.gov` as CSV into `/tmp/`
2. Run in order:
   ```sql
   -- Bronze
   \i bronze/bronze_setup.sql
   \i bronze/bronze_proc_load.sql
   CALL bronze_schema.load_bronze();

   -- Silver
   \i silver/silver_setup.sql
   \i silver/silver_proc_load.sql
   CALL silver_schema.load_silver();

   -- Gold
   \i gold/gold_setup.sql
   \i gold/gold_views.sql
   ```
3. Validate with `bronze/bronze_checks.sql`, `silver/silver_checks.sql`, `gold/gold_checks.sql`.

> **Note:** A single bootstrap script (`run_all.sh`) that handles CSV export and sequential execution would improve reproducibility.

## License

This project is licensed under the [MIT License](LICENSE).
