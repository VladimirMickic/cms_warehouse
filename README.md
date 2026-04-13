# Hospital Quality Analysis — CMS Hospital Compare

A data pipeline that takes raw CMS hospital quality data through a bronze-silver-gold medallion architecture in PostgreSQL, then uses the clean output to investigate three business questions about hospital performance.

I started with two intuitive hypotheses: that hospitals over-ordering imaging would also have longer ED waits (both feel like symptoms of the same problem), and that ownership type would predict which hospitals show that pattern.

Both were wrong. Imaging overuse and ED delays are statistically independent (r = -0.020). Ownership tells you where your ED baseline sits — Non-Profits run 20 minutes slower than Government hospitals — but has nothing to do with whether those two problems co-occur. The real finding turned out to be more useful than what I expected to find.

## Architecture

```
hospital.xlsx → CSV sheets → Bronze (raw TEXT) → Silver (typed + cleaned) → Gold (analytical views)
```

**Bronze** — Raw CMS data loaded as-is. All TEXT, no transforms. Exists so there's always something to diff against when silver produces weird numbers.

**Silver** — Proper types, standardized categories, derived flags. Ownership consolidated from 12 values to 4 groups. Footnote codes parsed into `is_score_usable` and `score_exclusion_reason` so downstream queries only touch trustworthy data.

**Gold** — One base view (`dim_hospital` with region mapping) and 3 analytical views answering the business questions. No unified fact table — each view joins only the silver tables it needs.

## File Structure

```
bronze/
  bronze_setup.sql          -- 5 raw tables, all TEXT columns
  bronze_proc_load.sql      -- COPY FROM CSV with per-table exception handling
  bronze_checks.sql         -- Profiling: nulls, dupes, value distributions

silver/
  silver_setup.sql          -- Typed tables with derived columns
  silver_proc_load.sql      -- Type casts, ownership mapping, footnote parsing, score splitting
  silver_checks.sql         -- 10 pass/fail validation checks
  silver_validation.sql     -- Bronze vs silver spot-checks
  silver_exploration.sql    -- Queries that informed gold layer design

gold/
  gold_setup.sql            -- dim_hospital (region mapping + attributes)
  gold_views.sql            -- 3 analytical views (Q1 scatter, Q2 ownership summary, Q3 complications)
  gold_checks.sql           -- Row count validation, correlation queries
  sql_analysis.sql          -- Standalone analyses (ownership vs ratings, reporting completeness)

query_reasoning.md          -- Why each exploration query exists and what it found
```

## Row Counts

| Table | Rows | What it covers |
|---|---|---|
| cms_hospital_general | 5,426 | One row per facility — ratings, ownership, location |
| cms_timely_care | 138,129 | ED waits, vaccination, surgical timing (58% "Not Available") |
| cms_complications | 95,780 | Mortality and complication rates vs national benchmarks |
| cms_outpatient_imaging | 18,500 | CT/MRI ordering efficiency — cleanest table |
| cms_infections | 172,404 | HAI rates: CLABSI, CAUTI, SSI, MRSA, C.diff (36 sub-measures per hospital) |

## Key Transformations

**Ownership:** 12 raw CMS values mapped to 4 groups (Government, Non-Profit, For-Profit, Tribal). Tribal stays separate — ~30 hospitals with a different regulatory framework. Original value kept in `hospital_ownership_details`.

**Score usability:** CMS footnote codes indicate when a score is suppressed, unreliable, or based on too few cases. I parsed these into `is_score_usable` (boolean) and `score_exclusion_reason` with a tiered priority system — footnote codes are evaluated before checking for NULL scores, because multi-code footnotes like '3, 13' need to resolve to the right tier.

**Score splitting (timely_care):** The score column mixes numeric values, categorical text ('very high', 'low'), and 'Not Available' in the same field. Split into `score_numeric`, `score_text`, and `score_available` so each type can be handled correctly.

**compared_to_national:** 8 raw wording variants across complications and infections (e.g., "Better Than the National Rate" vs "Better than National Benchmark") collapsed to 3 values + NULL using ILIKE wildcards.

## Findings

### Q1: Imaging overuse vs ED wait times

3,589 hospitals had both usable imaging and ED scores. Pearson r = -0.020, R-squared = 0.0004. No relationship. A hospital with high CT overuse is no more likely to have a long ED wait. They are separate problems driven by separate causes.

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

### Data quality patterns worth knowing

- Rural data suppression is baked into the CMS data. Critical Access hospitals (25 beds or fewer) rarely have enough volume to report. MT, SD, WY show 65-80% unusable scores vs 15-30% for NJ, CT, RI.
- US territories (PR, GU, VI, AS, MP) run 87-100% unusable. Included in `dim_hospital` but excluded from analysis.
- Only 11.9% of hospitals have usable scores across all 6 infection measures.
- 47% of hospitals have no star rating at all.

## Bugs Found and Fixed

Building the pipeline turned up several data-handling bugs that would have produced wrong numbers downstream.

**Footnote matching used exact strings instead of arrays.** Boolean flags on `hospital_general` checked `footnote = '5'`, which fails when CMS writes `'5, 23'`. Switched to array overlap (`&&` + `string_to_array`), matching the pattern already used in the fact tables.

**Score exclusion priority was backwards.** The CASE statement checked for NULL scores first, so rows with a NULL score AND a meaningful footnote code (like '3, 13') got labeled 'No Score' instead of the correct footnote-based tier. Reordered so footnote codes are evaluated first.

**Zero-infection hospitals weren't flagged.** Footnote code 8 (zero infections) was missing from the `is_score_usable` blacklist. All code 8 rows have NULL scores — CMS doesn't compute a ratio when there's nothing to ratio — but the pipeline was treating them as if a score might exist.

**Q3 view used wrong condition names.** Filtered to 'Colonoscopy', 'Vaccination', and 'Surgical Care' — none of which exist verbatim in the data. Actual values are 'Colonoscopy care' and 'Healthcare Personnel Vaccination'. The view was returning 0 rows.

**`birthing_friendly_designation` conflated "unknown" with "no."** Mapped 'Y' to TRUE, everything else (including NULL) to FALSE. Fixed to use NULL for unknowns. (Documented but not yet applied to the proc.)

## Further Analysis (`sql_analysis.sql`)

While building the pipeline, new questions kept coming up that didn't fit neatly into the three dashboard views. I wrote four standalone analyses in `sql_analysis.sql` to chase those threads.

**Ownership vs quality ratings.** The gold views already showed that ownership predicts ED wait times, but I wanted to know if it also predicts star ratings — and whether "Government" is even a meaningful category when it includes both a 25-bed county hospital and the VA. It does matter: VA hospitals average 4.2 stars with 77% high performers, while local and state government hospitals sit around 2.7-2.8. Lumping them together hides a massive gap. Non-Profits are consistently strong across all subtypes. For-Profit is mostly low performers (48%), except physician-owned hospitals which outperform at 3.32 average.

**Reporting completeness vs star ratings.** Hospitals that report on more quality measures might have better infrastructure, or they might just be bigger. Turns out it barely matters — Pearson r = -0.041, meaning reporting completeness explains about 0.17% of the variation in star ratings. Transparency and quality are independent in this data.

**Infections vs complications vs process compliance.** Infections and complications are both "things that go wrong" but they come from different measurement systems. I wanted to know if they cluster in the same hospitals. Then I flipped it — does following checklists (process-of-care compliance) actually predict fewer complications? Used a quadrant analysis splitting hospitals by median on both dimensions. The interesting group is hospitals that follow every protocol but still have high complication rates. They exist, and there are a lot of them.

**UPMC Presbyterian Shadyside deep dive.** A friend works at this hospital and wanted to know how they stack up. I pulled their scores across all four fact domains and benchmarked against PA non-profit acute care peers. The gap-to-5-star query compares their four weakest measures (PSI_03, PSI_90, OP-10, HAI_1_SIR) against what the 12 five-star hospitals in that peer group actually score. Two problems, one roadmap: pressure ulcer protocols and CT contrast ordering behavior at this specific campus.

## Limitations

- **Survivorship bias.** The Q1/Q2 sample (3,589 hospitals) skews toward larger urban facilities. Smaller hospitals drop out because they don't report on both domains. Non-Profit representation rises from 54% to 64%; Government star ratings drop from 3.13 to 2.81 in the joined sample.
- **Rural suppression.** States with high Critical Access density show 2-5x higher data suppression rates. Rural hospital quality is structurally underrepresented in CMS data.
- **Single snapshot.** All data comes from one CMS release cycle. No trend analysis is possible.
- **Process-of-care averaging.** Q3 averages compliance scores across conditions with different baselines (sepsis 65-92% vs vaccination ~74.5%). Documented, not hidden.

## How to Run

**Requires:** PostgreSQL 14+

1. Export each sheet from `hospital.xlsx` as CSV into `/tmp/`
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
