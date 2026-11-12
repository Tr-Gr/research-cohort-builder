# Databricks notebook source
# MAGIC %md
# MAGIC # 02 QA checks
# MAGIC
# MAGIC Automated checks on the cohort built in notebook 01. Every check writes one row to `<prefix>qa_results_<study_id>` with `PASS`, `WARN` or `FAIL`. A `FAIL` stops the job before the analytic file is built (see the gate cell).
# MAGIC
# MAGIC Also writes `<prefix>qa_distribution_<study_id>` (demographic strata with small-cell flags) for the QA dashboard.
# MAGIC
# MAGIC Small-cell rule: any stratum with fewer than 11 persons is flagged and displayed as `<11`.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inputs for the checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PHI (mrn, date_of_birth) is re-read from the source patients table, so checks are not distorted by masking on the cohort.
# MAGIC CREATE OR REPLACE TEMP VIEW rs_qa_cohort AS
# MAGIC SELECT c.patient_id, p.mrn, p.date_of_birth, c.index_date, c.index_icd10_code, c.qualifying_dx_events, c.age_at_index, c.ndc_drug_exposure_in_window
# MAGIC FROM IDENTIFIER(:table_prefix || 'cohort_' || :study_id) c
# MAGIC JOIN src_patients p ON p.patient_id = c.patient_id;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_qa_funnel_final AS
# MAGIC SELECT n_persons FROM IDENTIFIER(:table_prefix || 'attrition_' || :study_id) WHERE step_order = 5;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_qa_dx_codes AS
# MAGIC SELECT DISTINCT upper(trim(code_value)) AS icd10_prefix FROM study_codes WHERE upper(trim(code_type)) = 'ICD10CM';
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_qa_ndc_codes AS
# MAGIC SELECT DISTINCT regexp_replace(code_value, '[^0-9]', '') AS ndc_11 FROM study_codes WHERE upper(trim(code_type)) = 'NDC';
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_qa_dx_hits AS
# MAGIC SELECT c.icd10_prefix, count(d.icd10_code) AS dx_rows_in_source
# MAGIC FROM rs_qa_dx_codes c
# MAGIC LEFT JOIN src_diagnoses d ON upper(d.icd10_code) LIKE concat(c.icd10_prefix, '%')
# MAGIC GROUP BY c.icd10_prefix;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demographic distributions (feeds the dashboard, with small-cell flags)
# MAGIC
# MAGIC Age bands use age at index. Race, ethnicity, gender and payer come from the patients table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW rs_qa_person AS
# MAGIC SELECT c.patient_id, c.age_at_index,
# MAGIC        CASE WHEN c.age_at_index < 30 THEN '18-29' WHEN c.age_at_index < 40 THEN '30-39' WHEN c.age_at_index < 50 THEN '40-49'
# MAGIC             WHEN c.age_at_index < 60 THEN '50-59' WHEN c.age_at_index < 70 THEN '60-69' WHEN c.age_at_index < 80 THEN '70-79'
# MAGIC             ELSE '80+' END AS age_band,
# MAGIC        p.gender, p.race, p.ethnicity, p.insurance_payer_category
# MAGIC FROM rs_qa_cohort c JOIN src_patients p ON p.patient_id = c.patient_id;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_qa_distribution AS
# MAGIC SELECT 'age_band' AS dimension, age_band AS level, count(*) AS n_persons FROM rs_qa_person GROUP BY age_band
# MAGIC UNION ALL SELECT 'gender', coalesce(gender, 'Unknown'), count(*) FROM rs_qa_person GROUP BY coalesce(gender, 'Unknown')
# MAGIC UNION ALL SELECT 'race', coalesce(race, 'Unknown'), count(*) FROM rs_qa_person GROUP BY coalesce(race, 'Unknown')
# MAGIC UNION ALL SELECT 'ethnicity', coalesce(ethnicity, 'Unknown'), count(*) FROM rs_qa_person GROUP BY coalesce(ethnicity, 'Unknown')
# MAGIC UNION ALL SELECT 'insurance_payer', coalesce(insurance_payer_category, 'Unknown'), count(*) FROM rs_qa_person GROUP BY coalesce(insurance_payer_category, 'Unknown')
# MAGIC UNION ALL SELECT 'payer_x_race_x_age_band', concat(coalesce(insurance_payer_category, 'Unknown'), ' | ', coalesce(race, 'Unknown'), ' | ', age_band), count(*)
# MAGIC FROM rs_qa_person GROUP BY concat(coalesce(insurance_payer_category, 'Unknown'), ' | ', coalesce(race, 'Unknown'), ' | ', age_band);
# MAGIC
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:table_prefix || 'qa_distribution_' || :study_id)
# MAGIC COMMENT 'Demographic strata for the cohort with small-cell flags (n < 11). Display n_persons_display, never n_persons, in dashboards and exports.'
# MAGIC AS
# MAGIC SELECT dimension, level, n_persons,
# MAGIC        CASE WHEN n_persons < 11 THEN '<11' ELSE cast(n_persons AS STRING) END AS n_persons_display,
# MAGIC        n_persons < 11 AS small_cell_flag,
# MAGIC        round(100.0 * n_persons / sum(n_persons) OVER (PARTITION BY dimension), 1) AS pct_within_dimension,
# MAGIC        :study_id AS study_id
# MAGIC FROM rs_qa_distribution;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Automated checks
# MAGIC
# MAGIC Each row is one check. The `status` expression is the rule. Add checks by adding a `UNION ALL` row; keep the rules readable so reviewers can follow them.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW rs_qa_checks AS
# MAGIC WITH base AS (
# MAGIC   SELECT count(*) AS n, count(DISTINCT patient_id) AS n_pt, count(DISTINCT mrn) AS n_mrn,
# MAGIC          sum(CASE WHEN mrn IS NULL THEN 1 ELSE 0 END) AS null_mrn,
# MAGIC          sum(CASE WHEN date_of_birth IS NULL THEN 1 ELSE 0 END) AS null_dob,
# MAGIC          sum(CASE WHEN index_date IS NULL THEN 1 ELSE 0 END) AS null_index,
# MAGIC          sum(CASE WHEN age_at_index IS NULL THEN 1 ELSE 0 END) AS null_age,
# MAGIC          sum(CASE WHEN index_date NOT BETWEEN DATE(:index_window_start) AND DATE(:index_window_end) THEN 1 ELSE 0 END) AS index_out_window,
# MAGIC          sum(CASE WHEN date_of_birth < DATE '1900-01-01' OR date_of_birth > index_date THEN 1 ELSE 0 END) AS dob_implausible,
# MAGIC          sum(CASE WHEN age_at_index < 18 OR age_at_index > 110 THEN 1 ELSE 0 END) AS age_out_range
# MAGIC   FROM rs_qa_cohort
# MAGIC ),
# MAGIC dx_cov AS (SELECT count(*) AS codes_total, sum(CASE WHEN dx_rows_in_source = 0 THEN 1 ELSE 0 END) AS codes_no_hits,
# MAGIC                   array_join(collect_list(CASE WHEN dx_rows_in_source = 0 THEN icd10_prefix END), ', ') AS no_hit_list
# MAGIC            FROM rs_qa_dx_hits),
# MAGIC ndc_cov AS (
# MAGIC   SELECT (SELECT count(*) FROM rs_qa_ndc_codes) AS ndc_total,
# MAGIC          (SELECT count(*) FROM rs_qa_ndc_codes n WHERE EXISTS (SELECT 1 FROM src_ndc_crosswalk w WHERE w.ndc_11 = n.ndc_11)) AS ndc_matched
# MAGIC ),
# MAGIC mrn_rec AS (
# MAGIC   SELECT count(*) AS listed,
# MAGIC          coalesce(sum(CASE WHEN l.mrn LIKE '*%' THEN 1 ELSE 0 END), 0) AS masked,
# MAGIC          sum(CASE WHEN p.patient_id IS NOT NULL THEN 1 ELSE 0 END) AS matched_to_patient,
# MAGIC          sum(CASE WHEN c.patient_id IS NOT NULL THEN 1 ELSE 0 END) AS in_cohort
# MAGIC   FROM mrn_list l
# MAGIC   LEFT JOIN src_patients p ON p.mrn = l.mrn
# MAGIC   LEFT JOIN rs_qa_cohort c ON c.mrn = l.mrn
# MAGIC ),
# MAGIC small AS (SELECT count(*) AS small_strata FROM rs_qa_distribution WHERE n_persons < 11),
# MAGIC dup_mrn AS (SELECT count(*) - count(DISTINCT mrn) AS dup FROM rs_qa_cohort)
# MAGIC SELECT * FROM (
# MAGIC SELECT 'R01' AS check_id, 'Row counts' AS check_group, 'Cohort has at least one person' AS check_name,
# MAGIC        CAST(n AS DOUBLE) AS metric_value, '>= 1' AS rule,
# MAGIC        CASE WHEN n >= 1 THEN 'PASS' ELSE 'FAIL' END AS status, concat(n, ' persons') AS detail FROM base
# MAGIC UNION ALL SELECT 'R02', 'Row counts', 'Cohort rows equal final attrition step',
# MAGIC        CAST(n - (SELECT n_persons FROM rs_qa_funnel_final) AS DOUBLE), '= 0 difference',
# MAGIC        CASE WHEN n = (SELECT n_persons FROM rs_qa_funnel_final) THEN 'PASS' ELSE 'FAIL' END,
# MAGIC        concat('cohort ', n, ' vs funnel ', (SELECT n_persons FROM rs_qa_funnel_final)) FROM base
# MAGIC UNION ALL SELECT 'D01', 'Duplicates', 'No duplicate persons (patient_id)',
# MAGIC        CAST(n - n_pt AS DOUBLE), '0 duplicates',
# MAGIC        CASE WHEN n = n_pt THEN 'PASS' ELSE 'FAIL' END, concat(n - n_pt, ' duplicate rows') FROM base
# MAGIC UNION ALL SELECT 'D02', 'Duplicates', 'No MRN shared by two persons (linkage integrity)',
# MAGIC        CAST(dup AS DOUBLE), '0 duplicates',
# MAGIC        CASE WHEN dup = 0 THEN 'PASS' ELSE 'FAIL' END, concat(dup, ' duplicate MRNs') FROM dup_mrn
# MAGIC UNION ALL SELECT 'N01', 'Null rates', 'Null rate of mrn',
# MAGIC        round(100.0 * null_mrn / n, 2), '< 1% WARN, >= 1% FAIL',
# MAGIC        CASE WHEN null_mrn = 0 THEN 'PASS' WHEN 100.0 * null_mrn / n < 1 THEN 'WARN' ELSE 'FAIL' END, concat(null_mrn, ' null') FROM base
# MAGIC UNION ALL SELECT 'N02', 'Null rates', 'Null rate of date_of_birth',
# MAGIC        round(100.0 * null_dob / n, 2), '< 1% WARN, >= 1% FAIL',
# MAGIC        CASE WHEN null_dob = 0 THEN 'PASS' WHEN 100.0 * null_dob / n < 1 THEN 'WARN' ELSE 'FAIL' END, concat(null_dob, ' null') FROM base
# MAGIC UNION ALL SELECT 'N03', 'Null rates', 'Null rate of index_date',
# MAGIC        round(100.0 * null_index / n, 2), '< 1% WARN, >= 1% FAIL',
# MAGIC        CASE WHEN null_index = 0 THEN 'PASS' WHEN 100.0 * null_index / n < 1 THEN 'WARN' ELSE 'FAIL' END, concat(null_index, ' null') FROM base
# MAGIC UNION ALL SELECT 'N04', 'Null rates', 'Null rate of age_at_index',
# MAGIC        round(100.0 * null_age / n, 2), '< 1% WARN, >= 1% FAIL',
# MAGIC        CASE WHEN null_age = 0 THEN 'PASS' WHEN 100.0 * null_age / n < 1 THEN 'WARN' ELSE 'FAIL' END, concat(null_age, ' null') FROM base
# MAGIC UNION ALL SELECT 'T01', 'Date sanity', 'Index date falls inside the index window',
# MAGIC        CAST(index_out_window AS DOUBLE), '0 outside window',
# MAGIC        CASE WHEN index_out_window = 0 THEN 'PASS' ELSE 'FAIL' END, concat(index_out_window, ' outside') FROM base
# MAGIC UNION ALL SELECT 'T02', 'Date sanity', 'Date of birth plausible (after 1900, on or before index)',
# MAGIC        CAST(dob_implausible AS DOUBLE), '0 implausible',
# MAGIC        CASE WHEN dob_implausible = 0 THEN 'PASS' ELSE 'FAIL' END, concat(dob_implausible, ' implausible') FROM base
# MAGIC UNION ALL SELECT 'T03', 'Date sanity', 'Age at index between 18 and 110',
# MAGIC        CAST(age_out_range AS DOUBLE), '0 out of range',
# MAGIC        CASE WHEN age_out_range = 0 THEN 'PASS' ELSE 'FAIL' END, concat(age_out_range, ' out of range') FROM base
# MAGIC UNION ALL SELECT 'C01', 'Code coverage', 'Every listed ICD-10 code has at least one hit in source diagnoses',
# MAGIC        CAST(codes_no_hits AS DOUBLE), '0 codes with no hits',
# MAGIC        CASE WHEN codes_no_hits = 0 THEN 'PASS' ELSE 'WARN' END,
# MAGIC        concat(codes_no_hits, ' of ', codes_total, ' codes no hits: ', coalesce(nullif(no_hit_list, ''), 'none')) FROM dx_cov
# MAGIC UNION ALL SELECT 'C02', 'Code coverage', 'Every listed NDC maps to the crosswalk',
# MAGIC        CAST(ndc_total - ndc_matched AS DOUBLE), '0 unmatched',
# MAGIC        CASE WHEN ndc_total = ndc_matched THEN 'PASS' ELSE 'WARN' END, concat(ndc_matched, ' of ', ndc_total, ' NDCs matched') FROM ndc_cov
# MAGIC UNION ALL SELECT 'M01', 'MRN list', 'Listed MRNs found in the patients table',
# MAGIC        CAST(listed - matched_to_patient AS DOUBLE), '0 unmatched',
# MAGIC        CASE WHEN listed = matched_to_patient THEN 'PASS' ELSE 'WARN' END,
# MAGIC        CASE WHEN listed = 0 THEN 'no MRN list provided'
# MAGIC             WHEN masked > 0 THEN 'MRN list reads masked for the run identity (not in approved_group), so it cannot be reconciled'
# MAGIC             ELSE concat(matched_to_patient, ' of ', listed, ' matched') END FROM mrn_rec
# MAGIC UNION ALL SELECT 'M02', 'MRN list', 'Listed MRNs that are in the cohort (reconciliation, informational)',
# MAGIC        CAST(in_cohort AS DOUBLE), 'informational',
# MAGIC        CASE WHEN listed = 0 OR in_cohort > 0 THEN 'PASS' ELSE 'WARN' END,
# MAGIC        CASE WHEN listed = 0 THEN 'no MRN list provided'
# MAGIC             WHEN masked > 0 THEN 'MRN list reads masked for the run identity; see M01'
# MAGIC             ELSE concat(in_cohort, ' of ', listed, ' listed MRNs are in the cohort') END FROM mrn_rec
# MAGIC UNION ALL SELECT 'R03', 'Run identity', 'Run identity is in the approved group',
# MAGIC        CAST(CASE WHEN :approved_group <> '' AND is_account_group_member(:approved_group) THEN 1 ELSE 0 END AS DOUBLE), 'approved = 1',
# MAGIC        CASE WHEN :approved_group <> '' AND is_account_group_member(:approved_group) THEN 'PASS' ELSE 'WARN' END,
# MAGIC        CASE WHEN :approved_group = '' THEN 'no approved_group configured'
# MAGIC             WHEN is_account_group_member(:approved_group) THEN 'approved identity'
# MAGIC             ELSE 'run identity is not in the approved group: it sees masked values in tagged tables' END FROM base
# MAGIC UNION ALL SELECT 'S01', 'Small cells', 'No demographic stratum below 11 persons (flagged for suppression)',
# MAGIC        CAST(small_strata AS DOUBLE), '0 strata under 11',
# MAGIC        CASE WHEN small_strata = 0 THEN 'PASS' ELSE 'WARN' END, concat(small_strata, ' strata flagged <11, shown as <11 on the dashboard') FROM small
# MAGIC ) t;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write QA results

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:table_prefix || 'qa_results_' || :study_id)
# MAGIC COMMENT 'Automated QA checks for the study cohort (notebook 02) and analytic file (notebook 03). status is PASS, WARN or FAIL.'
# MAGIC AS
# MAGIC SELECT current_timestamp() AS run_ts, :study_id AS study_id, 'cohort' AS stage, check_id, check_group, check_name,
# MAGIC        metric_value, rule, status, detail
# MAGIC FROM rs_qa_checks;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gate
# MAGIC
# MAGIC Stops the job if any cohort check failed. The analytic file is not built when this cell raises.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT status, count(*) AS n_checks FROM IDENTIFIER(:table_prefix || 'qa_results_' || :study_id) GROUP BY status ORDER BY status;

# COMMAND ----------

_fails = spark.sql(f"SELECT count(*) AS n FROM {T('qa_results_' + STUDY)} WHERE status = 'FAIL'").first()["n"]
assert _fails == 0, f"QA gate: {_fails} check(s) FAILED; see {T('qa_results_' + STUDY)}"
print("QA gate passed: no FAIL checks")
