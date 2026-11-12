# Databricks notebook source
# MAGIC %md
# MAGIC # 03 Analytic file
# MAGIC
# MAGIC Summarizes the cohort into one row per person for the research project team.
# MAGIC
# MAGIC - `<prefix>af_<study_id>`: person-level file with PHI columns (`mrn`, `date_of_birth`), tagged `class.*` so ABAC masks them outside the approved group.
# MAGIC - `<prefix>af_<study_id>_deid`: view with no direct identifiers, year-month dates, age bands and a keyed `linkage_token` (SHA-256 of the MRN with a secret key). This is what gets exported and shared.
# MAGIC - Writes CSV and Parquet to `/Volumes/<catalog>/<schema>/<export_volume>/exports/<study_id>/`.
# MAGIC
# MAGIC The baseline comorbidity and medication flags below are examples for a type 2 diabetes study. Edit them per study.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1. Inputs
# MAGIC
# MAGIC The cohort's PHI is re-read from the source patients table so the analytic file is never built from masked values.
# MAGIC
# MAGIC The linkage token key comes from a secret scope. Without one, a demo-only key is used and the run prints a warning: never use that for real data. In production, use keyed tokens from your tokenization service.

# COMMAND ----------

if P["linkage_secret_scope"]:
    _key = dbutils.secrets.get(P["linkage_secret_scope"], P["linkage_secret_key"])
else:
    _key = f"demo-only-key-{CAT}.{SCH}"
    print("WARNING: linkage_secret_scope is empty, so linkage_token uses a demo-only key. Do not use for real data.")
spark.sql("CREATE OR REPLACE TEMP VIEW rs_af_key AS SELECT :k AS k", args={"k": _key})

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW rs_af_cohort AS
# MAGIC SELECT c.patient_id, p.mrn, p.date_of_birth, c.index_date, c.index_icd10_code, c.age_at_index, c.ndc_drug_exposure_in_window
# MAGIC FROM IDENTIFIER(:table_prefix || 'cohort_' || :study_id) c
# MAGIC JOIN src_patients p ON p.patient_id = c.patient_id;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_af_dx AS
# MAGIC SELECT d.patient_id, e.encounter_date, upper(d.icd10_code) AS icd10_code
# MAGIC FROM src_diagnoses d
# MAGIC JOIN src_encounters e ON e.encounter_id = d.encounter_id AND e.patient_id = d.patient_id;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_af_data_end AS
# MAGIC SELECT max(encounter_date) AS data_max_date FROM src_encounters;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2. Baseline comorbidities and medications
# MAGIC
# MAGIC Baseline = the `baseline_lookback_days` before the index date, including the index date. Comorbidity flags: HTN `I10`, CKD `N18`, hyperlipidemia `E78`, obesity `E66`, heart failure `I50`. Medication flags: metformin, insulin glargine, atorvastatin.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW rs_af_comorb AS
# MAGIC SELECT c.patient_id,
# MAGIC        max(CASE WHEN x.icd10_code LIKE 'I10%' THEN 1 ELSE 0 END) AS htn_baseline,
# MAGIC        max(CASE WHEN x.icd10_code LIKE 'N18%' THEN 1 ELSE 0 END) AS ckd_baseline,
# MAGIC        max(CASE WHEN x.icd10_code LIKE 'E78%' THEN 1 ELSE 0 END) AS hld_baseline,
# MAGIC        max(CASE WHEN x.icd10_code LIKE 'E66%' THEN 1 ELSE 0 END) AS obesity_baseline,
# MAGIC        max(CASE WHEN x.icd10_code LIKE 'I50%' THEN 1 ELSE 0 END) AS chf_baseline
# MAGIC FROM rs_af_cohort c
# MAGIC LEFT JOIN rs_af_dx x ON x.patient_id = c.patient_id
# MAGIC   AND x.encounter_date BETWEEN date_sub(c.index_date, CAST(:baseline_lookback_days AS INT)) AND c.index_date
# MAGIC GROUP BY c.patient_id;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_af_meds AS
# MAGIC SELECT c.patient_id,
# MAGIC        max(CASE WHEN lower(m.medication_name) = 'metformin' THEN 1 ELSE 0 END) AS metformin_baseline,
# MAGIC        max(CASE WHEN lower(m.medication_name) = 'insulin glargine' THEN 1 ELSE 0 END) AS insulin_glargine_baseline,
# MAGIC        max(CASE WHEN lower(m.medication_name) = 'atorvastatin' THEN 1 ELSE 0 END) AS statin_baseline
# MAGIC FROM rs_af_cohort c
# MAGIC LEFT JOIN src_medications m ON m.patient_id = c.patient_id
# MAGIC   AND CAST(m.ordered_datetime AS DATE) BETWEEN date_sub(c.index_date, CAST(:baseline_lookback_days AS INT)) AND c.index_date
# MAGIC GROUP BY c.patient_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3. HbA1c (LOINC 4548-4), baseline and follow-up
# MAGIC
# MAGIC - **Baseline:** most recent result in the `a1c_baseline_window_days` before the index date.
# MAGIC - **Follow-up:** earliest result 6 to 12 months after the index date.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW rs_af_a1c_base AS
# MAGIC SELECT patient_id, a1c_baseline, a1c_baseline_date FROM (
# MAGIC   SELECT c.patient_id, l.result_value AS a1c_baseline, CAST(l.resulted_datetime AS DATE) AS a1c_baseline_date,
# MAGIC          row_number() OVER (PARTITION BY c.patient_id ORDER BY l.resulted_datetime DESC) AS rn
# MAGIC   FROM rs_af_cohort c
# MAGIC   JOIN src_lab_results l ON l.patient_id = c.patient_id AND l.loinc_code = '4548-4'
# MAGIC    AND CAST(l.resulted_datetime AS DATE) BETWEEN date_sub(c.index_date, CAST(:a1c_baseline_window_days AS INT)) AND c.index_date
# MAGIC ) WHERE rn = 1;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_af_a1c_fu AS
# MAGIC SELECT patient_id, a1c_followup, a1c_followup_date FROM (
# MAGIC   SELECT c.patient_id, l.result_value AS a1c_followup, CAST(l.resulted_datetime AS DATE) AS a1c_followup_date,
# MAGIC          row_number() OVER (PARTITION BY c.patient_id ORDER BY l.resulted_datetime ASC) AS rn
# MAGIC   FROM rs_af_cohort c
# MAGIC   JOIN src_lab_results l ON l.patient_id = c.patient_id AND l.loinc_code = '4548-4'
# MAGIC    AND CAST(l.resulted_datetime AS DATE) BETWEEN date_add(c.index_date, 180) AND date_add(c.index_date, 365)
# MAGIC ) WHERE rn = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4. Utilization in the follow-up window and follow-up completeness
# MAGIC
# MAGIC Counts encounters after the index date, within `followup_days`. Observation stays are not counted. `followup_complete_12m` is false when the data end date (latest encounter in the source) falls before index plus `followup_days`, so late index dates are censored.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW rs_af_util AS
# MAGIC SELECT c.patient_id,
# MAGIC        sum(CASE WHEN e.encounter_type = 'emergency' THEN 1 ELSE 0 END) AS ed_visits_12m,
# MAGIC        sum(CASE WHEN e.encounter_type = 'inpatient' THEN 1 ELSE 0 END) AS inpatient_admissions_12m,
# MAGIC        sum(CASE WHEN e.encounter_type = 'outpatient' THEN 1 ELSE 0 END) AS outpatient_visits_12m
# MAGIC FROM rs_af_cohort c
# MAGIC LEFT JOIN src_encounters e ON e.patient_id = c.patient_id
# MAGIC   AND e.encounter_date > c.index_date AND e.encounter_date <= date_add(c.index_date, CAST(:followup_days AS INT))
# MAGIC GROUP BY c.patient_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5. Write the person-level analytic file
# MAGIC
# MAGIC `linkage_token` is computed here, in the base table, from the unmasked MRN. Computing it in the de-identified view would hash a masked value for readers outside the approved group.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:table_prefix || 'af_' || :study_id)
# MAGIC COMMENT 'Person-level analytic file, one row per person. PHI columns mrn and date_of_birth are tagged class.* and masked by ABAC outside the approved group. Share or export only the _deid view.'
# MAGIC AS
# MAGIC SELECT c.patient_id,
# MAGIC        c.mrn,
# MAGIC        c.date_of_birth,
# MAGIC        :study_id AS study_id,
# MAGIC        c.index_date,
# MAGIC        c.index_icd10_code,
# MAGIC        c.age_at_index,
# MAGIC        CASE WHEN c.age_at_index < 30 THEN '18-29' WHEN c.age_at_index < 40 THEN '30-39' WHEN c.age_at_index < 50 THEN '40-49'
# MAGIC             WHEN c.age_at_index < 60 THEN '50-59' WHEN c.age_at_index < 70 THEN '60-69' WHEN c.age_at_index < 80 THEN '70-79'
# MAGIC             ELSE '80+' END AS age_band,
# MAGIC        p.gender, p.race, p.ethnicity, p.insurance_payer_category,
# MAGIC        substr(p.zip_code, 1, 3) AS zip3,
# MAGIC        cm.htn_baseline, cm.ckd_baseline, cm.hld_baseline, cm.obesity_baseline, cm.chf_baseline,
# MAGIC        md.metformin_baseline, md.insulin_glargine_baseline, md.statin_baseline,
# MAGIC        ab.a1c_baseline, ab.a1c_baseline_date,
# MAGIC        af.a1c_followup, af.a1c_followup_date,
# MAGIC        u.ed_visits_12m, u.inpatient_admissions_12m, u.outpatient_visits_12m,
# MAGIC        least(CAST(:followup_days AS INT), datediff(dm.data_max_date, c.index_date)) AS followup_days_observable,
# MAGIC        datediff(dm.data_max_date, c.index_date) >= CAST(:followup_days AS INT) AS followup_complete_12m,
# MAGIC        c.ndc_drug_exposure_in_window,
# MAGIC        substr(sha2(concat(k.k, '|', upper(trim(c.mrn))), 256), 1, 32) AS linkage_token
# MAGIC FROM rs_af_cohort c
# MAGIC JOIN src_patients p ON p.patient_id = c.patient_id
# MAGIC LEFT JOIN rs_af_comorb cm ON cm.patient_id = c.patient_id
# MAGIC LEFT JOIN rs_af_meds md ON md.patient_id = c.patient_id
# MAGIC LEFT JOIN rs_af_a1c_base ab ON ab.patient_id = c.patient_id
# MAGIC LEFT JOIN rs_af_a1c_fu af ON af.patient_id = c.patient_id
# MAGIC LEFT JOIN rs_af_util u ON u.patient_id = c.patient_id
# MAGIC CROSS JOIN rs_af_data_end dm
# MAGIC CROSS JOIN rs_af_key k;
# MAGIC
# MAGIC ALTER TABLE IDENTIFIER(:table_prefix || 'af_' || :study_id) ALTER COLUMN mrn SET TAGS ('class.medical_record_number' = '');
# MAGIC ALTER TABLE IDENTIFIER(:table_prefix || 'af_' || :study_id) ALTER COLUMN date_of_birth SET TAGS ('class.date_of_birth' = '');
# MAGIC ALTER TABLE IDENTIFIER(:table_prefix || 'af_' || :study_id) ALTER COLUMN race SET TAGS ('class.ethnicity' = '');
# MAGIC ALTER TABLE IDENTIFIER(:table_prefix || 'af_' || :study_id) ALTER COLUMN ethnicity SET TAGS ('class.ethnicity' = '');
# MAGIC ALTER TABLE IDENTIFIER(:table_prefix || 'af_' || :study_id) ALTER COLUMN zip3 SET TAGS ('class.location' = '');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6. De-identified view (the shareable file)
# MAGIC
# MAGIC Drops `patient_id`, `mrn`, `date_of_birth`, `zip3`, exact dates and the raw age. Keeps the linkage token, age band, year-month dates and the analytic variables. Review this column list with your privacy office for each study.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW IDENTIFIER(:table_prefix || 'af_' || :study_id || '_deid')
# MAGIC COMMENT 'De-identified analytic file for export and sharing. No patient_id, MRN, DOB, ZIP or exact dates. linkage_token is a SHA-256 hash of the MRN with a secret key.'
# MAGIC AS
# MAGIC SELECT study_id,
# MAGIC        linkage_token,
# MAGIC        date_format(index_date, 'yyyy-MM') AS index_year_month,
# MAGIC        age_band,
# MAGIC        gender, race, ethnicity, insurance_payer_category,
# MAGIC        htn_baseline, ckd_baseline, hld_baseline, obesity_baseline, chf_baseline,
# MAGIC        metformin_baseline, insulin_glargine_baseline, statin_baseline,
# MAGIC        a1c_baseline, date_format(a1c_baseline_date, 'yyyy-MM') AS a1c_baseline_year_month,
# MAGIC        a1c_followup, date_format(a1c_followup_date, 'yyyy-MM') AS a1c_followup_year_month,
# MAGIC        ed_visits_12m, inpatient_admissions_12m, outpatient_visits_12m,
# MAGIC        followup_days_observable, followup_complete_12m,
# MAGIC        ndc_drug_exposure_in_window
# MAGIC FROM IDENTIFIER(:table_prefix || 'af_' || :study_id);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6b. Share-ready copy
# MAGIC
# MAGIC OpenSharing does not accept a view whose base table is governed by an ABAC policy, so the share carries an untagged, materialized copy of the de-identified file. The governed `_deid` view stays the source for the CSV and Parquet export.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:table_prefix || 'af_' || :study_id || '_deid_share')
# MAGIC COMMENT 'Materialized copy of the de-identified analytic file for OpenSharing (no ABAC-tagged columns).'
# MAGIC AS SELECT * FROM IDENTIFIER(:table_prefix || 'af_' || :study_id || '_deid');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7. Analytic-stage QA (appended to `<prefix>qa_results_<study_id>`)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW rs_af_qa AS
# MAGIC WITH af AS (SELECT * FROM IDENTIFIER(:table_prefix || 'af_' || :study_id)),
# MAGIC base AS (SELECT count(*) AS n, count(DISTINCT patient_id) AS n_pt,
# MAGIC                 sum(CASE WHEN a1c_baseline IS NOT NULL THEN 1 ELSE 0 END) AS a1c_base_n,
# MAGIC                 sum(CASE WHEN a1c_followup IS NOT NULL THEN 1 ELSE 0 END) AS a1c_fu_n,
# MAGIC                 sum(CASE WHEN a1c_baseline NOT BETWEEN 3 AND 15 OR a1c_followup NOT BETWEEN 3 AND 15 THEN 1 ELSE 0 END) AS a1c_implausible,
# MAGIC                 sum(CASE WHEN followup_complete_12m THEN 1 ELSE 0 END) AS fu_complete_n
# MAGIC          FROM af),
# MAGIC cohort_n AS (SELECT count(*) AS n FROM IDENTIFIER(:table_prefix || 'cohort_' || :study_id)),
# MAGIC tagged AS (
# MAGIC   SELECT count(*) AS n_tagged FROM IDENTIFIER(:catalog || '.information_schema.column_tags')
# MAGIC   WHERE schema_name = :schema AND table_name = :table_prefix || 'af_' || :study_id
# MAGIC     AND tag_name IN ('class.medical_record_number', 'class.date_of_birth'))
# MAGIC SELECT * FROM (
# MAGIC SELECT 'A01' AS check_id, 'Row counts' AS check_group, 'Analytic file rows equal cohort rows' AS check_name,
# MAGIC        CAST(base.n - cohort_n.n AS DOUBLE) AS metric_value, '0 difference' AS rule,
# MAGIC        CASE WHEN base.n = cohort_n.n THEN 'PASS' ELSE 'FAIL' END AS status,
# MAGIC        concat(base.n, ' vs ', cohort_n.n) AS detail FROM base CROSS JOIN cohort_n
# MAGIC UNION ALL SELECT 'A02', 'Duplicates', 'One row per person in analytic file', CAST(n - n_pt AS DOUBLE), '0 duplicates',
# MAGIC        CASE WHEN n = n_pt THEN 'PASS' ELSE 'FAIL' END, concat(n - n_pt, ' duplicates') FROM base
# MAGIC UNION ALL SELECT 'A03', 'Null rates', 'HbA1c baseline coverage >= 50%', round(100.0 * a1c_base_n / n, 1), '>= 50%',
# MAGIC        CASE WHEN a1c_base_n >= 0.5 * n THEN 'PASS' ELSE 'WARN' END, concat(a1c_base_n, ' of ', n, ' have a baseline A1c') FROM base
# MAGIC UNION ALL SELECT 'A04', 'Value ranges', 'HbA1c values plausible (3 to 15 percent)', CAST(a1c_implausible AS DOUBLE), '0 implausible',
# MAGIC        CASE WHEN a1c_implausible = 0 THEN 'PASS' ELSE 'FAIL' END, concat(a1c_implausible, ' implausible') FROM base
# MAGIC UNION ALL SELECT 'A05', 'Follow-up', 'Follow-up window complete for all persons', round(100.0 * fu_complete_n / n, 1), '100%',
# MAGIC        CASE WHEN fu_complete_n = n THEN 'PASS' ELSE 'WARN' END,
# MAGIC        concat(n - fu_complete_n, ' persons censored: index date too close to the data end date')
# MAGIC        FROM base
# MAGIC UNION ALL SELECT 'A06', 'PHI controls', 'PHI columns carry class.* tags (ABAC masks them)', CAST(n_tagged AS DOUBLE), '2 tags',
# MAGIC        CASE WHEN n_tagged >= 2 THEN 'PASS' ELSE 'FAIL' END, concat(n_tagged, ' PHI column tags') FROM tagged
# MAGIC UNION ALL SELECT 'A07', 'PHI controls', 'De-identified view exposes no mrn, date_of_birth, patient_id or ZIP',
# MAGIC        CAST(count(*) AS DOUBLE), '0 columns',
# MAGIC        CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END, 'checked against information_schema.columns'
# MAGIC        FROM IDENTIFIER(:catalog || '.information_schema.columns')
# MAGIC        WHERE table_schema = :schema AND table_name = :table_prefix || 'af_' || :study_id || '_deid'
# MAGIC          AND lower(column_name) IN ('mrn', 'date_of_birth', 'patient_id', 'zip_code', 'zip3')
# MAGIC ) t;
# MAGIC
# MAGIC INSERT INTO IDENTIFIER(:table_prefix || 'qa_results_' || :study_id)
# MAGIC SELECT current_timestamp(), :study_id, 'analytic', check_id, check_group, check_name, metric_value, rule, status, detail
# MAGIC FROM rs_af_qa;

# COMMAND ----------

_fails = spark.sql(f"SELECT count(*) AS n FROM {T('qa_results_' + STUDY)} WHERE stage = 'analytic' AND status = 'FAIL'").first()["n"]
assert _fails == 0, f"Analytic QA gate: {_fails} check(s) FAILED; nothing was exported"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8. Export the de-identified file (CSV and Parquet)
# MAGIC
# MAGIC Exports only the `_deid` view. The volume is governed by Unity Catalog and reads and writes are in the audit log.

# COMMAND ----------

import os
_out = f"/Volumes/{CAT}/{SCH}/{P['export_volume']}/exports/{STUDY}"
os.makedirs(_out, exist_ok=True)
_pdf = spark.table(T(f"af_{STUDY}_deid")).toPandas()
assert not {"mrn", "date_of_birth", "patient_id"} & set(_pdf.columns), "PHI column in export"
_csv = f"{_out}/{STUDY}_analytic_file.csv"
_pq = f"{_out}/{STUDY}_analytic_file.parquet"
_pdf.to_csv(_csv, index=False)
_pdf.to_parquet(_pq, index=False)
print(f"exported {len(_pdf)} rows and {len(_pdf.columns)} columns to:\n  {_csv}\n  {_pq}")
