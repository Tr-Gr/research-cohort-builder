# Databricks notebook source
# MAGIC %md
# MAGIC # 01 Build study cohort
# MAGIC
# MAGIC Turns an uploaded code list (ICD-10-CM and NDC rows) into a study population with a documented attrition funnel.
# MAGIC
# MAGIC **Inputs** (resolved by `_config`): the code list table, the NDC crosswalk, and the patients, encounters, diagnoses and medications tables.
# MAGIC
# MAGIC **Outputs:** `<prefix>cohort_<study_id>` (one row per person) and `<prefix>attrition_<study_id>` (one row per funnel step).

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1. Parse the code list
# MAGIC
# MAGIC ICD-10 rows become **prefix** matches, so `E11` catches every E11.x code and `E11.9` catches only that code. NDC rows are normalized to 11 digits (non-digits removed) and joined to the crosswalk to get `medication_name`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW rs_dx_codes AS
# MAGIC SELECT DISTINCT upper(trim(code_value)) AS icd10_prefix
# MAGIC FROM study_codes
# MAGIC WHERE upper(trim(code_type)) = 'ICD10CM';
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_ndc_codes AS
# MAGIC SELECT DISTINCT regexp_replace(code_value, '[^0-9]', '') AS ndc_11
# MAGIC FROM study_codes
# MAGIC WHERE upper(trim(code_type)) = 'NDC';
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_ndc_meds AS
# MAGIC SELECT DISTINCT w.medication_name, w.rxnorm_concept_id
# MAGIC FROM rs_ndc_codes n
# MAGIC JOIN src_ndc_crosswalk w ON w.ndc_11 = n.ndc_11;
# MAGIC
# MAGIC SELECT 'icd10_prefixes' AS item, count(*) AS n FROM rs_dx_codes
# MAGIC UNION ALL SELECT 'ndc_codes_in_list', count(*) FROM rs_ndc_codes
# MAGIC UNION ALL SELECT 'ndc_codes_matched_to_crosswalk', count(*) FROM rs_ndc_meds;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2. Match diagnoses and define the index date
# MAGIC
# MAGIC A person qualifies on any diagnosis whose ICD-10 code starts with a listed prefix, on an encounter dated inside the index window. The **index date** is the earliest qualifying encounter date.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW rs_dx_events AS
# MAGIC SELECT d.patient_id, e.encounter_date, upper(d.icd10_code) AS icd10_code
# MAGIC FROM src_diagnoses d
# MAGIC JOIN src_encounters e
# MAGIC   ON e.encounter_id = d.encounter_id AND e.patient_id = d.patient_id;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_index AS
# MAGIC SELECT x.patient_id,
# MAGIC        min(x.encounter_date) AS index_date,
# MAGIC        min_by(x.icd10_code, x.encounter_date) AS index_icd10_code,
# MAGIC        count(*) AS qualifying_dx_events
# MAGIC FROM rs_dx_events x
# MAGIC JOIN rs_dx_codes c ON x.icd10_code LIKE concat(c.icd10_prefix, '%')
# MAGIC WHERE x.encounter_date BETWEEN DATE(:index_window_start) AND DATE(:index_window_end)
# MAGIC GROUP BY x.patient_id;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_ndc_exposure AS
# MAGIC SELECT DISTINCT m.patient_id
# MAGIC FROM src_medications m
# MAGIC JOIN rs_ndc_meds n ON lower(m.medication_name) = lower(n.medication_name)
# MAGIC WHERE CAST(m.ordered_datetime AS DATE) BETWEEN DATE(:index_window_start) AND DATE(:index_window_end);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3. Apply inclusion and exclusion, one step at a time
# MAGIC
# MAGIC Each step is a temp view so the attrition counts come from the same logic that builds the cohort.
# MAGIC - **Inclusion A:** age 18 or older at the index date.
# MAGIC - **Inclusion B:** at least one encounter in the lookback window before the index date (continuity of care).
# MAGIC - **Exclusion:** any diagnosis on or before the index date with an excluded prefix (default `E10` type 1 diabetes and `N18.6` ESRD).
# MAGIC - **Optional:** restrict to the uploaded MRN list (`apply_mrn_restriction = true`).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW rs_excl AS
# MAGIC SELECT DISTINCT upper(trim(p)) AS icd10_prefix
# MAGIC FROM (SELECT explode(split(:exclusion_icd10_prefixes, ',')) AS p)
# MAGIC WHERE trim(p) <> '';
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_step0 AS
# MAGIC SELECT patient_id, mrn, date_of_birth FROM src_patients;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_step1 AS
# MAGIC SELECT p.patient_id, p.mrn, p.date_of_birth, i.index_date, i.index_icd10_code, i.qualifying_dx_events
# MAGIC FROM src_patients p
# MAGIC JOIN rs_index i ON i.patient_id = p.patient_id;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_step2 AS
# MAGIC SELECT * FROM rs_step1 WHERE timestampdiff(YEAR, date_of_birth, index_date) >= 18;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_step3 AS
# MAGIC SELECT s.* FROM rs_step2 s
# MAGIC WHERE EXISTS (
# MAGIC   SELECT 1 FROM src_encounters e
# MAGIC   WHERE e.patient_id = s.patient_id
# MAGIC     AND e.encounter_date >= date_sub(s.index_date, CAST(:lookback_days AS INT))
# MAGIC     AND e.encounter_date < s.index_date);
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_step4 AS
# MAGIC SELECT s.* FROM rs_step3 s
# MAGIC WHERE NOT EXISTS (
# MAGIC   SELECT 1 FROM rs_dx_events x
# MAGIC   JOIN rs_excl ex ON x.icd10_code LIKE concat(ex.icd10_prefix, '%')
# MAGIC   WHERE x.patient_id = s.patient_id AND x.encounter_date <= s.index_date);
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW rs_step5 AS
# MAGIC SELECT s.* FROM rs_step4 s
# MAGIC WHERE :apply_mrn_restriction <> 'true'
# MAGIC    OR s.mrn IN (SELECT mrn FROM mrn_list);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4. Write the cohort and the attrition funnel

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:table_prefix || 'cohort_' || :study_id)
# MAGIC COMMENT 'Study cohort (one row per person) built by 01_build_study_cohort. Contains PHI columns (mrn, date_of_birth), tagged class.* for ABAC masking.'
# MAGIC AS
# MAGIC SELECT s.patient_id,
# MAGIC        s.mrn,
# MAGIC        s.date_of_birth,
# MAGIC        s.index_date,
# MAGIC        s.index_icd10_code,
# MAGIC        s.qualifying_dx_events,
# MAGIC        timestampdiff(YEAR, s.date_of_birth, s.index_date) AS age_at_index,
# MAGIC        (ne.patient_id IS NOT NULL) AS ndc_drug_exposure_in_window,
# MAGIC        :study_id AS study_id
# MAGIC FROM rs_step5 s
# MAGIC LEFT JOIN rs_ndc_exposure ne ON ne.patient_id = s.patient_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW rs_attrition_counts AS
# MAGIC SELECT 0 AS step_order, 'Starting population: all patients in the source' AS step, (SELECT count(*) FROM rs_step0) AS n_persons
# MAGIC UNION ALL SELECT 1, 'Qualifying ICD-10 diagnosis (prefix match) with encounter in index window', (SELECT count(*) FROM rs_step1)
# MAGIC UNION ALL SELECT 2, 'Inclusion: age 18 or older at index date', (SELECT count(*) FROM rs_step2)
# MAGIC UNION ALL SELECT 3, 'Inclusion: at least one encounter in lookback window before index', (SELECT count(*) FROM rs_step3)
# MAGIC UNION ALL SELECT 4, concat('Exclusion: diagnosis with an excluded prefix (', :exclusion_icd10_prefixes, ') on or before index'), (SELECT count(*) FROM rs_step4)
# MAGIC UNION ALL SELECT 5, concat('Optional: restricted to the uploaded MRN list (', CASE WHEN :apply_mrn_restriction = 'true' THEN 'on' ELSE 'off' END, ')'), (SELECT count(*) FROM rs_step5);
# MAGIC
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER(:table_prefix || 'attrition_' || :study_id)
# MAGIC COMMENT 'Attrition funnel for the study cohort. n_removed is the drop from the prior step. The informational row (step_order 99) is NOT part of the funnel.'
# MAGIC AS
# MAGIC SELECT step_order, step, n_persons,
# MAGIC        coalesce(lag(n_persons) OVER (ORDER BY step_order) - n_persons, 0) AS n_removed,
# MAGIC        round(100.0 * n_persons / max(n_persons) OVER (), 2) AS pct_of_starting_population,
# MAGIC        :study_id AS study_id
# MAGIC FROM rs_attrition_counts
# MAGIC UNION ALL
# MAGIC SELECT 99, 'Informational (not in funnel): persons with an NDC-matched study drug in window but no qualifying dx', (SELECT count(*) FROM rs_ndc_exposure n WHERE NOT EXISTS (SELECT 1 FROM rs_index i WHERE i.patient_id = n.patient_id)), NULL, NULL, :study_id
# MAGIC ORDER BY step_order;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IDENTIFIER(:table_prefix || 'attrition_' || :study_id) ORDER BY step_order;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5. Tag the PHI columns
# MAGIC
# MAGIC The schema's ABAC policies (created by the setup job) match these `class.*` tags, so the cohort is masked for anyone outside the approved group with no extra work.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE IDENTIFIER(:table_prefix || 'cohort_' || :study_id) ALTER COLUMN mrn SET TAGS ('class.medical_record_number' = '');
# MAGIC ALTER TABLE IDENTIFIER(:table_prefix || 'cohort_' || :study_id) ALTER COLUMN date_of_birth SET TAGS ('class.date_of_birth' = '');
