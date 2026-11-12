# Research Cohort Builder

From an investigator's code list to a governed study cohort, an automated QA gate, and a de-identified analytic file that can be exported or shared. One job run is one study: change the parameters and run it again for the next study.

```
code list (CSV upload)  ─►  <prefix>study_codes   (a governed table: owner, history, lineage)
                                  │
patients, encounters,             ▼
diagnoses, medications,  ─►  01 build cohort ─► cohort_<study> + attrition_<study>   (one step = one count)
lab results, NDC crosswalk        │
                                  ▼
                             02 QA checks ─► qa_results_<study> (PASS / WARN / FAIL) ─► gate: FAIL stops the job
                                  │
                                  ▼
                             03 analytic file ─► af_<study> (identified, tagged, masked by ABAC)
                                  │               af_<study>_deid (no identifiers) ─► CSV + Parquet in a volume
                                  ▼
                             04 share (optional) ─► af_<study>_deid_share in an OpenSharing share (no recipients)
```

## What gets deployed

| Resource | What it does |
|---|---|
| `Research Cohort Builder - setup` job | Schema, export volume, mask functions, ABAC policies, and (optionally) synthetic sources, a sample code list, an NDC crosswalk and an MRN list. Safe to re-run. |
| `Research Cohort Builder - study` job | Cohort, QA, analytic file, export and optional share for one study. Every study setting is a job parameter. |
| `Research Cohort QA` dashboard | Attrition funnel, QA results, demographics with small cells suppressed, HbA1c distribution, utilization |

Everything is created in `<catalog>.<schema>` with the prefix `rs_` (configurable). Per study (`<study>` is `study_id`):

| Object | Contents |
|---|---|
| `rs_cohort_<study>` | One row per person: identifiers (tagged), index date, index code, age at index, drug exposure flag |
| `rs_attrition_<study>` | One row per inclusion or exclusion step, with persons remaining and removed |
| `rs_qa_results_<study>` | One row per QA check: rule, metric, status and detail |
| `rs_qa_distribution_<study>` | Demographic strata with small-cell flags (`<11`) |
| `rs_af_<study>` | Person-level analytic file, identifiers tagged and masked outside the approved group |
| `rs_af_<study>_deid` | De-identified view: no patient id, MRN, date of birth, ZIP or exact dates; age bands, year-month dates and a linkage token |
| `rs_af_<study>_deid_share` | Materialized copy of the de-identified view for OpenSharing |
| `/Volumes/<catalog>/<schema>/research_files/exports/<study>/` | `<study>_analytic_file.csv` and `.parquet` |

## Prerequisites

- A Unity Catalog-enabled Azure Databricks workspace with serverless jobs and a SQL warehouse.
- A catalog where you can create a schema, tables, views, functions and a volume, and `MANAGE` on the schema for the ABAC policies.
- An **account-level** group for people approved to see identifiers (for example, per study or per IRB approval). ABAC policies accept only account-level principals.
- The `class.medical_record_number`, `class.date_of_birth`, `class.ethnicity` and `class.location` governed tags, which come with [Data Classification](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-classification). If your workspace doesn't have them, create governed tags with those names, or change the tag names in notebooks 01 and 03 and in `00_governance.py`.
- The [Databricks CLI](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/install).

## Get running in dev

1. In `databricks.yml`, under `targets.dev`, set `workspace.host`, `catalog` and `warehouse_id`. Set `approved_group` to your account-level group, or leave it empty to mask identifiers for everyone while you try it out.
2. `databricks auth login --host https://<your-dev-workspace>.azuredatabricks.net`
3. `databricks bundle deploy -t dev`
4. `databricks bundle run -t dev research_setup` creates the schema, the volume, the policies and about 20,000 synthetic patients.
5. `databricks bundle run -t dev research_study` builds the sample type 2 diabetes study (`study_id = t2d`).
6. Open the job run, then `rs_attrition_t2d`, `rs_qa_results_t2d` and the **Research Cohort QA** dashboard. Query `rs_af_t2d` to see masked identifiers, and add yourself to the approved group to see them unmasked (group changes take a few minutes to apply).

What to expect on the sample data (our test run took about 3 minutes for setup and 5 for the study):

| Step | Persons |
|---|---|
| All patients | 20,000 |
| Qualifying ICD-10 diagnosis in the index window | 2,644 |
| Age 18 or older at index | 2,636 |
| Encounter in the 365 days before index | 2,152 |
| No E10 or N18.6 on or before index | **2,033** |
| Informational: study drug but no qualifying diagnosis | 924 |

QA ends with **0 FAIL** and a few deliberate `WARN`s, the kind of findings you want before analysis: two listed codes (E11.21, E11.42) have no hits, about a third of the cohort has a baseline HbA1c, 109 persons lack a full 12-month follow-up, some demographic strata are under 11, and `R03` warns until `approved_group` is set. `M01` and `M02` warn that the MRN list reads masked until the run identity is in `approved_group` (the MRN list is tagged as PHI); once it is, they reconcile the list, including three MRNs that are deliberately not in the patients table. If `apply_mrn_restriction` is `true` and the list reads masked, the job stops rather than silently matching nobody.

## Run a new study

1. **Upload the code list** as a table: Catalog Explorer > your schema > Create > Table > Upload file. Use the columns `study_id, code_type, code_value, description`, with `code_type` = `ICD10CM` (prefix match: `E11` matches every E11.x) or `NDC` (any format; non-digits are removed). `data/study_codes_t2d.csv` is the template. The sample CSVs are also in the volume under `intake/`.
2. **Optional MRN list:** upload a table with an `mrn` column. It is tagged and masked like any identifier (`00_governance.py` tags the sample list; tag yours the same way).
3. **Run the study job with parameters:**
   ```
   databricks bundle run -t dev research_study --params study_id=ckd2026,code_table=<catalog>.<schema>.ckd_codes,exclusion_icd10_prefixes=N18.6,index_window_start=2025-01-01,index_window_end=2025-12-31
   ```
   or use **Run now with different parameters** in the job UI.
4. **Read the QA results** before using the file. `WARN` rows are findings to review; any `FAIL` stops the job before anything is written or exported.
5. To show a study on the dashboard: `STUDY=ckd2026 python scripts/build_dashboard.py`, then deploy again (or duplicate the dashboard per study).

| Parameter | Default | Meaning |
|---|---|---|
| `study_id` | `t2d` | Suffix for every output (letters, digits, underscores) |
| `code_table` | `<prefix>study_codes` | The uploaded code list |
| `index_window_start`, `index_window_end` | 2024-01-01, 2025-12-31 | Qualifying diagnosis must fall in this window; the earliest one is the index date |
| `lookback_days` | 365 | Requires at least one encounter in this many days before the index date |
| `exclusion_icd10_prefixes` | `E10,N18.6` | Comma-separated prefixes; any of them on or before the index date excludes the person |
| `mrn_table`, `apply_mrn_restriction` | `<prefix>mrn_list`, `false` | Restrict the cohort to an uploaded MRN list |
| `baseline_lookback_days`, `a1c_baseline_window_days`, `followup_days` | 365, 180, 365 | Analytic-file windows |
| `share_name` | empty | OpenSharing share for the de-identified copy (empty = no share) |

The baseline comorbidity, medication and lab variables in notebook 03 are examples for a diabetes study. Copy the notebook, or add variables, per study design.

## Use your own data

Set these variables (or job parameters) to your Standardized or Enriched tables, then set `create_sample_data: "false"`. If your column names differ, create views that rename them.

| Variable | Required columns |
|---|---|
| `patients_table` | `patient_id`, `mrn`, `date_of_birth` (DATE), `gender`, `race`, `ethnicity`, `insurance_payer_category`, `zip_code` |
| `encounters_table` | `encounter_id`, `patient_id`, `encounter_date` (DATE), `encounter_type` (`outpatient`, `emergency`, `inpatient`, ...) |
| `diagnoses_table` | `encounter_id`, `patient_id`, `icd10_code` (with the dot, for example `E11.9`) |
| `medications_table` | `patient_id`, `medication_name`, `ordered_datetime` |
| `lab_results_table` | `patient_id`, `loinc_code`, `result_value` (numeric), `resulted_datetime` |
| `ndc_crosswalk_table` | `ndc_11` (11 digits, no dashes), `medication_name` (matches `medications_table`), `rxnorm_concept_id` |

The sample crosswalk is synthetic. In production, use the NDC crosswalk your pharmacy informatics team owns, so every study matches drugs the same way. If you use OMOP, the same pattern applies with concept sets in place of raw code lists.

## Governance

- **Code lists are governed tables.** The code list defines the study, so it gets an owner, a description, version history (Delta time travel) and lineage to every cohort and file built from it. That is what you show an IRB, a reviewer or a grant office when they ask how you got your numbers.
- **Tags and masks.** Notebooks 01 and 03 tag identifier columns. Two ABAC policies on the schema (`00_governance.py`) mask any column tagged `class.medical_record_number` (last 4 characters) or `class.date_of_birth` (NULL) for everyone except `approved_group`. New tables are covered as soon as their columns are tagged. See [ABAC](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/abac/).
- **Reads of identifiers.** The job reads identifiers from the source patients table, so outputs are correct whoever runs it. Run staging and prod as a service principal that is allowed to read the source tables. QA check `R03` reports whether the run identity is in the approved group.
- **Linkage token.** `linkage_token` is a SHA-256 hash of the MRN with a secret key from `linkage_secret_scope`. Without a secret scope a demo-only key is used and the run prints a warning: never use that for real data. In production, use keyed tokens from your tokenization service.
- **What leaves.** Only the de-identified view is exported, and the volume is audited. For a named partner under a data use agreement, share it with [OpenSharing](https://learn.microsoft.com/en-us/azure/databricks/delta-sharing/) (read-only, revocable). When partners must not see row-level records, use [Clean Rooms](https://learn.microsoft.com/en-us/azure/databricks/clean-rooms/). The share job never adds recipients.
- **Small cells.** Strata under 11 are flagged in `rs_qa_distribution_<study>`, shown as `<11` and not plotted on the dashboard. The de-identified file is person-level, so apply your own disclosure review before release.
- `00_governance.py` creates the two policies with `TO account users EXCEPT <approved_group>`. Our end-to-end test ran against a schema that already had equivalent policies, so test policy creation in your dev schema first and confirm with a non-approved user that MRN and date of birth read masked.
- Review the de-identification rules (columns dropped, date precision, age bands) with your privacy office. This package is a starting pattern, not a certified de-identification method.

## Staging and prod

1. Create a service principal per environment with access to the source tables, the catalog privileges above, and permission to use serverless jobs.
2. Set `workspace.host`, `catalog`, `warehouse_id`, `approved_group`, the source table variables and `linkage_secret_scope` for `staging` and `prod` in `databricks.yml`.
3. In GitHub, create `staging` and `prod` environments with the variable `DATABRICKS_HOST` and the secrets `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`. Add required reviewers to `prod`.
4. Pull requests run `validate.yml`; merging to `main` deploys to staging and runs setup; run `deploy.yml` with `target = prod` to promote. The workflows skip themselves until `DATABRICKS_HOST` is set, so nothing runs before you configure them.

## Drafting cohort logic with Genie Code

Genie Code can draft the cohort SQL from a plain-language description in any notebook. Paste the generated SQL next to notebook 01's steps and keep the stepwise structure, so every inclusion and exclusion still produces a count. An example prompt is in `docs/GENIE_CODE_PROMPT.md`.

## Learn more

- [Declarative Automation Bundles](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/)
- [Lakeflow Jobs](https://learn.microsoft.com/en-us/azure/databricks/jobs/) and [job parameters](https://learn.microsoft.com/en-us/azure/databricks/jobs/job-parameters)
- [Create a table from an uploaded file](https://learn.microsoft.com/en-us/azure/databricks/ingestion/file-upload/upload-data)
- [Data lineage](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/data-lineage)
- [Governed tags](https://learn.microsoft.com/en-us/azure/databricks/database-objects/tags)
- [Genie Code](https://learn.microsoft.com/en-us/azure/databricks/notebooks/code-assistant)
