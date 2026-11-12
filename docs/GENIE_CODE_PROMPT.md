# Genie Code prompt: draft the cohort SQL

Open notebook `01_build_study_cohort`, open Genie Code, and paste a prompt like this one. Replace the table names with yours.

```
Using <catalog>.<schema>.rs_sample_diagnoses, <catalog>.<schema>.rs_sample_encounters and
<catalog>.<schema>.rs_study_codes, find patients with a type 2 diabetes diagnosis code from the code list
(ICD10CM rows, prefix match) on an encounter between 2024-01-01 and 2025-12-31. Set the index date to the
earliest qualifying encounter date. Include adults aged 18 or older at the index date with at least one
encounter in the 365 days before the index date. Exclude anyone with an E10 or N18.6 diagnosis on or before
the index date. Return one row per patient with patient_id, mrn, date_of_birth, index_date and age_at_index.
```

Compare the result with Steps 2 and 3 of the notebook. Keep the stepwise version as the record: each inclusion and exclusion is its own view with its own count, so the attrition funnel shows exactly the logic that ran. Analysts review generated SQL before it goes into the job.
