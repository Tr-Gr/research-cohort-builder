# Databricks notebook source
# Research Cohort Builder: shared parameters for the study notebooks (included with %run ./_config).
# Defines every widget, resolves table names, sets the current catalog and schema, and creates the temp views
# the SQL cells read from:
#   src_patients, src_encounters, src_diagnoses, src_medications, src_lab_results, src_ndc_crosswalk,
#   study_codes (the code list), mrn_list (empty if no MRN list was given)
# Outputs are written to <catalog>.<schema>.<table_prefix>... so SQL cells refer to them as
# IDENTIFIER(:table_prefix || '<name>_' || :study_id).

# COMMAND ----------

DEFAULTS = {
    "catalog": "", "schema": "research", "table_prefix": "rs_", "study_id": "t2d",
    "code_table": "", "mrn_table": "", "apply_mrn_restriction": "false",
    "index_window_start": "2024-01-01", "index_window_end": "2025-12-31",
    "lookback_days": "365", "exclusion_icd10_prefixes": "E10,N18.6",
    "baseline_lookback_days": "365", "a1c_baseline_window_days": "180", "followup_days": "365",
    "patients_table": "", "encounters_table": "", "diagnoses_table": "", "medications_table": "",
    "lab_results_table": "", "ndc_crosswalk_table": "",
    "approved_group": "", "export_volume": "research_files",
    "linkage_secret_scope": "", "linkage_secret_key": "linkage_salt", "share_name": "",
}
for _k, _v in DEFAULTS.items():
    dbutils.widgets.text(_k, _v)
P = {k: dbutils.widgets.get(k).strip() for k in DEFAULTS}
CAT, SCH, PFX, STUDY = P["catalog"], P["schema"], P["table_prefix"], P["study_id"]
assert CAT and SCH, "Set the catalog and schema parameters"
assert STUDY.replace("_", "").isalnum(), "study_id may contain only letters, digits and underscores"
T = lambda n: f"{CAT}.{SCH}.{PFX}{n}"

SOURCES = {
    "src_patients": P["patients_table"] or T("sample_patients"),
    "src_encounters": P["encounters_table"] or T("sample_encounters"),
    "src_diagnoses": P["diagnoses_table"] or T("sample_diagnoses"),
    "src_medications": P["medications_table"] or T("sample_medications"),
    "src_lab_results": P["lab_results_table"] or T("sample_lab_results"),
    "src_ndc_crosswalk": P["ndc_crosswalk_table"] or T("ndc_crosswalk"),
    "study_codes": P["code_table"] or T("study_codes"),
}
MRN_TABLE = P["mrn_table"] or T("mrn_list")

spark.sql(f"USE CATALOG `{CAT}`")
spark.sql(f"USE SCHEMA `{SCH}`")
for _view, _table in SOURCES.items():
    assert spark.catalog.tableExists(_table), f"{_view}: table {_table} not found (run the setup job or set the parameter)"
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {_view} AS SELECT * FROM {_table}")
if spark.catalog.tableExists(MRN_TABLE):
    spark.sql(f"CREATE OR REPLACE TEMP VIEW mrn_list AS SELECT trim(mrn) AS mrn FROM {MRN_TABLE}")
    # The MRN list is tagged, so it reads masked unless the run identity is in approved_group.
    _masked = spark.sql("SELECT count(*) AS n FROM mrn_list WHERE mrn LIKE '*%'").first()["n"]
    assert not (_masked and P["apply_mrn_restriction"] == "true"), (
        f"{MRN_TABLE} reads masked for this identity, so the MRN restriction would match nobody. "
        "Run the job as a member of approved_group.")
else:
    assert P["apply_mrn_restriction"] != "true", f"apply_mrn_restriction is true but {MRN_TABLE} does not exist"
    spark.sql("CREATE OR REPLACE TEMP VIEW mrn_list AS SELECT CAST(NULL AS STRING) AS mrn WHERE false")
print("study", STUDY, "| outputs", T(f"*_{STUDY}"))
for _view, _table in SOURCES.items():
    print(f"  {_view:18} <- {_table}")
print(f"  {'mrn_list':18} <- {MRN_TABLE if spark.catalog.tableExists(MRN_TABLE) else '(none)'}")
