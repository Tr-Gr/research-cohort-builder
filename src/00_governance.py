# Databricks notebook source
# Research Cohort Builder: 00_governance (part of the setup job; safe to re-run)
# Mask functions plus two ABAC policies on the schema. Any column tagged class.medical_record_number or
# class.date_of_birth in this schema is masked for everyone except approved_group (an account-level group).
# If approved_group is empty the masks apply to everyone; the study job still builds correct files because it
# reads identifiers from the source patients table, not from the masked outputs.
# Requires MANAGE (or ownership) on the schema. Set create_policies = false to manage policies yourself.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "research")
dbutils.widgets.text("table_prefix", "rs_")
dbutils.widgets.text("approved_group", "")
dbutils.widgets.text("create_policies", "true")
CAT = dbutils.widgets.get("catalog").strip()
SCH = dbutils.widgets.get("schema").strip()
P = dbutils.widgets.get("table_prefix").strip()
GROUP = dbutils.widgets.get("approved_group").strip()
assert CAT and SCH, "Set the catalog and schema parameters"
S = f"{CAT}.{SCH}"

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE FUNCTION {S}.{P}mask_mrn(v STRING) RETURNS STRING
COMMENT 'ABAC mask for class.medical_record_number: last 4 characters only.'
RETURN CASE WHEN v IS NULL THEN NULL ELSE concat(repeat('*', greatest(length(v) - 4, 0)), right(v, 4)) END""")
spark.sql(f"""CREATE OR REPLACE FUNCTION {S}.{P}mask_dob(v DATE) RETURNS DATE
COMMENT 'ABAC mask for class.date_of_birth: NULL.'
RETURN CAST(NULL AS DATE)""")

# Tag the MRN list too: an investigator's list of patients is PHI.
if spark.catalog.tableExists(f"{S}.{P}mrn_list"):
    spark.sql(f"ALTER TABLE {S}.{P}mrn_list ALTER COLUMN mrn SET TAGS ('class.medical_record_number' = '')")

# COMMAND ----------

if dbutils.widgets.get("create_policies").strip().lower() != "true":
    dbutils.notebook.exit("create_policies is false; mask functions created, policies skipped")

except_clause = f" EXCEPT `{GROUP}`" if GROUP else ""
if not GROUP:
    print("WARNING: approved_group is empty, so the masks apply to everyone. Set it to an account-level group.")
for name, tag, fn in [(f"{P}mask_mrn", "class.medical_record_number", f"{P}mask_mrn"),
                      (f"{P}mask_dob", "class.date_of_birth", f"{P}mask_dob")]:
    try:
        spark.sql(f"DROP POLICY {name} ON SCHEMA {S}")
    except Exception as e:
        if "NOT_FOUND" not in str(e) and "does not exist" not in str(e):
            raise
    spark.sql(f"CREATE POLICY {name} ON SCHEMA {S} COMMENT 'Research Cohort Builder: mask {tag}' "
              f"COLUMN MASK {S}.{fn} TO `account users`{except_clause} FOR TABLES "
              f"MATCH COLUMNS has_tag('{tag}') AS c ON COLUMN c")
    print(f"policy {name}: {tag} masked for account users{except_clause}")
