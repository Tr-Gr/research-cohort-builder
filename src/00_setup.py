# Databricks notebook source
# Research Cohort Builder: 00_setup (part of the setup job; safe to re-run)
# Creates the schema and the export volume. Nothing else depends on sample data.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "research")
dbutils.widgets.text("export_volume", "research_files")
CAT = dbutils.widgets.get("catalog").strip()
SCH = dbutils.widgets.get("schema").strip()
VOL = dbutils.widgets.get("export_volume").strip()
assert CAT and SCH and VOL, "Set the catalog, schema and export_volume parameters"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CAT}`.`{SCH}` COMMENT 'Research Cohort Builder: code lists, cohorts, QA results and analytic files'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CAT}`.`{SCH}`.`{VOL}` COMMENT 'Code list intake and de-identified exports. Reads and writes are audited.'")
for d in ("intake", "exports"):
    dbutils.fs.mkdirs(f"/Volumes/{CAT}/{SCH}/{VOL}/{d}")
print(f"ready: {CAT}.{SCH} and /Volumes/{CAT}/{SCH}/{VOL}")
