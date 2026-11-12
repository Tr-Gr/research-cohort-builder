# Databricks notebook source
# Research Cohort Builder: 04_share
# Adds the de-identified copy (<prefix>af_<study_id>_deid_share) to an OpenSharing share. Skipped when share_name is empty.
# No recipient is created: attaching a recipient is a separate, approved step (data use agreement first).
# Needs CREATE SHARE on the metastore (first run) and ownership of the share.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

SHARE = P["share_name"]
if not SHARE:
    dbutils.notebook.exit("share_name is empty; no share created")

table = T(f"af_{STUDY}_deid_share")
spark.sql(f"CREATE SHARE IF NOT EXISTS `{SHARE}` COMMENT 'De-identified research analytic files. Recipients are added only under an approved data use agreement.'")
shared = {r["name"] for r in spark.sql(f"SHOW ALL IN SHARE `{SHARE}`").collect()}
if table in shared:
    print(f"{table} is already in share {SHARE}; the share always serves the latest version")
else:
    spark.sql(f"ALTER SHARE `{SHARE}` ADD TABLE {table} COMMENT 'De-identified analytic file, study {STUDY}'")
    print(f"added {table} to share {SHARE}")
