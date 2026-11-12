# Databricks notebook source
# Research Cohort Builder: 00_sample_data (optional; part of the setup job)
# Creates SYNTHETIC source tables shaped like a standardized clinical model, plus a sample code list, an NDC
# crosswalk and an MRN list. Nothing here reads real data. Set create_sample_data = false and point the study job
# at your own tables (patients_table, encounters_table, ...) once they are mapped.
#
# Tables (in <catalog>.<schema>, prefixed):
#   sample_patients, sample_encounters, sample_diagnoses, sample_medications, sample_lab_results
#   ndc_crosswalk  SYNTHETIC NDCs (labelers 99001-99003), not from the FDA NDC Directory
#   study_codes    the sample type 2 diabetes code list (data/study_codes_t2d.csv)
#   mrn_list       a sample investigator MRN list (200 rows, 3 deliberately unknown MRNs)
# The two CSVs are also copied to /Volumes/<catalog>/<schema>/<export_volume>/intake/ to practice the upload step.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "research")
dbutils.widgets.text("table_prefix", "rs_")
dbutils.widgets.text("export_volume", "research_files")
dbutils.widgets.text("create_sample_data", "true")
dbutils.widgets.text("sample_patients", "20000")
CAT = dbutils.widgets.get("catalog").strip()
SCH = dbutils.widgets.get("schema").strip()
P = dbutils.widgets.get("table_prefix").strip()
VOL = dbutils.widgets.get("export_volume").strip()
assert CAT and SCH, "Set the catalog and schema parameters"
if dbutils.widgets.get("create_sample_data").strip().lower() != "true":
    dbutils.notebook.exit("create_sample_data is false; skipped")
N = int(dbutils.widgets.get("sample_patients"))
T = lambda n: f"{CAT}.{SCH}.{P}{n}"

# COMMAND ----------

import numpy as np
import pandas as pd

rng = np.random.default_rng(2026)
DATA_START, DATA_END = pd.Timestamp("2023-01-01"), pd.Timestamp("2026-06-30")
AS_OF = pd.Timestamp("2025-01-01")

# Patients
pid = np.array([f"P{i:07d}" for i in range(1, N + 1)])
mrn = np.array([f"MRN{i:08d}" for i in range(1, N + 1)])
age = np.clip(rng.normal(46, 22, N), 0, 98).astype(int)
dob = AS_OF - pd.to_timedelta(age * 365.25 + rng.integers(0, 365, N), unit="D")
payer = np.where(age >= 65, rng.choice(["Medicare", "Commercial"], N, p=[0.85, 0.15]),
                 rng.choice(["Commercial", "Medicaid", "Self-pay"], N, p=[0.72, 0.24, 0.04]))
patients = pd.DataFrame({
    "patient_id": pid, "mrn": mrn, "date_of_birth": dob.date,
    "gender": rng.choice(["Female", "Male", "Unknown"], N, p=[0.51, 0.485, 0.005]),
    "race": rng.choice(["White", "Black or African American", "Asian", "American Indian or Alaska Native",
                        "Native Hawaiian or Other Pacific Islander", "Multiple", "Unknown"], N,
                       p=[0.71, 0.10, 0.08, 0.012, 0.003, 0.045, 0.05]),
    "ethnicity": rng.choice(["Not Hispanic or Latino", "Hispanic or Latino", "Unknown"], N, p=[0.88, 0.07, 0.05]),
    "insurance_payer_category": payer,
    "zip_code": rng.choice(["55101", "55104", "55106", "55406", "55418", "55337", "55901", "56001", "54016", "55021",
                            "56301", "55303", "55044", "54701"], N),
})

# Conditions per patient (drive diagnoses, medications and labs)
adult = age >= 18
p_t2d = np.clip((age - 25) / 50, 0, 1) * 0.32
t2d = adult & (rng.random(N) < p_t2d)
t1d = (rng.random(N) < 0.006) & ~t2d
t1d_miscoded = t1d & (rng.random(N) < 0.4)             # T1D patients who also carry an E11 code (exclusion removes them)
esrd = (t2d & (rng.random(N) < 0.05)) | (adult & (rng.random(N) < 0.003))
ckd = t2d & (rng.random(N) < 0.18)
htn = adult & (rng.random(N) < np.where(t2d, 0.65, 0.25))
hld = adult & (rng.random(N) < np.where(t2d, 0.60, 0.22))
obesity = adult & (rng.random(N) < np.where(t2d, 0.50, 0.20))
chf = adult & (rng.random(N) < np.where(t2d, 0.07, 0.015))
prediabetes_metformin = adult & ~t2d & (rng.random(N) < 0.07)   # on metformin without a T2D diagnosis

# Encounters
n_enc = rng.poisson(np.where(t2d | t1d, 9, 4))
enc_pidx = np.repeat(np.arange(N), n_enc)
M = len(enc_pidx)
span = (DATA_END - DATA_START).days
encounters = pd.DataFrame({
    "encounter_id": [f"E{i:09d}" for i in range(1, M + 1)],
    "patient_id": pid[enc_pidx],
    "encounter_date": (DATA_START + pd.to_timedelta(rng.integers(0, span + 1, M), unit="D")).date,
    "encounter_type": rng.choice(["outpatient", "emergency", "inpatient", "observation"], M, p=[0.80, 0.12, 0.06, 0.02]),
})

# Diagnoses: one or two per encounter. E11.21 and E11.42 are never generated, so QA flags them as codes with no hits.
E11 = ["E11.9", "E11.65", "E11.22", "E11.40", "E11.319"]
E11_P = [0.55, 0.20, 0.12, 0.08, 0.05]
GENERIC = ["Z00.00", "J06.9", "M54.50", "R51.9", "K21.9", "F41.1", "J45.909", "Z23", "R10.9", "N39.0"]
dx_rows = []
for k in range(M):
    i = enc_pidx[k]
    eid, p = encounters.encounter_id.iat[k], pid[i]
    codes = []
    if (t2d[i] or t1d_miscoded[i]) and rng.random() < 0.55:
        codes.append(rng.choice(E11, p=E11_P))
    if t1d[i] and rng.random() < 0.6:
        codes.append("E10.9")
    for flag, code, pr in ((esrd, "N18.6", 0.4), (ckd, "N18.3", 0.35), (htn, "I10", 0.35), (hld, "E78.5", 0.3),
                           (obesity, "E66.9", 0.25), (chf, "I50.9", 0.4)):
        if flag[i] and rng.random() < pr:
            codes.append(code)
    if not codes or rng.random() < 0.3:
        codes.append(rng.choice(GENERIC))
    dx_rows.extend((eid, p, c) for c in dict.fromkeys(codes))
diagnoses = pd.DataFrame(dx_rows, columns=["encounter_id", "patient_id", "icd10_code"])
diagnoses.insert(0, "diagnosis_id", [f"D{i:09d}" for i in range(1, len(diagnoses) + 1)])

# Medications and HbA1c labs, attached to encounters
OTHER_MEDS = ["acetaminophen", "amoxicillin", "aspirin", "azithromycin", "gabapentin", "levothyroxine", "omeprazole",
              "prednisone", "metoprolol", "furosemide", "warfarin"]
med_rows, lab_rows = [], []
enc_dates = pd.to_datetime(encounters.encounter_date)
for k in range(M):
    i = enc_pidx[k]
    eid, p, ts = encounters.encounter_id.iat[k], pid[i], enc_dates.iat[k] + pd.Timedelta(hours=int(rng.integers(8, 18)))
    meds = []
    if t2d[i] and rng.random() < 0.55: meds.append("metformin")
    if (t2d[i] and rng.random() < 0.15) or (t1d[i] and rng.random() < 0.7): meds.append("insulin glargine")
    if prediabetes_metformin[i] and rng.random() < 0.5: meds.append("metformin")
    if hld[i] and rng.random() < 0.45: meds.append("atorvastatin")
    if htn[i] and rng.random() < 0.45: meds.append("lisinopril")
    if rng.random() < 0.15: meds.append(rng.choice(OTHER_MEDS))
    med_rows.extend((eid, p, m, ts) for m in dict.fromkeys(meds))
    if (t2d[i] or t1d[i]) and rng.random() < 0.17:
        lab_rows.append((eid, p, "4548-4", "Hemoglobin A1c", round(float(np.clip(rng.normal(7.9, 1.3), 5.4, 13.5)), 1), "%", ts + pd.Timedelta(hours=3)))
    elif adult[i] and rng.random() < 0.04:
        lab_rows.append((eid, p, "4548-4", "Hemoglobin A1c", round(float(np.clip(rng.normal(5.4, 0.3), 4.3, 6.4)), 1), "%", ts + pd.Timedelta(hours=3)))
    if rng.random() < 0.2:
        lab_rows.append((eid, p, "2345-7", "Glucose", round(float(np.clip(rng.normal(150 if t2d[i] else 95, 25), 60, 400)), 0), "mg/dL", ts + pd.Timedelta(hours=2)))
medications = pd.DataFrame(med_rows, columns=["encounter_id", "patient_id", "medication_name", "ordered_datetime"])
medications.insert(0, "medication_order_id", [f"M{i:09d}" for i in range(1, len(medications) + 1)])
labs = pd.DataFrame(lab_rows, columns=["encounter_id", "patient_id", "loinc_code", "lab_name", "result_value", "unit", "resulted_datetime"])
labs.insert(0, "lab_result_id", [f"L{i:09d}" for i in range(1, len(labs) + 1)])
print(f"patients {N}, encounters {M}, diagnoses {len(diagnoses)}, medications {len(medications)}, labs {len(labs)}")

# COMMAND ----------

# SYNTHETIC NDC crosswalk: every medication above, with synthetic labelers 99001-99003. Not from the FDA NDC Directory.
MEDS = sorted(set(OTHER_MEDS + ["metformin", "insulin glargine", "atorvastatin", "lisinopril"]))
FORM = {"insulin glargine": ("INJECTION, SOLUTION", ["3 mL pen, 5-pack", "10 mL vial", "3 mL pen, 1-pack"]),
        "metformin": ("TABLET, FILM COATED", ["500 mg, 30 tablets", "1000 mg, 90 tablets", "500 mg unit dose, 100 tablets", "850 mg ER, 60 tablets"])}
FIXED = {("metformin", 0): "1444", ("metformin", 1): "1557", ("insulin glargine", 0): "1333"}  # match data/study_codes_t2d.csv
LABELERS = [("99001", "SYNTHETIC REPACKAGER A"), ("99002", "SYNTHETIC MANUFACTURER B"), ("99003", "SYNTHETIC UNIT-DOSE C")]
xw = []
for i, med in enumerate(MEDS):
    form, pkgs = FORM.get(med, ("TABLET", ["30 tablets", "90 tablets", "100 tablets unit dose"]))
    for k, pkg in enumerate(pkgs):
        lab, lab_name = LABELERS[k % 3]
        prod = FIXED.get((med, k), f"{(i * 37 + k * 113) % 7000 + 2000:04d}")
        pack = f"{k + 1:02d}"
        xw.append((f"{lab}{prod}{pack}", f"{lab}-{prod}-{pack}", lab, prod, pack, med, f"{med.title()} {pkg}", form, pkg, lab_name, None, True))
crosswalk = pd.DataFrame(xw, columns=["ndc_11", "ndc_hyphenated", "labeler_code", "product_code", "package_code", "medication_name",
                                      "product_description", "dosage_form", "package_size", "labeler_name", "rxnorm_concept_id", "is_synthetic"])
crosswalk["rxnorm_concept_id"] = crosswalk["rxnorm_concept_id"].astype("string")

# COMMAND ----------

import os, shutil
nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
data_dir = os.path.normpath(os.path.join("/Workspace" + os.path.dirname(nb), "..", "data"))
codes = pd.read_csv(os.path.join(data_dir, "study_codes_t2d.csv"), dtype=str)

t2d_adults = pid[t2d & adult]
picked = list(rng.choice(t2d_adults, 150, replace=False)) + list(rng.choice(pid[~t2d], 47, replace=False))
mrn_by_pid = dict(zip(pid, mrn))
mrn_list = pd.DataFrame({"mrn": [mrn_by_pid[x] for x in picked] + ["MRN99999901", "MRN99999902", "MRN99999903"],
                         "list_source": "sample_investigator_list"})

intake = f"/Volumes/{CAT}/{SCH}/{VOL}/intake"
os.makedirs(intake, exist_ok=True)
shutil.copy(os.path.join(data_dir, "study_codes_t2d.csv"), f"{intake}/study_codes_t2d.csv")
mrn_list.to_csv(f"{intake}/mrn_list_sample.csv", index=False)

# COMMAND ----------

def save(pdf, name, comment):
    spark.createDataFrame(pdf).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T(name))
    spark.sql(f"COMMENT ON TABLE {T(name)} IS '{comment}'")
    print(f"{T(name)}: {len(pdf)} rows")

save(patients, "sample_patients", "SYNTHETIC patients (one row per person). Replace with your standardized patient table.")
save(encounters, "sample_encounters", "SYNTHETIC encounters, 2023-01-01 to 2026-06-30.")
save(diagnoses, "sample_diagnoses", "SYNTHETIC ICD-10-CM diagnoses, one or more per encounter.")
save(medications, "sample_medications", "SYNTHETIC medication orders. medication_name joins to the NDC crosswalk.")
save(labs, "sample_lab_results", "SYNTHETIC lab results (LOINC 4548-4 HbA1c, 2345-7 glucose).")
save(crosswalk, "ndc_crosswalk", "SYNTHETIC NDC crosswalk (labelers 99001-99003), NOT from the FDA NDC Directory. Replace with the crosswalk your pharmacy informatics team owns.")
codes["code_type"] = codes["code_type"].str.strip().str.upper()
save(codes, "study_codes", "Sample study code list (ICD-10-CM and NDC) from data/study_codes_t2d.csv.")
save(mrn_list, "mrn_list", "SYNTHETIC investigator MRN list (200 rows; 3 MRNs deliberately unknown). PHI in real use: tagged and masked.")
