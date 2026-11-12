"""Build the Research Cohort QA dashboard (src/dashboards/research_cohort_qa.lvdash.json).
Datasets read only the summary tables and the de-identified view; no mrn or date_of_birth column is queried.
Table names are unqualified; the bundle sets the catalog and schema (dataset_catalog / dataset_schema).
The dashboard shows one study. To change it, or if you change table_prefix, run:
    PREFIX=rs_ STUDY=t2d python scripts/build_dashboard.py"""
import json, os
PREFIX = os.environ.get("PREFIX", "rs_")
S = os.environ.get("STUDY", "t2d")
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "dashboards", "research_cohort_qa.lvdash.json")
datasets = [
 {"name": "ds_kpi", "displayName": "KPIs", "queryLines": [
   f"SELECT (SELECT count(*) FROM {PREFIX}af_{S}_deid) AS cohort_persons, ",
   f"(SELECT count(*) FROM {PREFIX}qa_results_{S} WHERE status = 'PASS') AS pass_n, ",
   f"(SELECT count(*) FROM {PREFIX}qa_results_{S} WHERE status = 'WARN') AS warn_n, ",
   f"(SELECT count(*) FROM {PREFIX}qa_results_{S} WHERE status = 'FAIL') AS fail_n, ",
   f"(SELECT round(avg(age_at_index), 1) FROM {PREFIX}cohort_{S}) AS mean_age_at_index"]},
 {"name": "ds_attrition", "displayName": "Attrition", "queryLines": [
   "SELECT CASE step_order WHEN 0 THEN '0 All patients' WHEN 1 THEN '1 Qualifying dx' WHEN 2 THEN '2 Age 18+' ",
   "WHEN 3 THEN '3 Prior encounter' WHEN 4 THEN '4 Exclusions' WHEN 5 THEN '5 MRN list' END AS step_label, ",
   f"n_persons, n_removed FROM {PREFIX}attrition_{S} WHERE step_order <= 5 ORDER BY step_order"]},
 {"name": "ds_demo", "displayName": "Demographics", "queryLines": [
   f"SELECT dimension, level, n_persons_display, CASE WHEN small_cell_flag THEN NULL ELSE n_persons END AS n_plot ",
   f"FROM {PREFIX}qa_distribution_{S} WHERE dimension IN ('gender', 'race', 'ethnicity', 'insurance_payer') ORDER BY dimension, level"]},
 {"name": "ds_age", "displayName": "Age bands", "queryLines": [
   f"SELECT level AS age_band, CASE WHEN small_cell_flag THEN NULL ELSE n_persons END AS n_plot ",
   f"FROM {PREFIX}qa_distribution_{S} WHERE dimension = 'age_band' ORDER BY level"]},
 {"name": "ds_a1c", "displayName": "HbA1c distribution", "queryLines": [
   "SELECT 'Baseline' AS measure, format_number(floor(a1c_baseline * 4) / 4, 2) AS a1c_bin, count(*) AS n_persons ",
   f"FROM {PREFIX}af_{S}_deid WHERE a1c_baseline IS NOT NULL GROUP BY 1, 2 HAVING count(*) >= 11 ",
   "UNION ALL ",
   "SELECT 'Follow-up 6-12 mo', format_number(floor(a1c_followup * 4) / 4, 2), count(*) ",
   f"FROM {PREFIX}af_{S}_deid WHERE a1c_followup IS NOT NULL GROUP BY 1, 2 HAVING count(*) >= 11 ORDER BY 1, 2"]},
 {"name": "ds_util", "displayName": "Utilization", "queryLines": [
   f"SELECT 'ED visits' AS measure, round(avg(ed_visits_12m), 2) AS mean_per_person FROM {PREFIX}af_{S}_deid ",
   f"UNION ALL SELECT 'Inpatient admissions', round(avg(inpatient_admissions_12m), 2) FROM {PREFIX}af_{S}_deid ",
   f"UNION ALL SELECT 'Outpatient visits', round(avg(outpatient_visits_12m), 2) FROM {PREFIX}af_{S}_deid"]},
 {"name": "ds_qa", "displayName": "QA checks", "queryLines": [
   f"SELECT stage, check_id, check_name, status, detail FROM {PREFIX}qa_results_{S} ORDER BY stage, check_id"]},
 {"name": "ds_small", "displayName": "Small cells", "queryLines": [
   f"SELECT dimension, level, n_persons_display FROM {PREFIX}qa_distribution_{S} WHERE small_cell_flag ORDER BY dimension, level"]},
]
def q(name, ds, fields):
    return {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": fields, "disaggregated": True}}]}
def fld(n): return {"name": n, "expression": f"`{n}`"}
def agg(n, expr): return {"name": f"sum({n})", "expression": expr}
def counter(name, title, col, x, y, color=None):
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": "ds_kpi", "fields": [agg(col, f"SUM(`{col}`)")], "disaggregated": False}}],
      "spec": {"version": 2, "widgetType": "counter", "frame": {"showTitle": True, "title": title},
               "encodings": {"value": {"fieldName": f"sum({col})", "displayName": title, "format": {"type": "number-plain", "decimalPlaces": {"type": "max", "places": 0}}}}}},
      "position": {"x": x, "y": y, "width": 3, "height": 3}}
def bar(name, title, ds, xf, yf, x, y, w, h, xtitle, ytitle, color=None):
    enc = {"x": {"fieldName": xf, "scale": {"type": "categorical"}, "axis": {"title": xtitle}},
           "y": {"fieldName": f"sum({yf})", "scale": {"type": "quantitative"}, "axis": {"title": ytitle}}}
    fields = [fld(xf), agg(yf, f"SUM(`{yf}`)")]
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}}
        fields.insert(1, fld(color))
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": fields, "disaggregated": False}}],
      "spec": {"version": 3, "widgetType": "bar", "frame": {"showTitle": True, "title": title}, "encodings": enc}},
      "position": {"x": x, "y": y, "width": w, "height": h}}
def table(name, title, ds, cols, x, y, w, h):
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {"datasetName": ds, "fields": [fld(c) for c, _ in cols], "disaggregated": True}}],
      "spec": {"version": 2, "widgetType": "table", "frame": {"showTitle": True, "title": title},
               "encodings": {"columns": [{"fieldName": c, "displayName": d} for c, d in cols]}}},
      "position": {"x": x, "y": y, "width": w, "height": h}}
def text(name, lines, y, h):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": lines}}, "position": {"x": 0, "y": y, "width": 12, "height": h}}
layout = [
  text("rs_title", [f"## Research Cohort QA: study {S}"], 0, 1),
  text("rs_subtitle", ["Attrition, QA results and the de-identified analytic file for one study. Cells under 11 show as <11 and are not plotted. No MRN or date of birth is queried on this page."], 1, 2),
  counter("k_cohort", "Persons in cohort", "cohort_persons", 0, 3),
  counter("k_pass", "QA PASS", "pass_n", 3, 3),
  counter("k_warn", "QA WARN", "warn_n", 6, 3),
  counter("k_fail", "QA FAIL", "fail_n", 9, 3),
  bar("w_attr", "Attrition funnel (persons)", "ds_attrition", "step_label", "n_persons", 0, 6, 6, 6, "Step", "Persons"),
  bar("w_age", "Age band at index (cells under 11 not plotted)", "ds_age", "age_band", "n_plot", 6, 6, 6, 6, "Age band", "Persons"),
  table("w_demo", "Demographics (small cells shown as <11)", "ds_demo", [("dimension", "Dimension"), ("level", "Level"), ("n_persons_display", "Persons")], 0, 12, 6, 7),
  bar("w_a1c", "HbA1c distribution (cells under 11 suppressed)", "ds_a1c", "a1c_bin", "n_persons", 6, 12, 6, 7, "HbA1c % (quarter-point bin)", "Persons", color="measure"),
  table("w_qa", "Automated QA checks (rules in notebook 02 and 03)", "ds_qa", [("stage", "Stage"), ("check_id", "ID"), ("check_name", "Check"), ("status", "Status"), ("detail", "Detail")], 0, 19, 12, 9),
  bar("w_util", "Mean 12-month utilization per person (de-identified file)", "ds_util", "measure", "mean_per_person", 0, 28, 6, 5, "Visit type", "Mean per person"),
  table("w_small", "Small-cell flags (suppressed in outputs)", "ds_small", [("dimension", "Dimension"), ("level", "Stratum"), ("n_persons_display", "Persons")], 6, 28, 6, 5),
]
dash = {"datasets": datasets, "pages": [{"name": "rs_qa", "displayName": "Research Cohort QA", "pageType": "PAGE_TYPE_CANVAS", "layoutVersion": "GRID_V1", "layout": layout}]}
json.dump(dash, open(OUT, "w"), indent=1)
print("wrote", os.path.normpath(OUT), "datasets", len(datasets), "widgets", len(layout))
