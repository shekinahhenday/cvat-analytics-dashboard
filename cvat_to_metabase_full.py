# ###-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ## A CODE WITH CHANGES TO HELP PULL ALL THE NECESSARY DATA FROM CVAT
# import os, time, math, json
# from collections import defaultdict, Counter
# from datetime import datetime
# from urllib.parse import urljoin

# import requests
# import pandas as pd
# from dotenv import load_dotenv
# import os
# from scripts.sheets_io import write_df_to_sheet

# # ---------------------------
# # Config & helpers
# # ---------------------------
# load_dotenv()

# raw_base = os.getenv("CVAT_BASE_URL", "").strip()
# if not raw_base:
#     raise SystemExit("Missing CVAT_BASE_URL (e.g. https://app.cvat.ai). Set it in .env or PowerShell env.")
# if not raw_base.startswith(("http://", "https://")):
#     raw_base = "https://" + raw_base
# BASE = raw_base.rstrip("/") + "/"

# API_TOKEN = os.getenv("CVAT_API_TOKEN")
# if not API_TOKEN:
#     raise SystemExit("Missing CVAT_API_TOKEN. Set it in .env or PowerShell env.")
# ORG = os.getenv("CVAT_ORG_HEADER", "").strip() or None

# # Optional limits for first run
# MAX_PROJECTS = int(os.getenv("ETL_MAX_PROJECTS", "0") or "0")          # 0 = unlimited
# MAX_TASKS_PER_PROJ = int(os.getenv("ETL_MAX_TASKS_PER_PROJ", "0") or "0")
# MAX_JOBS_PER_TASK = int(os.getenv("ETL_MAX_JOBS_PER_TASK", "0") or "0")

# # Session
# sess = requests.Session()
# sess.headers.update({
#     "Authorization": f"Token {API_TOKEN}",
#     "Accept": "application/vnd.cvat+json",
# })
# if ORG:
#     sess.headers["X-Organization"] = ORG

# def get_all(endpoint, params=None, sleep=0.0):
#     """Fetches all pages from DRF-style paginated endpoints (or a list)."""
#     url = urljoin(BASE, endpoint.lstrip("/"))
#     out = []
#     while url:
#         r = sess.get(url, params=params, timeout=60)
#         r.raise_for_status()
#         data = r.json()
#         if isinstance(data, dict) and "results" in data:
#             out.extend(data["results"])
#             url = data.get("next")
#             params = None
#         else:
#             if isinstance(data, list):
#                 out.extend(data)
#             else:
#                 out.append(data)
#             url = None
#         if sleep:
#             time.sleep(sleep)
#     return out

# def get_projects():
#     return get_all("/api/projects")

# def get_tasks(project_id):
#     return get_all("/api/tasks", params={"project_id": project_id})

# def get_jobs(task_id):
#     return get_all("/api/jobs", params={"task_id": task_id})

# def get_project_labels(project_payload):
#     """Use embedded labels; if missing, try labels endpoint."""
#     labels = project_payload.get("labels", []) or []
#     if labels:
#         return labels
#     # Fallback (some deployments support filtering labels by project_id)
#     try:
#         return get_all("/api/labels", params={"project_id": project_payload.get("id")})
#     except Exception:
#         return []

# def get_job_annotations(job_id):
#     """
#     Pull annotations for a job. This can be heavy; keep first run scoped with MAX_* envs.
#     Standard JSON has keys: shapes, tracks, tags (each item has label_id).
#     """
#     url = urljoin(BASE, f"/api/jobs/{job_id}/annotations")
#     r = sess.get(url, timeout=120)
#     r.raise_for_status()
#     return r.json()  # dict with 'shapes', 'tracks', 'tags'

# def parse_iso(ts):
#     if not ts:
#         return None
#     try:
#         # CVAT timestamps are usually ISO 8601
#         return datetime.fromisoformat(ts.replace("Z", "+00:00"))
#     except Exception:
#         return None

# # ---------------------------
# # ETL
# # ---------------------------
# projects_raw = get_projects()
# if MAX_PROJECTS and len(projects_raw) > MAX_PROJECTS:
#     projects_raw = projects_raw[:MAX_PROJECTS]

# proj_rows = []
# label_catalog_rows = []
# label_usage_rows = []
# user_workload_rows = []
# time_series_rows = []

# for p in projects_raw:
#     pid = p.get("id")
#     pname = p.get("name")
#     powner = p.get("owner", {}).get("username") if isinstance(p.get("owner"), dict) else p.get("owner")
#     pcreated = p.get("created_date") or p.get("created")  # name varies
#     pupdated = p.get("updated_date") or p.get("updated")  # name varies
#     pcreated_dt = parse_iso(pcreated)
#     pupdated_dt = parse_iso(pupdated)

#     # Labels present (catalog)
#     labels = get_project_labels(p)
#     label_names = []
#     for L in labels or []:
#         if not isinstance(L, dict):
#             continue
#         label_catalog_rows.append({
#             "project_id": pid,
#             "project_name": pname,
#             "label_id": L.get("id"),
#             "label_name": L.get("name"),
#             "label_color": L.get("color"),
#             "label_type": L.get("type"),
#         })
#         if L.get("name"):
#             label_names.append(L["name"])

#     # Tasks & jobs (progress + time metrics + workload + label distribution)
#     tasks = get_tasks(pid)
#     if MAX_TASKS_PER_PROJ and len(tasks) > MAX_TASKS_PER_PROJ:
#         tasks = tasks[:MAX_TASKS_PER_PROJ]

#     # Task status summary
#     task_status_counts = Counter()
#     jobs_total = 0
#     job_status_counts = Counter()

#     # Label usage counters for this project
#     label_usage_counter = Counter()

#     # User workload: jobs per assignee & status
#     # We'll collect rows at the per-job level for flexibility
#     for t in tasks:
#         tid = t.get("id")
#         tname = t.get("name")
#         tstatus = t.get("status") or t.get("state")
#         task_status_counts[tstatus] += 1

#         tcreated = parse_iso(t.get("created_date") or t.get("created"))
#         tupdated = parse_iso(t.get("updated_date") or t.get("updated"))
#         # Time series table for trends
#         time_series_rows.append({
#             "project_id": pid,
#             "project_name": pname,
#             "task_id": tid,
#             "task_name": tname,
#             "task_status": tstatus,
#             "task_created": tcreated,
#             "task_updated": tupdated,
#         })

#         jobs = get_jobs(tid)
#         if MAX_JOBS_PER_TASK and len(jobs) > MAX_JOBS_PER_TASK:
#             jobs = jobs[:MAX_JOBS_PER_TASK]
#         jobs_total += len(jobs)

#         for j in jobs:
#             jid = j.get("id")
#             jstatus = j.get("status") or j.get("state")
#             job_status_counts[jstatus] += 1

#             # Assignee can be dict or id/username depending on API
#             assignee = None
#             a = j.get("assignee")
#             if isinstance(a, dict):
#                 assignee = a.get("username") or a.get("email") or a.get("id")
#             else:
#                 assignee = a

#             jcreated = parse_iso(j.get("created_date") or j.get("created"))
#             jupdated = parse_iso(j.get("updated_date") or j.get("updated"))
#             duration_hours = None
#             if jcreated and jupdated:
#                 duration_hours = round((jupdated - jcreated).total_seconds() / 3600, 2)

#             user_workload_rows.append({
#                 "project_id": pid,
#                 "project_name": pname,
#                 "task_id": tid,
#                 "job_id": jid,
#                 "assignee": assignee,
#                 "job_status": jstatus,
#                 "job_created": jcreated,
#                 "job_updated": jupdated,
#                 "job_duration_hours": duration_hours,
#             })

#             # ---- Annotation stats (label distribution) ----
#             try:
#                 anns = get_job_annotations(jid)
#             except Exception as e:
#                 # Network hiccup or permissions—skip but keep going
#                 anns = {}

#             # Count labels across shapes / tracks / tags
#             for key in ("shapes", "tracks", "tags"):
#                 for item in anns.get(key, []) or []:
#                     lab_id = item.get("label_id")
#                     if lab_id is not None:
#                         label_usage_counter[lab_id] += 1

#     tasks_total = sum(task_status_counts.values())
#     tasks_completed = task_status_counts.get("completed", 0)
#     tasks_in_progress = (
#         task_status_counts.get("annotation", 0)
#         + task_status_counts.get("validation", 0)
#         + task_status_counts.get("in_progress", 0)
#     )
#     tasks_other = tasks_total - tasks_completed - tasks_in_progress
#     jobs_completed = job_status_counts.get("completed", 0)
#     jobs_in_progress = (
#         job_status_counts.get("annotation", 0)
#         + job_status_counts.get("validation", 0)
#         + job_status_counts.get("in_progress", 0)
#     )
#     progress_percent = round(100.0 * tasks_completed / tasks_total, 2) if tasks_total else 0.0

#     proj_rows.append({
#         "project_id": pid,
#         "project_name": pname,
#         "owner": powner,
#         "created": pcreated_dt,
#         "updated": pupdated_dt,
#         "labels_defined_count": len(label_names),
#         "labels_defined": "; ".join(sorted(label_names)),
#         "tasks_total": tasks_total,
#         "tasks_completed": tasks_completed,
#         "tasks_in_progress": tasks_in_progress,
#         "tasks_other": tasks_other,
#         "jobs_total": jobs_total,
#         "jobs_completed": jobs_completed,
#         "jobs_in_progress": jobs_in_progress,
#         "progress_percent": progress_percent,
#     })

#     # Materialize label usage rows (map label_id → label_name if we have it)
#     lab_id_to_name = {r["label_id"]: r["label_name"] for r in label_catalog_rows if r["project_id"] == pid}
#     for lab_id, count in label_usage_counter.items():
#         label_usage_rows.append({
#             "project_id": pid,
#             "project_name": pname,
#             "label_id": lab_id,
#             "label_name": lab_id_to_name.get(lab_id),
#             "annotation_count": int(count),
#         })

# # ---------------------------
# # External metadata links (placeholder)
# # ---------------------------
# # If a file 'metadata_links.csv' exists with columns:
# #   project_id, external_metadata_url
# # we'll pass it through so you can join in Metabase later.
# meta_links_path = "metadata_links.csv"
# if not os.path.exists(meta_links_path):
#     # create an empty template you can fill later
#     pd.DataFrame([{"project_id": "", "external_metadata_url": ""}]).to_csv(meta_links_path, index=False)

# # ---------------------------
# # Write CSVs
# # ---------------------------
# pd.DataFrame(proj_rows).to_csv("cvat_projects_overview.csv", index=False)
# pd.DataFrame(label_catalog_rows).to_csv("cvat_labels_catalog.csv", index=False)
# pd.DataFrame(label_usage_rows).to_csv("cvat_label_usage.csv", index=False)
# pd.DataFrame(user_workload_rows).to_csv("cvat_user_workload.csv", index=False)
# pd.DataFrame(time_series_rows).to_csv("cvat_time_series.csv", index=False)

# print("Wrote:")
# for f in [
#     "cvat_projects_overview.csv",
#     "cvat_labels_catalog.csv",
#     "cvat_label_usage.csv",
#     "cvat_user_workload.csv",
#     "cvat_time_series.csv",
#     "metadata_links.csv (template if you didn’t have one)",
# ]:
#     print(" -", f)
# SHEET_URL = os.getenv("GSHEET_URL_CVAT")

# # replace df_* with your actual DataFrame names:
# write_df_to_sheet(df_projects,        SHEET_URL, "projects_overview")
# write_df_to_sheet(df_labels_catalog,  SHEET_URL, "labels_catalog")
# write_df_to_sheet(df_labels_catalog,  SHEET_URL, "label_usage")
# write_df_to_sheet(df_user_workload,   SHEET_URL, "user_workload")
# write_df_to_sheet(df_time_series,     SHEET_URL, "time_series")

##----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## New script for pushing to google sheet
##-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# === CVAT → DataFrames → CSV + Google Sheets ===
# === CVAT → DataFrames → CSV + Google Sheets ===
import os
import time
from collections import Counter
from datetime import datetime
from urllib.parse import urljoin

import requests
import pandas as pd
from dotenv import load_dotenv

# Sheets helper (make sure scripts/sheets_io.py exists in your repo)
from scripts.sheets_io import write_df_to_sheet

# ---------------------------
# Config & helpers
# ---------------------------
load_dotenv()

raw_base = (os.getenv("CVAT_BASE_URL", "") or "").strip()
if not raw_base:
    raise SystemExit("Missing CVAT_BASE_URL (e.g. https://app.cvat.ai). Set it in .env or GitHub Secrets.")
if not raw_base.startswith(("http://", "https://")):
    raw_base = "https://" + raw_base
BASE = raw_base.rstrip("/") + "/"

API_TOKEN = (os.getenv("CVAT_API_TOKEN", "") or "").strip()
if API_TOKEN.lower().startswith("token "):
    API_TOKEN = API_TOKEN[6:].strip()
if not API_TOKEN:
    raise SystemExit("Missing CVAT_API_TOKEN. Set it in .env or GitHub Secrets.")

ORG = (os.getenv("CVAT_ORG_HEADER", "") or "").strip() or None

# Optional limits
MAX_PROJECTS = int(os.getenv("ETL_MAX_PROJECTS", "0") or "0")          # 0 = unlimited
MAX_TASKS_PER_PROJ = int(os.getenv("ETL_MAX_TASKS_PER_PROJ", "0") or "0")
MAX_JOBS_PER_TASK = int(os.getenv("ETL_MAX_JOBS_PER_TASK", "0") or "0")

# Optional: skip heavy annotations on nightly runs
FETCH_ANNOTATIONS = (os.getenv("ETL_FETCH_ANNOTATIONS", "0") or "0").strip().lower() in ("1", "true", "yes")

# HTTP session
sess = requests.Session()
sess.headers.update({
    "Authorization": f"Token {API_TOKEN}",
    "Accept": "application/vnd.cvat+json",
})
if ORG:
    sess.headers["X-Organization"] = ORG
    # --- Robust retries for CVAT rate limits / hiccups ---
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Optional knobs via env (have safe defaults)
RETRY_TOTAL         = int(os.getenv("ETL_RETRY_TOTAL", "6") or "6")        # total retry attempts
RETRY_BACKOFF       = float(os.getenv("ETL_RETRY_BACKOFF", "1.0") or "1.0") # wait grows: 1s, 2s, 4s, ...
RETRY_STATUS_CODES  = [int(s) for s in (os.getenv("ETL_RETRY_STATUS", "429,500,502,503,504").split(","))]

retry = Retry(
    total=RETRY_TOTAL,
    backoff_factor=RETRY_BACKOFF,
    status_forcelist=RETRY_STATUS_CODES,
    allowed_methods=["GET"],   # our script uses GETs for reads
    raise_on_status=False,     # let requests hand the response back for our handling
)

adapter = HTTPAdapter(max_retries=retry)
sess.mount("http://", adapter)
sess.mount("https://", adapter)
# --- end retries setup ---
def get_all(endpoint, params=None, sleep=0.0):
    """Fetch all pages from DRF-style endpoints; also works for list/non-paginated."""
    url = urljoin(BASE, endpoint.lstrip("/"))
    out = []
    while url:
        # r = sess.get(url, params=params, timeout=120)
        # r.raise_for_status()
        r = sess.get(url, params=params, timeout=120)
        # If server tells us how long to wait, respect it
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    time.sleep(float(retry_after))
                except Exception:
                    time.sleep(2)
        r.raise_for_status()
        # small pacing sleep to avoid rate limits
        if REQUEST_SLEEP:
            time.sleep(REQUEST_SLEEP)

        data = r.json()
        if isinstance(data, dict) and "results" in data:
            out.extend(data["results"])
            url = data.get("next")
            params = None
        else:
            out.extend(data if isinstance(data, list) else [data])
            url = None
        if sleep:
            time.sleep(sleep)
    return out

def get_projects():
    projs = get_all("/api/projects")
    if MAX_PROJECTS and len(projs) > MAX_PROJECTS:
        projs = projs[:MAX_PROJECTS]
    return projs

def get_tasks(project_id):
    tasks = get_all("/api/tasks", params={"project_id": project_id})
    if MAX_TASKS_PER_PROJ and len(tasks) > MAX_TASKS_PER_PROJ:
        tasks = tasks[:MAX_TASKS_PER_PROJ]
    return tasks

def get_jobs(task_id):
    jobs = get_all("/api/jobs", params={"task_id": task_id})
    if MAX_JOBS_PER_TASK and len(jobs) > MAX_JOBS_PER_TASK:
        jobs = jobs[:MAX_JOBS_PER_TASK]
    return jobs

def get_project_labels(project_payload):
    """
    Try multiple ways to get labels for a project, to support different CVAT versions.
    Return a list of label dicts with keys like: id, name/title, color, type.
    """
    pid = project_payload.get("id")

    # 0) Embedded on the project payload
    embedded = project_payload.get("labels") or []
    if isinstance(embedded, list) and embedded:
        return embedded

    # 1) /api/projects/{id} (refresh payload; sometimes includes labels)
    try:
        r = sess.get(urljoin(BASE, f"/api/projects/{pid}"), timeout=60)
        if r.ok:
            pr = r.json()
            if isinstance(pr, dict):
                labels = pr.get("labels") or []
                if isinstance(labels, list) and labels:
                    return labels
    except Exception:
        pass

    # 2) /api/labels?project_id={id} (supported on many versions)
    try:
        labels = get_all("/api/labels", params={"project_id": pid})
        if isinstance(labels, list) and labels:
            return labels
    except Exception:
        pass

    # 3) /api/projects/{id}/labels (exists in some builds)
    try:
        r = sess.get(urljoin(BASE, f"/api/projects/{pid}/labels"), timeout=60)
        if r.ok:
            data = r.json()
            if isinstance(data, list) and data:
                return data
            if isinstance(data, dict) and data.get("results"):
                return data["results"]
    except Exception:
        pass

    # 4) Fallback via tasks → labels per task (merge unique by id/name/title)
    try:
        tasks = get_all("/api/tasks", params={"project_id": pid})
        seen = {}
        for t in tasks or []:
            # direct task labels if present
            tl = t.get("labels")
            if isinstance(tl, list) and tl:
                for L in tl:
                    key = L.get("id") or L.get("name") or L.get("title")
                    if key and key not in seen:
                        seen[key] = L
                continue
            # /api/labels?task_id={tid}
            try:
                tlabs = get_all("/api/labels", params={"task_id": t.get("id")})
                for L in tlabs or []:
                    key = L.get("id") or L.get("name") or L.get("title")
                    if key and key not in seen:
                        seen[key] = L
            except Exception:
                continue
        if seen:
            return list(seen.values())
    except Exception:
        pass

    print(f"⚠️  No labels found for project [{pid}] {project_payload.get('name')}")
    return []

def get_job_annotations(job_id):
    """Pull annotations for a job. JSON keys: shapes, tracks, tags (each item has label_id)."""
    url = urljoin(BASE, f"/api/jobs/{job_id}/annotations")
    r = sess.get(url, timeout=300)
    r.raise_for_status()
    return r.json()

def parse_iso(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

# ---------------------------
# ETL
# ---------------------------
projects_raw = get_projects()

proj_rows = []
label_catalog_rows = []
label_usage_rows = []
user_workload_rows = []
time_series_rows = []

for p in projects_raw:
    pid = p.get("id")
    pname = p.get("name")
    powner = p.get("owner", {}).get("username") if isinstance(p.get("owner"), dict) else p.get("owner")
    pcreated = p.get("created_date") or p.get("created")
    pupdated = p.get("updated_date") or p.get("updated")
    pcreated_dt = parse_iso(pcreated)
    pupdated_dt = parse_iso(pupdated)

    # Labels (catalog)
    labels = get_project_labels(p)
    label_names = []
    for L in (labels or []):
        if not isinstance(L, dict):
            continue
        label_name = L.get("name") or L.get("title")
        label_catalog_rows.append({
            "project_id": pid,
            "project_name": pname,
            "label_id": L.get("id"),
            "label_name": label_name,
            "label_color": L.get("color"),
            "label_type": L.get("type"),  # may be None on some versions
        })
        if label_name:
            label_names.append(label_name)

    # Tasks & Jobs
    tasks = get_tasks(pid)

    task_status_counts = Counter()
    job_status_counts = Counter()
    jobs_total = 0

    # Label usage counter for this project
    label_usage_counter = Counter()

    for t in tasks:
        tid = t.get("id")
        tname = t.get("name")
        tstatus = t.get("status") or t.get("state")
        task_status_counts[tstatus] += 1

        tcreated = parse_iso(t.get("created_date") or t.get("created"))
        tupdated = parse_iso(t.get("updated_date") or t.get("updated"))
        time_series_rows.append({
            "project_id": pid,
            "project_name": pname,
            "task_id": tid,
            "task_name": tname,
            "task_status": tstatus,
            "task_created": tcreated,
            "task_updated": tupdated,
        })

        jobs = get_jobs(tid)
        jobs_total += len(jobs)

        for j in jobs:
            jid = j.get("id")
            jstatus = j.get("status") or j.get("state")
            job_status_counts[jstatus] += 1

            # Assignee might be dict or scalar
            a = j.get("assignee")
            assignee = a.get("username") or a.get("email") or a.get("id") if isinstance(a, dict) else a

            jcreated = parse_iso(j.get("created_date") or j.get("created"))
            jupdated = parse_iso(j.get("updated_date") or j.get("updated"))
            duration_hours = None
            if jcreated and jupdated:
                duration_hours = round((jupdated - jcreated).total_seconds() / 3600, 2)

            user_workload_rows.append({
                "project_id": pid,
                "project_name": pname,
                "task_id": tid,
                "job_id": jid,
                "assignee": assignee,
                "job_status": jstatus,
                "job_created": jcreated,
                "job_updated": jupdated,
                "job_duration_hours": duration_hours,
            })

            # Annotation stats → label usage (optional)
            anns = {}
            if FETCH_ANNOTATIONS:
                try:
                    anns = get_job_annotations(jid)
                except Exception:
                    anns = {}

            for key in ("shapes", "tracks", "tags"):
                for item in (anns.get(key, []) or []):
                    lab_id = item.get("label_id")
                    if lab_id is not None:
                        label_usage_counter[lab_id] += 1

    # Project-level summary
    tasks_total = sum(task_status_counts.values())
    tasks_completed = task_status_counts.get("completed", 0)
    tasks_in_progress = (
        task_status_counts.get("annotation", 0)
        + task_status_counts.get("validation", 0)
        + task_status_counts.get("in_progress", 0)
    )
    tasks_other = tasks_total - tasks_completed - tasks_in_progress
    jobs_completed = job_status_counts.get("completed", 0)
    jobs_in_progress = (
        job_status_counts.get("annotation", 0)
        + job_status_counts.get("validation", 0)
        + job_status_counts.get("in_progress", 0)
    )
    progress_percent = round(100.0 * tasks_completed / tasks_total, 2) if tasks_total else 0.0

    proj_rows.append({
        "project_id": pid,
        "project_name": pname,
        "owner": powner,
        "created": pcreated_dt,
        "updated": pupdated_dt,
        "labels_defined_count": len(label_names),
        "labels_defined": "; ".join(sorted(label_names)),
        "tasks_total": tasks_total,
        "tasks_completed": tasks_completed,
        "tasks_in_progress": tasks_in_progress,
        "tasks_other": tasks_other,
        "jobs_total": jobs_total,
        "jobs_completed": jobs_completed,
        "jobs_in_progress": jobs_in_progress,
        "progress_percent": progress_percent,
    })

    # Materialize label usage rows (map id → name)
    lab_id_to_name = {r["label_id"]: r["label_name"] for r in label_catalog_rows if r["project_id"] == pid}
    for lab_id, count in label_usage_counter.items():
        label_usage_rows.append({
            "project_id": pid,
            "project_name": pname,
            "label_id": lab_id,
            "label_name": lab_id_to_name.get(lab_id),
            "annotation_count": int(count),
        })

# ---------------------------
# External metadata links (placeholder)
# ---------------------------
meta_links_path = "metadata_links.csv"
if not os.path.exists(meta_links_path):
    pd.DataFrame([{"project_id": "", "external_metadata_url": ""}]).to_csv(meta_links_path, index=False)

# ---------------------------
# Build DataFrames
# ---------------------------
df_projects       = pd.DataFrame(proj_rows)
df_labels_catalog = pd.DataFrame(label_catalog_rows)
df_label_usage    = pd.DataFrame(label_usage_rows)
df_user_workload  = pd.DataFrame(user_workload_rows)
df_time_series    = pd.DataFrame(time_series_rows)

# ---------------------------
# (Optional) write local CSVs
# ---------------------------
df_projects.to_csv("cvat_projects_overview.csv", index=False)
df_labels_catalog.to_csv("cvat_labels_catalog.csv", index=False)
df_label_usage.to_csv("cvat_label_usage.csv", index=False)
df_user_workload.to_csv("cvat_user_workload.csv", index=False)
df_time_series.to_csv("cvat_time_series.csv", index=False)

print("Wrote local CSVs:")
for f in [
    "cvat_projects_overview.csv",
    "cvat_labels_catalog.csv",
    "cvat_label_usage.csv",
    "cvat_user_workload.csv",
    "cvat_time_series.csv",
    "metadata_links.csv (template if you didn’t have one)",
]:
    print(" -", f)

# ---------------------------
# Push to Google Sheets
# ---------------------------
SHEET_URL = os.getenv("GSHEET_URL_CVAT")
if SHEET_URL:
    try:
        write_df_to_sheet(df_projects,       SHEET_URL, "projects_overview")
        write_df_to_sheet(df_labels_catalog, SHEET_URL, "labels_catalog")
        write_df_to_sheet(df_label_usage,    SHEET_URL, "label_usage")
        write_df_to_sheet(df_user_workload,  SHEET_URL, "user_workload")
        write_df_to_sheet(df_time_series,    SHEET_URL, "time_series")
        print("Updated Google Sheet tabs: projects_overview, labels_catalog, label_usage, user_workload, time_series")
    except Exception as e:
        print("⚠️ Google Sheets upload failed:", e)
        print("Tip: ensure GSHEET_URL_CVAT is set and the Sheet is shared with the service account (Editor).")
else:
    print("GSHEET_URL_CVAT not set; skipped Google Sheets upload.")
