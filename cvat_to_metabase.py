# # WORKS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
## PULLS THE PROJECT BUT LABELS ARE MISSING
# import os
# import time
# import requests
# import pandas as pd
# from urllib.parse import urljoin
# from dotenv import load_dotenv

# # ---------- Config ----------
# load_dotenv()

# raw_base = os.getenv("CVAT_BASE_URL", "").strip()
# if not raw_base:
#     raise SystemExit("Missing CVAT_BASE_URL (e.g. https://app.cvat.ai). Set it in .env or PowerShell env.")
# if not raw_base.startswith(("http://", "https://")):
#     raw_base = "https://" + raw_base
# BASE = os.getenv("CVAT_BASE_URL", "").rstrip("/") + "/"

# API_TOKEN = os.getenv("CVAT_API_TOKEN")
# if not API_TOKEN:
#     raise SystemExit("Missing CVAT_API_TOKEN. Set it in .env or PowerShell env.")
# ORG = os.getenv("CVAT_ORG_HEADER")  # your 3-letter/short org id (or slug)

# # Optional Postgres
# PGHOST = os.getenv("PGHOST")
# PGPORT = os.getenv("PGPORT")
# PGDATABASE = os.getenv("PGDATABASE")
# PGUSER = os.getenv("PGUSER")
# PGPASSWORD = os.getenv("PGPASSWORD")

# WRITE_POSTGRES = all([PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD])

# # ---------- Session / Headers ----------
# # CVAT uses token auth:  Authorization: Token <key>
# # Org scoping with:      X-Organization: <org>
# sess = requests.Session()
# sess.headers.update({
#     "Authorization": f"Token {API_TOKEN}",
#     "Accept": "application/vnd.cvat+json",
# })
# if ORG:
#     sess.headers["X-Organization"] = ORG

# def get_all(endpoint, params=None, sleep=0.0):
#     """
#     Fetches all pages from a DRF-style paginated endpoint.
#     Expects JSON with keys: count, next, previous, results
#     """
#     url = urljoin(BASE, endpoint.lstrip("/"))
#     out = []
#     while url:
#         r = sess.get(url, params=params, timeout=60)
#         r.raise_for_status()
#         data = r.json()
#         if isinstance(data, dict) and "results" in data:
#             out.extend(data["results"])
#             url = data.get("next")
#             params = None  # next already carries query
#         else:
#             # Non-paginated (returns list)
#             if isinstance(data, list):
#                 out.extend(data)
#             else:
#                 out.append(data)
#             url = None
#         if sleep:
#             time.sleep(sleep)
#     return out

# def get_projects():
#     # /api/projects
#     return get_all("/api/projects")

# def get_tasks(project_id):
#     # /api/tasks?project_id=<id>
#     return get_all("/api/tasks", params={"project_id": project_id})

# def get_jobs(task_id):
#     # /api/jobs?task_id=<id>
#     return get_all("/api/jobs", params={"task_id": task_id})

# def summarize():
#     rows = []
#     projects = get_projects()
#     for p in projects:
#         pid = p.get("id")
#         pname = p.get("name")
#         powner = p.get("owner", {}).get("username") if isinstance(p.get("owner"), dict) else p.get("owner")
#         pcreated = p.get("created_date") or p.get("created")  # field name can vary
#         plabels = p.get("labels", [])
#         label_count = len(plabels) if isinstance(plabels, list) else None

#         tasks = get_tasks(pid)
#         task_count = len(tasks)
#         task_status_counts = {}
#         total_jobs = 0
#         job_status_counts = {}

#         for t in tasks:
#             t_status = t.get("status") or t.get("state")  # status/state can vary by version
#             task_status_counts[t_status] = task_status_counts.get(t_status, 0) + 1

#             # jobs per task
#             jobs = get_jobs(t.get("id"))
#             total_jobs += len(jobs)
#             for j in jobs:
#                 j_status = j.get("status") or j.get("state")
#                 job_status_counts[j_status] = job_status_counts.get(j_status, 0) + 1

#         # Simple progress proxy (completed tasks / total)
#         completed = task_status_counts.get("completed", 0)
#         progress_pct = round(100.0 * completed / task_count, 2) if task_count else 0.0

#         rows.append({
#             "project_id": pid,
#             "project_name": pname,
#             "owner": powner,
#             "created": pcreated,
#             "labels_defined": label_count,
#             "tasks_total": task_count,
#             "tasks_completed": completed,
#             "tasks_in_progress": (
#                 task_status_counts.get("annotation", 0)
#                 + task_status_counts.get("validation", 0)
#                 + task_status_counts.get("in_progress", 0)
#             ),
#             "tasks_other": sum(v for k, v in task_status_counts.items()
#                                if k not in {"completed", "annotation", "validation", "in_progress"}),
#             "jobs_total": total_jobs,
#             "jobs_completed": job_status_counts.get("completed", 0),
#             "jobs_in_progress": (
#                 job_status_counts.get("annotation", 0)
#                 + job_status_counts.get("validation", 0)
#                 + job_status_counts.get("in_progress", 0)
#             ),
#             "progress_percent": progress_pct,
#         })
#     return pd.DataFrame(rows)

# def write_csv(df, path="cvat_projects_overview.csv"):
#     df.to_csv(path, index=False)
#     print(f"Wrote {path} with {len(df)} rows")

# def write_postgres(df, table="cvat_projects_overview"):
#     import psycopg2
#     from psycopg2.extras import execute_values

#     conn = psycopg2.connect(
#         host=PGHOST, port=int(PGPORT), dbname=PGDATABASE,
#         user=PGUSER, password=PGPASSWORD
#     )
#     cols = list(df.columns)
#     with conn, conn.cursor() as cur:
#         # Create table if not exists (simple types)
#         cur.execute(f"""
#             CREATE TABLE IF NOT EXISTS {table} (
#                 project_id INTEGER PRIMARY KEY,
#                 project_name TEXT,
#                 owner TEXT,
#                 created TIMESTAMP NULL,
#                 labels_defined INTEGER NULL,
#                 tasks_total INTEGER,
#                 tasks_completed INTEGER,
#                 tasks_in_progress INTEGER,
#                 tasks_other INTEGER,
#                 jobs_total INTEGER,
#                 jobs_completed INTEGER,
#                 jobs_in_progress INTEGER,
#                 progress_percent NUMERIC
#             );
#         """)
#         # Upsert rows
#         records = [tuple(x) for x in df.itertuples(index=False, name=None)]
#         placeholders = "(" + ",".join(["%s"] * len(cols)) + ")"
#         upserts = ", ".join([f"{c}=EXCLUDED.{c}" for c in cols if c != "project_id"])
#         execute_values(
#             cur,
#             f"""
#             INSERT INTO {table} ({",".join(cols)})
#             VALUES %s
#             ON CONFLICT (project_id) DO UPDATE SET {upserts};
#             """,
#             records
#         )
#     conn.close()
#     print(f"Upserted {len(df)} rows into table {table}")

# if __name__ == "__main__":
#     df = summarize()
#     print(df.head(10))
#     write_csv(df)
#     if WRITE_POSTGRES:
#         write_postgres(df)

###-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
## A CODE WITH CHANGES TO HELP PULL ALL THE NECESSARY DATA FROM CVAT
import os, time, math, json
from collections import defaultdict, Counter
from datetime import datetime
from urllib.parse import urljoin

import requests
import pandas as pd
from dotenv import load_dotenv

# ---------------------------
# Config & helpers
# ---------------------------
load_dotenv()

raw_base = os.getenv("CVAT_BASE_URL", "").strip()
if not raw_base:
    raise SystemExit("Missing CVAT_BASE_URL (e.g. https://app.cvat.ai). Set it in .env or PowerShell env.")
if not raw_base.startswith(("http://", "https://")):
    raw_base = "https://" + raw_base
BASE = raw_base.rstrip("/") + "/"

API_TOKEN = os.getenv("CVAT_API_TOKEN")
if not API_TOKEN:
    raise SystemExit("Missing CVAT_API_TOKEN. Set it in .env or PowerShell env.")
ORG = os.getenv("CVAT_ORG_HEADER", "").strip() or None

# Optional limits for first run
MAX_PROJECTS = int(os.getenv("ETL_MAX_PROJECTS", "0") or "0")          # 0 = unlimited
MAX_TASKS_PER_PROJ = int(os.getenv("ETL_MAX_TASKS_PER_PROJ", "0") or "0")
MAX_JOBS_PER_TASK = int(os.getenv("ETL_MAX_JOBS_PER_TASK", "0") or "0")

# Session
sess = requests.Session()
sess.headers.update({
    "Authorization": f"Token {API_TOKEN}",
    "Accept": "application/vnd.cvat+json",
})
if ORG:
    sess.headers["X-Organization"] = ORG

def get_all(endpoint, params=None, sleep=0.0):
    """Fetches all pages from DRF-style paginated endpoints (or a list)."""
    url = urljoin(BASE, endpoint.lstrip("/"))
    out = []
    while url:
        r = sess.get(url, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and "results" in data:
            out.extend(data["results"])
            url = data.get("next")
            params = None
        else:
            if isinstance(data, list):
                out.extend(data)
            else:
                out.append(data)
            url = None
        if sleep:
            time.sleep(sleep)
    return out

def get_projects():
    return get_all("/api/projects")

def get_tasks(project_id):
    return get_all("/api/tasks", params={"project_id": project_id})

def get_jobs(task_id):
    return get_all("/api/jobs", params={"task_id": task_id})

def get_project_labels(project_payload):
    """Use embedded labels; if missing, try labels endpoint."""
    labels = project_payload.get("labels", []) or []
    if labels:
        return labels
    # Fallback (some deployments support filtering labels by project_id)
    try:
        return get_all("/api/labels", params={"project_id": project_payload.get("id")})
    except Exception:
        return []

def get_job_annotations(job_id):
    """
    Pull annotations for a job. This can be heavy; keep first run scoped with MAX_* envs.
    Standard JSON has keys: shapes, tracks, tags (each item has label_id).
    """
    url = urljoin(BASE, f"/api/jobs/{job_id}/annotations")
    r = sess.get(url, timeout=120)
    r.raise_for_status()
    return r.json()  # dict with 'shapes', 'tracks', 'tags'

def parse_iso(ts):
    if not ts:
        return None
    try:
        # CVAT timestamps are usually ISO 8601
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

# ---------------------------
# ETL
# ---------------------------
projects_raw = get_projects()
if MAX_PROJECTS and len(projects_raw) > MAX_PROJECTS:
    projects_raw = projects_raw[:MAX_PROJECTS]

proj_rows = []
label_catalog_rows = []
label_usage_rows = []
user_workload_rows = []
time_series_rows = []

for p in projects_raw:
    pid = p.get("id")
    pname = p.get("name")
    powner = p.get("owner", {}).get("username") if isinstance(p.get("owner"), dict) else p.get("owner")
    pcreated = p.get("created_date") or p.get("created")  # name varies
    pupdated = p.get("updated_date") or p.get("updated")  # name varies
    pcreated_dt = parse_iso(pcreated)
    pupdated_dt = parse_iso(pupdated)

    # Labels present (catalog)
    labels = get_project_labels(p)
    label_names = []
    for L in labels or []:
        if not isinstance(L, dict):
            continue
        label_catalog_rows.append({
            "project_id": pid,
            "project_name": pname,
            "label_id": L.get("id"),
            "label_name": L.get("name"),
            "label_color": L.get("color"),
            "label_type": L.get("type"),
        })
        if L.get("name"):
            label_names.append(L["name"])

    # Tasks & jobs (progress + time metrics + workload + label distribution)
    tasks = get_tasks(pid)
    if MAX_TASKS_PER_PROJ and len(tasks) > MAX_TASKS_PER_PROJ:
        tasks = tasks[:MAX_TASKS_PER_PROJ]

    # Task status summary
    task_status_counts = Counter()
    jobs_total = 0
    job_status_counts = Counter()

    # Label usage counters for this project
    label_usage_counter = Counter()

    # User workload: jobs per assignee & status
    # We'll collect rows at the per-job level for flexibility
    for t in tasks:
        tid = t.get("id")
        tname = t.get("name")
        tstatus = t.get("status") or t.get("state")
        task_status_counts[tstatus] += 1

        tcreated = parse_iso(t.get("created_date") or t.get("created"))
        tupdated = parse_iso(t.get("updated_date") or t.get("updated"))
        # Time series table for trends
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
        if MAX_JOBS_PER_TASK and len(jobs) > MAX_JOBS_PER_TASK:
            jobs = jobs[:MAX_JOBS_PER_TASK]
        jobs_total += len(jobs)

        for j in jobs:
            jid = j.get("id")
            jstatus = j.get("status") or j.get("state")
            job_status_counts[jstatus] += 1

            # Assignee can be dict or id/username depending on API
            assignee = None
            a = j.get("assignee")
            if isinstance(a, dict):
                assignee = a.get("username") or a.get("email") or a.get("id")
            else:
                assignee = a

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

            # ---- Annotation stats (label distribution) ----
            try:
                anns = get_job_annotations(jid)
            except Exception as e:
                # Network hiccup or permissions—skip but keep going
                anns = {}

            # Count labels across shapes / tracks / tags
            for key in ("shapes", "tracks", "tags"):
                for item in anns.get(key, []) or []:
                    lab_id = item.get("label_id")
                    if lab_id is not None:
                        label_usage_counter[lab_id] += 1

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

    # Materialize label usage rows (map label_id → label_name if we have it)
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
# If a file 'metadata_links.csv' exists with columns:
#   project_id, external_metadata_url
# we'll pass it through so you can join in Metabase later.
meta_links_path = "metadata_links.csv"
if not os.path.exists(meta_links_path):
    # create an empty template you can fill later
    pd.DataFrame([{"project_id": "", "external_metadata_url": ""}]).to_csv(meta_links_path, index=False)

# ---------------------------
# Write CSVs
# ---------------------------
pd.DataFrame(proj_rows).to_csv("cvat_projects_overview.csv", index=False)
pd.DataFrame(label_catalog_rows).to_csv("cvat_labels_catalog.csv", index=False)
pd.DataFrame(label_usage_rows).to_csv("cvat_label_usage.csv", index=False)
pd.DataFrame(user_workload_rows).to_csv("cvat_user_workload.csv", index=False)
pd.DataFrame(time_series_rows).to_csv("cvat_time_series.csv", index=False)

print("Wrote:")
for f in [
    "cvat_projects_overview.csv",
    "cvat_labels_catalog.csv",
    "cvat_label_usage.csv",
    "cvat_user_workload.csv",
    "cvat_time_series.csv",
    "metadata_links.csv (template if you didn’t have one)",
]:
    print(" -", f)
