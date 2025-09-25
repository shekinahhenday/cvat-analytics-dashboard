import os, time
from collections import Counter, defaultdict
from urllib.parse import urljoin
import requests
import pandas as pd

# ----- Env / config -----
BASE = (os.getenv("CVAT_BASE_URL", "").strip() or "")
if not BASE:
    raise SystemExit("Missing CVAT_BASE_URL (e.g. https://app.cvat.ai)")
if not BASE.startswith(("http://","https://")):
    BASE = "https://" + BASE
BASE = BASE.rstrip("/") + "/"

TOKEN = os.getenv("CVAT_API_TOKEN") or ""
if not TOKEN:
    raise SystemExit("Missing CVAT_API_TOKEN")
ORG = os.getenv("CVAT_ORG_HEADER") or None

MAX_PROJECTS       = int(os.getenv("ETL_MAX_PROJECTS", "0") or "0")       # 0 = unlimited
MAX_TASKS_PER_PROJ = int(os.getenv("ETL_MAX_TASKS_PER_PROJ", "0") or "0")
MAX_JOBS_PER_TASK  = int(os.getenv("ETL_MAX_JOBS_PER_TASK", "0") or "0")
MAX_RUNTIME        = int(os.getenv("ETL_MAX_RUNTIME_SEC", "0") or "0")    # 0 = no cap
PROJECT_ID_FILTER  = os.getenv("ETL_PROJECT_ID")  # e.g. "123" or exact project name

START_TS = time.monotonic()
def deadline_ok():
    return (MAX_RUNTIME == 0) or (time.monotonic() - START_TS < MAX_RUNTIME)

# ----- HTTP session with retries -----
sess = requests.Session()
sess.headers.update({"Authorization": f"Token {TOKEN}", "Accept":"application/vnd.cvat+json"})
if ORG:
    sess.headers["X-Organization"] = ORG

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
retry = Retry(total=5, backoff_factor=1.0, status_forcelist=[429,500,502,503,504], allowed_methods=["GET"])
sess.mount("http://", HTTPAdapter(max_retries=retry))
sess.mount("https://", HTTPAdapter(max_retries=retry))

def get_all(endpoint, params=None, timeout=180):
    url = urljoin(BASE, endpoint.lstrip("/"))
    out = []
    while url:
        r = sess.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and "results" in data:
            out.extend(data["results"])
            url = data.get("next"); params = None
        else:
            out.extend(data if isinstance(data, list) else [data])
            url = None
    return out

def get_projects():
    projs = get_all("/api/projects")
    if PROJECT_ID_FILTER:
        try:
            pid = int(PROJECT_ID_FILTER)
            projs = [p for p in projs if p.get("id") == pid]
        except:
            projs = [p for p in projs if str(p.get("name","")) == PROJECT_ID_FILTER]
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

def get_job_annotations(job_id):
    r = sess.get(urljoin(BASE, f"/api/jobs/{job_id}/annotations"), timeout=300)
    r.raise_for_status()
    return r.json()

def build_label_name_map():
    # Fetch ALL labels once and map (project_id, label_id) -> label_name
    labels = get_all("/api/labels")
    mp = defaultdict(dict)
    for L in labels:
        pid = L.get("project_id")
        lid = L.get("id")
        name = L.get("name")
        if pid is not None and lid is not None and name:
            mp[pid][lid] = name
    return mp

# Optional normalization to a compact set of type names
def normalize_shape_type(t: str) -> str:
    t = (t or "").lower()
    if t in {"rectangle", "bbox", "box"}: return "rectangle"
    if t in {"polygon"}: return "polygon"
    if t in {"polyline", "line"}: return "polyline"
    if t in {"points", "point"}: return "points"
    if t in {"mask", "bitmap"}: return "mask"
    if t in {"cuboid"}: return "cuboid"
    if t in {"skeleton"}: return "skeleton"
    if t in {"ellipse"}: return "ellipse"
    # for track items, sometimes we only know it's a "track"
    if t in {"track"}: return "track"
    return t or "unknown"

if __name__ == "__main__":
    print("Building label name map ...")
    label_map = build_label_name_map()

    rows = []
    projs = get_projects()
    print(f"Processing {len(projs)} projects for label usage by type...")

    for p in projs:
        if not deadline_ok():
            print("Reached max runtime; writing partial results.")
            break

        pid   = p.get("id")
        pname = p.get("name")
        tasks = get_tasks(pid)
        print(f"- Project {pid} | {pname}: {len(tasks)} tasks")

        # (label_id, shape_type) -> count
        usage = Counter()

        for t in tasks:
            if not deadline_ok(): break
            jobs = get_jobs(t.get("id"))
            for j in jobs:
                if not deadline_ok(): break
                jid = j.get("id")
                try:
                    anns = get_job_annotations(jid)
                except Exception:
                    continue

                # shapes: each has its own type (rectangle, polygon, points, mask, ...)
                for item in anns.get("shapes", []) or []:
                    lab_id = item.get("label_id")
                    shp_t  = normalize_shape_type(item.get("type"))
                    if lab_id is not None:
                        usage[(lab_id, shp_t)] += 1

                # tracks: each track has label_id; type can be derived from first shape.type if present
                for tr in anns.get("tracks", []) or []:
                    lab_id = tr.get("label_id")
                    shp_t = "track"
                    shapes = tr.get("shapes") or []
                    if shapes:
                        shp_t = normalize_shape_type(shapes[0].get("type"))
                    if lab_id is not None:
                        usage[(lab_id, shp_t)] += 1

                # tags: per-frame classification; treat as "tag"
                for tag in anns.get("tags", []) or []:
                    lab_id = tag.get("label_id")
                    if lab_id is not None:
                        usage[(lab_id, "tag")] += 1

        # emit rows for this project
        for (lab_id, shp_t), count in usage.items():
            rows.append({
                "project_id": pid,
                "project_name": pname,
                "label_id": lab_id,
                "label_name": label_map.get(pid, {}).get(lab_id),
                "shape_type": shp_t,
                "annotation_count": int(count),
            })

    df = pd.DataFrame(rows, columns=["project_id","project_name","label_id","label_name","shape_type","annotation_count"])
    out_path = "cvat_label_usage_by_type.csv"
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(df)} rows")
