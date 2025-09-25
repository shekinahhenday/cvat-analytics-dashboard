import os
import pandas as pd
import requests
from urllib.parse import urljoin

# ENV (same as your other scripts)
BASE = (os.getenv("CVAT_BASE_URL") or "").strip()
TOKEN = (os.getenv("CVAT_API_TOKEN") or "").strip()
ORG   = (os.getenv("CVAT_ORG_HEADER") or "").strip()

if not BASE:
    raise SystemExit("Missing CVAT_BASE_URL (e.g. https://app.cvat.ai)")
if not BASE.startswith(("http://","https://")):
    BASE = "https://" + BASE
BASE = BASE.rstrip("/") + "/"
if not TOKEN:
    raise SystemExit("Missing CVAT_API_TOKEN")

sess = requests.Session()
sess.headers.update({"Authorization": f"Token {TOKEN}", "Accept":"application/vnd.cvat+json"})
if ORG:
    sess.headers["X-Organization"] = ORG

def get_all(endpoint, params=None):
    url = urljoin(BASE, endpoint.lstrip("/"))
    out = []
    while url:
        r = sess.get(url, params=params, timeout=120)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and "results" in data:
            out.extend(data["results"])
            url = data.get("next"); params = None
        else:
            out.extend(data if isinstance(data, list) else [data])
            url = None
    return out

# 1) Build label map from /api/labels
labels = get_all("/api/labels")
# keep only labels that belong to a project (some could be task-only)
labels = [L for L in labels if L.get("project_id") is not None]
label_map = {(L["project_id"], L["id"]): L.get("name") for L in labels if "id" in L and "name" in L}

# 2) Create/refresh labels catalog CSV (one row per (project,label))
catalog_rows = []
for L in labels:
    catalog_rows.append({
        "project_id": L.get("project_id"),
        "label_id": L.get("id"),
        "label_name": L.get("name"),
        "label_color": L.get("color"),
        "label_type": L.get("type"),
    })
catalog_df = pd.DataFrame(catalog_rows)
catalog_df.to_csv("cvat_labels_catalog.csv", index=False)
print(f"Wrote cvat_labels_catalog.csv with {len(catalog_df)} rows")

# 3) Enrich usage CSV with label_name (if file exists)
usage_path = "cvat_label_usage.csv"
if os.path.exists(usage_path):
    usage = pd.read_csv(usage_path)
    def lookup(row):
        return label_map.get((int(row["project_id"]), int(row["label_id"])))
    usage["label_name"] = usage.apply(lookup, axis=1)
    usage.to_csv(usage_path, index=False)
    print(f"Updated {usage_path} with label_name values")
else:
    print("cvat_label_usage.csv not found here; only wrote catalog.")
