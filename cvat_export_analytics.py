# cvat_export_analytics.py — org header injected on every request + exports
import argparse, io, json, os, re, sys, time, zipfile
from getpass import getpass
from urllib.parse import urlparse, parse_qsl
from collections import Counter, defaultdict

import numpy as np
import pandas as pd
from tqdm import tqdm

from cvat_sdk.api_client import ApiClient, Configuration, exceptions

FORMAT_NAME = "Datumaro 1.0"

def parse_args():
    p = argparse.ArgumentParser(description="Export CVAT Projects/Tasks to Datumaro and compute analytics.json")
    p.add_argument("--host", required=True)
    # Auth (choose one)
    p.add_argument("--username")
    p.add_argument("--password")
    p.add_argument("--token")
    # Scope
    p.add_argument("--org-slug", help="Organization slug (shared workspace)")
    p.add_argument("--page-size", type=int, default=100)
    # Selection
    p.add_argument("--projects", default="all", help='IDs/names comma-separated, or "all"')
    p.add_argument("--include-tasks", action="store_true", help="Also export standalone Tasks (not in a Project)")
    p.add_argument("--with-images", action="store_true", help="Include images in exports (larger zips)")
    # Output
    p.add_argument("--out", default="analytics.json")
    p.add_argument("--save-zips", action="store_true")
    return p.parse_args()

def login_client(host, username=None, password=None, token=None) -> ApiClient:
    if token:
        cfg = Configuration(host=host)
        cfg.api_key = {"tokenAuth": token}
        cfg.api_key_prefix = {"tokenAuth": "Token"}
        return ApiClient(cfg)
    if not username:
        raise RuntimeError("Provide --username or use --token")
    pwd = password or getpass("CVAT password: ")
    cfg = Configuration(host=host, username=username, password=pwd)
    return ApiClient(cfg)

def apply_org_header(api, org_slug: str | None):
    """Force X-Organization header for all requests, across SDK variants."""
    if not org_slug:
        return

    set_any = False

    # Preferred helpers (may exist in some SDK builds)
    try:
        api.set_default_header("X-Organization", org_slug)
        print("[debug] set X-Organization via api.set_default_header")
        set_any = True
    except Exception:
        pass

    # Known dicts on some variants
    for attr in ("default_headers",):
        try:
            hdrs = getattr(api, attr)
            if isinstance(hdrs, dict):
                hdrs["X-Organization"] = org_slug
                print(f"[debug] set X-Organization via api.{attr}")
                set_any = True
        except Exception:
            pass

    try:
        conf_hdrs = getattr(api, "configuration").default_headers  # type: ignore[attr-defined]
        if isinstance(conf_hdrs, dict):
            conf_hdrs["X-Organization"] = org_slug
            print("[debug] set X-Organization via api.configuration.default_headers")
            set_any = True
    except Exception:
        pass

    # Hard mode: wrap call_api so every request gets the header
    try:
        orig_call_api = api.call_api

        def call_api_with_org(*args, **kwargs):
            headers = kwargs.get("header_params") or {}
            headers = dict(headers)
            headers["X-Organization"] = org_slug
            kwargs["header_params"] = headers
            return orig_call_api(*args, **kwargs)

        api.call_api = call_api_with_org  # type: ignore[assignment]
        print("[debug] call_api wrapper installed (injects X-Organization)")
        set_any = True
    except Exception:
        pass

    if not set_any:
        print("[warn] could not set org header on client; exports may default to personal workspace")

def list_projects(api: ApiClient, org_slug=None, page_size=100):
    """List projects; use only the 'org' query param (header comes from client)."""
    results, page = [], 1
    while True:
        kwargs = {"page": page, "page_size": page_size}
        if org_slug:
            kwargs["org"] = org_slug
        data, _ = api.projects_api.list(**kwargs)
        results.extend(data.results or [])
        if not data.next:
            break
        page += 1
    return results

def list_tasks(api: ApiClient, org_slug=None, page_size=100, project_id=None):
    results, page = [], 1
    while True:
        kwargs = {"page": page, "page_size": page_size}
        if org_slug:
            kwargs["org"] = org_slug
        if project_id is not None:
            kwargs["project_id"] = project_id
        data, _ = api.tasks_api.list(**kwargs)
        results.extend(data.results or [])
        if not data.next:
            break
        page += 1
    return results

def select_ids_by_name(objs, selector: str):
    if selector.strip().lower() == "all":
        return {o.id for o in objs}
    by_name = {o.name.lower(): o.id for o in objs if getattr(o, "name", None)}
    out = set()
    for tok in [s.strip() for s in selector.split(",") if s.strip()]:
        if re.fullmatch(r"\d+", tok):
            out.add(int(tok))
        else:
            pid = by_name.get(tok.lower())
            if pid is None:
                print(f"[warn] Not found by name: {tok}")
            else:
                out.add(pid)
    return out

# ------------------ EXPORTS (no org/org_id/x_organization kwargs here) ------------------
def export_project_zip(api: ApiClient, project_id: int, save_images=False) -> bytes:
    _, resp = api.projects_api.create_dataset_export(
        id=project_id, format=FORMAT_NAME, save_images=save_images, _parse_response=False
    )
    rq_id = json.loads(resp.data).get("rq_id")
    if not rq_id:
        raise RuntimeError("No rq_id from export")
    while True:
        req, _ = api.requests_api.retrieve(rq_id)
        if req.status.value == "finished":
            break
        if req.status.value == "failed":
            raise RuntimeError(f"Export failed: {req.message}")
        time.sleep(0.5)
    parsed = urlparse(req.result_url)
    _, dl = api.call_api(parsed.path, method="GET", query_params=parse_qsl(parsed.query),
                         auth_settings=api.configuration.auth_settings(), _parse_response=False)
    if dl.status != 200:
        raise RuntimeError(f"Download HTTP {dl.status}")
    return dl.data

def export_task_zip(api: ApiClient, task_id: int, save_images=False) -> bytes:
    _, resp = api.tasks_api.create_dataset_export(
        id=task_id, format=FORMAT_NAME, save_images=save_images, _parse_response=False
    )
    rq_id = json.loads(resp.data).get("rq_id")
    if not rq_id:
        raise RuntimeError("No rq_id from export")
    while True:
        req, _ = api.requests_api.retrieve(rq_id)
        if req.status.value == "finished":
            break
        if req.status.value == "failed":
            raise RuntimeError(f"Export failed: {req.message}")
        time.sleep(0.5)
    parsed = urlparse(req.result_url)
    _, dl = api.call_api(parsed.path, method="GET", query_params=parse_qsl(parsed.query),
                         auth_settings=api.configuration.auth_settings(), _parse_response=False)
    if dl.status != 200:
        raise RuntimeError(f"Download HTTP {dl.status}")
    return dl.data
# ---------------------------------------------------------------------------------------

def load_datumaro_from_zip(zip_bytes: bytes):
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    ann_files = [n for n in zf.namelist() if n.startswith("annotations/") and n.endswith(".json")]
    subsets = {}
    for path in ann_files:
        subset = os.path.splitext(os.path.basename(path))[0]
        with zf.open(path) as f:
            subsets[subset] = json.load(f)
    return subsets

def label_map_from_categories(categories_obj):
    labels = categories_obj.get("label", {}).get("labels", [])
    return {i: lab["name"] for i, lab in enumerate(labels)}

def compute_metrics_for_subset(subset_json):
    cats = subset_json.get("categories", {})
    items = subset_json.get("items", [])
    id2label = label_map_from_categories(cats)

    label_counts = Counter()
    type_counts = Counter()
    per_item_counts = []
    cooccur = Counter()
    zero_area_boxes = 0
    size_buckets = defaultdict(lambda: Counter())
    resolutions = Counter()

    for item in items:
        anns = item.get("annotations", []) or []
        per_item_counts.append(len(anns))

        H = W = None
        img = item.get("image") or {}
        if isinstance(img, dict) and "size" in img and isinstance(img["size"], list) and len(img["size"]) == 2:
            H, W = img["size"][0], img["size"][1]
            if H and W:
                resolutions[f"{W}x{H}"] += 1

        labels_in_item = set()
        for a in anns:
            t = a.get("type")
            type_counts[t] += 1
            lname = id2label.get(a.get("label_id"))
            if lname:
                label_counts[lname] += 1
                labels_in_item.add(lname)

            if t == "bbox":
                x, y, w, h = a.get("bbox", [0, 0, 0, 0])
                if w <= 0 or h <= 0:
                    zero_area_boxes += 1
                if W and H and w > 0 and h > 0 and lname:
                    frac = (w * h) / float(W * H)
                    bucket = "small" if frac < 0.05 else ("medium" if frac < 0.2 else "large")
                    size_buckets[lname][bucket] += 1

        lbls = sorted(labels_in_item)
        for i in range(len(lbls)):
            for j in range(i + 1, len(lbls)):
                cooccur[(lbls[i], lbls[j])] += 1

    arr = np.array(per_item_counts) if per_item_counts else np.array([0])
    return {
        "instances_per_item": {
            "mean": float(arr.mean()),
            "median": float(np.median(arr)),
            "p90": float(np.percentile(arr, 90)),
            "max": int(arr.max()),
        },
        "label_distribution": dict(label_counts),
        "type_distribution": dict(type_counts),
        "bbox_size_buckets": {k: dict(v) for k, v in size_buckets.items()},
        "geometry_issues": {"zero_area_boxes": int(zero_area_boxes)},
        "resolutions": dict(resolutions),
        "cooccurrence": [{"a": a, "b": b, "count": c} for (a, b), c in cooccur.items()],
    }

def merge_sum(dst, src):
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            merge_sum(dst[k], v)
        elif isinstance(v, (int, float)) and isinstance(dst.get(k), (int, float)):
            dst[k] += v
        else:
            dst[k] = json.loads(json.dumps(v))
    return dst

def main():
    a = parse_args()
    print(f"[info] host={a.host} org_slug={a.org_slug!r} page_size={a.page_size}")
    print(f"[info] auth={'token' if a.token else 'basic'}")

    with login_client(a.host, a.username, a.password, a.token) as api:
        apply_org_header(api, a.org_slug)

        # Debug: show what header is active (wrapper handles the rest)
        try:
            conf_hdr = getattr(api, "configuration").default_headers.get("X-Organization")
        except Exception:
            conf_hdr = None
        print(f"[debug] config.default_headers[X-Organization] = {conf_hdr!r}")

        print("[info] Listing projects ...")
        projs = list_projects(api, org_slug=a.org_slug, page_size=a.page_size)
        print(f"[info] Found {len(projs)} project(s).")
        chosen_proj_ids = select_ids_by_name(projs, a.projects)
        print(f"[info] Will export {len(chosen_proj_ids)} project(s).")

        standalone_tasks = []
        if a.include_tasks:
            print("[info] Listing standalone tasks ...")
            all_tasks = list_tasks(api, org_slug=a.org_slug, page_size=a.page_size)
            standalone_tasks = [t for t in all_tasks if getattr(t, "project_id", None) in (None, 0)]
            print(f"[info] Found {len(standalone_tasks)} standalone task(s).")

        os.makedirs("raw_exports", exist_ok=True)
        result = {"host": a.host, "generated_at": pd.Timestamp.utcnow().isoformat(), "datasets": {}, "global": {}}

        # Export each project (fallback: merge all tasks under the project)
        for p in tqdm(projs, desc="Exporting projects"):
            if p.id not in chosen_proj_ids:
                continue
            display = f"Project: {p.name}"
            subsets = None
            try:
                zbytes = export_project_zip(api, p.id, save_images=a.with_images)
                subsets = load_datumaro_from_zip(zbytes)
                if a.save_zips:
                    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", f"project_{p.name}")
                    with open(os.path.join("raw_exports", f"{safe}.zip"), "wb") as f:
                        f.write(zbytes)
            except Exception as e:
                print(f"[warn] project export failed for {p.id} ({p.name}): {e}")
                try:
                    ptasks = list_tasks(api, org_slug=a.org_slug, page_size=a.page_size, project_id=p.id)
                    merged = {}
                    for t in ptasks:
                        try:
                            tz = export_task_zip(api, t.id, save_images=a.with_images)
                            ts = load_datumaro_from_zip(tz)
                            for sname, sj in ts.items():
                                merged.setdefault(sname, {"items": [], "categories": sj.get("categories", {})})
                                merged[sname]["items"].extend(sj.get("items", []))
                            if a.save_zips:
                                safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", f"task_{t.name}")
                                with open(os.path.join("raw_exports", f"{safe}.zip"), "wb") as f:
                                    f.write(tz)
                        except Exception as te:
                            print(f"[warn] task export failed for {t.id} ({t.name}): {te}")
                    if merged:
                        subsets = {s: {"categories": b.get("categories", {}), "items": b.get("items", [])}
                                   for s, b in merged.items()}
                except Exception as e2:
                    print(f"[error] project fallback failed for {p.id} ({p.name}): {e2}")

            if not subsets:
                continue

            ds = {"by_subset": {}, "aggregate": {}}
            for sname, sj in subsets.items():
                ds["by_subset"][sname] = compute_metrics_for_subset(sj)
            agg = {}
            for s in ds["by_subset"].values():
                merge_sum(agg, s)
            ds["aggregate"] = agg
            result["datasets"][display] = ds

        # Standalone tasks (optional)
        for t in tqdm(standalone_tasks, desc="Exporting standalone tasks"):
            display = f"Task: {t.name}"
            try:
                tz = export_task_zip(api, t.id, save_images=a.with_images)
            except Exception as e:
                print(f"[warn] standalone task export failed for {t.id} ({t.name}): {e}")
                continue
            if a.save_zips:
                safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", display.replace(": ", "_"))
                with open(os.path.join("raw_exports", f"{safe}.zip"), "wb") as f:
                    f.write(tz)
            subsets = load_datumaro_from_zip(tz)
            ds = {"by_subset": {}, "aggregate": {}}
            for sname, sj in subsets.items():
                ds["by_subset"][sname] = compute_metrics_for_subset(sj)
            agg = {}
            for s in ds["by_subset"].values():
                merge_sum(agg, s)
            ds["aggregate"] = agg
            result["datasets"][display] = ds

        # Global roll-up
        glob = {}
        for ds in result["datasets"].values():
            merge_sum(glob, ds["aggregate"])
        result["global"] = glob

        with open(a.out, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)
        print(f"\n✅ Wrote {os.path.abspath(a.out)}")
        if a.save_zips:
            print(f"📦 Saved raw exports under {os.path.abspath('raw_exports')}")

if __name__ == "__main__":
    try:
        main()
    except exceptions.UnauthorizedException:
        print("\n❌ Unauthorized: check email/password or use --token.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
