# cvat_project_annotation_summaries.py
import argparse
import getpass
from cvat_sdk.api_client import ApiClient, Configuration, exceptions

def parse_args():
    p = argparse.ArgumentParser(description="List CVAT projects (personal or org context)")
    p.add_argument("--host", required=True, help="Base URL, e.g. https://app.cvat.ai")
    p.add_argument("--username", required=True, help="Login email/username")
    p.add_argument("--password", help="If omitted, you'll be prompted")
    # kept for compatibility (not used in this listing script)
    p.add_argument("--projects", default="all", help='(Unused here; kept for compatibility)')
    # Organization + pagination
    p.add_argument("--org-slug", help="Organization slug (shared workspace)")
    p.add_argument("--org-id", type=int, help="Organization ID (optional)")
    p.add_argument("--page-size", type=int, default=100, help="List pagination size")
    return p.parse_args()

def list_projects(api: ApiClient, org_id=None, org_slug=None, page_size=100):
    """Return a list of (id, name) for projects visible in the given org/personal context."""
    results, page = [], 1
    while True:
        kwargs = {
            "page": page,
            "page_size": page_size,
        }
        # Only include org params if provided (SDK rejects None)
        if org_slug:
            kwargs["org"] = org_slug
            kwargs["x_organization"] = org_slug
        if org_id is not None:
            kwargs["org_id"] = org_id

        data, _ = api.projects_api.list(**kwargs)
        results.extend(data.results or [])
        if not data.next:
            break
        page += 1
    return [(p.id, p.name) for p in results]

def main():
    args = parse_args()
    pwd = args.password or getpass.getpass("CVAT password: ")

    cfg = Configuration(host=args.host, username=args.username, password=pwd)

    try:
        with ApiClient(cfg) as api:
            items = list_projects(
                api,
                org_id=args.org_id,
                org_slug=args.org_slug,
                page_size=args.page_size,
            )

        ctx = (
            f'organization "{args.org_slug}"'
            if args.org_slug else (f"organization id {args.org_id}"
            if args.org_id is not None else "personal workspace")
        )

        print(f"\nFound {len(items)} projects in {ctx}:")
        for pid, name in items:
            print(f"  {pid}: {name}")

        if len(items) == 0:
            print(
                "\nNo projects found. If your projects are in a shared workspace, "
                "re-run with --org-slug YOUR_ORG_SLUG (shown in CVAT's top-left org switcher)."
            )

    except exceptions.UnauthorizedException:
        print("\n❌ Unauthorized: check that you used your **email** and the correct password.")
        print("If you sign in with Google/GitHub (SSO) and don't have a password set, use token auth instead.")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()
