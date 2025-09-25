# cvat_list_orgs.py
from getpass import getpass
from cvat_sdk.api_client import ApiClient, Configuration

HOST = "https://app.cvat.ai"
USER = "shekinah-jeanelle"
PWD  = getpass("CVAT password:Grenade@123")
with ApiClient(Configuration(host=HOST, username=USER, password=PWD)) as api:
    orgs, _ = api.organizations_api.list()  # list organizations
    print("Organizations:")
    for o in orgs:
        print(f"  id={o.id} slug={getattr(o, 'slug', None)} name={o.name}")
        # Count projects in this org
        page, total = 1, 0
        while True:
            data, _ = api.projects_api.list(page=page, page_size=100,
                                            org_id=o.id, org=getattr(o, 'slug', None),
                                            x_organization=getattr(o, 'slug', None))
            total += len(data.results or [])
            if not data.next: break
            page += 1
        print(f"    projects: {total}")
