import gspread
from oauth2client.service_account import ServiceAccountCredentials

def _client(creds_path="credentials.json"):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    return gspread.authorize(creds)

def write_df_to_sheet(df, sheet_url, tab_name="Sheet1"):
    gc = _client()
    sh = gc.open_by_url(sheet_url)
    try:
        ws = sh.worksheet(tab_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        rows = max(len(df) + 10, 100)
        cols = max(len(df.columns) + 5, 26)
        ws = sh.add_worksheet(title=tab_name, rows=str(rows), cols=str(cols))
    data = [list(df.columns)] + df.astype(object).where(df.notna(), "").values.tolist()
    ws.update("A1", data)
