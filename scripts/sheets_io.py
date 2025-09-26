# import gspread
# from oauth2client.service_account import ServiceAccountCredentials

# def _client(creds_path="credentials.json"):
#     scope = [
#         "https://spreadsheets.google.com/feeds",
#         "https://www.googleapis.com/auth/drive",
#     ]
#     creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
#     return gspread.authorize(creds)

# def write_df_to_sheet(df, sheet_url, tab_name="Sheet1"):
#     gc = _client()
#     sh = gc.open_by_url(sheet_url)
#     try:
#         ws = sh.worksheet(tab_name)
#         ws.clear()
#     except gspread.WorksheetNotFound:
#         rows = max(len(df) + 10, 100)
#         cols = max(len(df.columns) + 5, 26)
#         ws = sh.add_worksheet(title=tab_name, rows=str(rows), cols=str(cols))
#     data = [list(df.columns)] + df.astype(object).where(df.notna(), "").values.tolist()
#     ws.update("A1", data)


# scripts/sheets_io.py
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from datetime import datetime

def _client(creds_path="credentials.json"):
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
    return gspread.authorize(creds)

def _normalize_for_sheets(df: pd.DataFrame) -> list[list]:
    """
    Convert DataFrame to a 2D list of JSON-serializable values.
    - Datetime/Timestamp -> ISO string
    - NaN/NaT -> ""
    - Other objects -> str() if needed
    """
    def norm(x):
        if x is None:
            return ""
        # pandas/py datetime types
        if isinstance(x, (pd.Timestamp, datetime)):
            return x.isoformat()
        # pandas NA / NaT
        if pd.isna(x):
            return ""
        return x
    # Ensure object dtype cells are normalized too
    df2 = df.copy()
    for col in df2.columns:
        df2[col] = df2[col].map(norm)
    rows = [list(df2.columns)] + df2.values.tolist()
    return rows

def write_df_to_sheet(df: pd.DataFrame, sheet_url: str, tab_name: str = "Sheet1"):
    gc = _client()
    sh = gc.open_by_url(sheet_url)
    try:
        ws = sh.worksheet(tab_name)
        ws.clear()
    except gspread.WorksheetNotFound:
        rows = max(len(df) + 10, 100)
        cols = max(len(df.columns) + 5, 26)
        ws = sh.add_worksheet(title=tab_name, rows=str(rows), cols=str(cols))
    data = _normalize_for_sheets(df)
    ws.update("A1", data)
