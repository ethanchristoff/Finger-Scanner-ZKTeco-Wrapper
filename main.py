from fastapi import APIRouter, FastAPI, Query
from typing import Optional
from adms_wrapper.core.db_queries import get_attendences, get_device_logs, get_finger_log, get_migrations, get_users
from adms_wrapper.__main__ import process_attendance_summary, main
import pandas as pd

router = APIRouter()

@router.get("/attendences")
def attendences(
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format")
):
    """Return all attendence records as JSON, optionally filtered by date range."""
    data = get_attendences() or []
    if start_date or end_date:
        df = pd.DataFrame(data)
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            if start_date:
                df = df[df['timestamp'] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df['timestamp'] <= pd.to_datetime(end_date)]
            return df.to_dict(orient="records")
    return data

@router.get("/device-logs")
def device_logs():
    """Return all device logs as JSON."""
    return get_device_logs() or []

@router.get("/finger-logs")
def finger_logs():
    """Return all finger logs as JSON."""
    return get_finger_log() or []

@router.get("/migrations")
def migrations():
    """Return all migration logs as JSON."""
    return get_migrations() or []

@router.get("/users")
def users():
    """Return all user records as JSON."""
    return get_users() or []

@router.get("/attendance-summary")
def attendance_summary(
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format")
):
    """Return the processed attendance summary as JSON, optionally filtered by date range."""
    attendences = get_attendences() or []
    if start_date or end_date:
        df = pd.DataFrame(attendences)
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            if start_date:
                df = df[df['timestamp'] >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df['timestamp'] <= pd.to_datetime(end_date)]
            attendences = df.to_dict(orient="records")
    summary_df = process_attendance_summary(attendences)
    if summary_df is not None:
        return summary_df.to_dict(orient="records")
    return []

@router.get("/create_xlsx")
def create_xlsx(
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format")
):
    """
    Generate and export the XLSX file, optionally filtered by date range.
    """
    return main(start_date, end_date)

app = FastAPI()
app.include_router(router, prefix="/api")
