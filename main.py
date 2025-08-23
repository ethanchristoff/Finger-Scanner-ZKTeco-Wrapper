from typing import Optional

import pandas as pd
from fastapi import APIRouter, FastAPI, Query

from adms_wrapper.__main__ import main
from adms_wrapper.core.data_processing import process_attendance_summary
from adms_wrapper.core.db_queries import get_attendences, get_device_logs, get_finger_log, get_migrations, get_users

router = APIRouter()

# Run this only for testing or troubleshooting purposes


@router.get("/attendences")
def attendences(start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"), end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format")):
    """Return all attendence records as JSON, optionally filtered by date range."""
    data = get_attendences() or []
    if start_date or end_date:
        df = pd.DataFrame(data)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            if start_date:
                # Include full start day from 00:00:00
                start_datetime = pd.to_datetime(start_date).replace(hour=0, minute=0, second=0, microsecond=0)
                df = df[df["timestamp"] >= start_datetime]
            if end_date:
                # Include full end day until 23:59:59
                end_datetime = pd.to_datetime(end_date).replace(hour=23, minute=59, second=59, microsecond=999999)
                df = df[df["timestamp"] <= end_datetime]
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
def attendance_summary(start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"), end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format")):
    """Return the processed attendance summary as JSON, optionally filtered by date range."""
    attendences = get_attendences() or []
    if start_date or end_date:
        df = pd.DataFrame(attendences)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            if start_date:
                # Include full start day from 00:00:00
                start_datetime = pd.to_datetime(start_date).replace(hour=0, minute=0, second=0, microsecond=0)
                df = df[df["timestamp"] >= start_datetime]
            if end_date:
                # Include full end day until 23:59:59
                end_datetime = pd.to_datetime(end_date).replace(hour=23, minute=59, second=59, microsecond=999999)
                df = df[df["timestamp"] <= end_datetime]
            attendences = df.to_dict(orient="records")
    summary_df = process_attendance_summary(attendences, start_date, end_date)
    if summary_df is not None:
        # Filter out Sundays from the API response
        filtered_records = []
        for record in summary_df.to_dict(orient="records"):
            day = record.get("day")
            if day and day != "Subtotal":
                try:
                    # Check if it's Sunday (weekday 6) and skip if it is
                    if pd.to_datetime(day).weekday() != 6:
                        filtered_records.append(record)
                except:
                    # If date parsing fails, include the record anyway
                    filtered_records.append(record)
            else:
                # Include non-date records like subtotals
                filtered_records.append(record)
        return filtered_records
    return []


@router.get("/create_xlsx")
def create_xlsx(start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format"), end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format")):
    """
    Generate and export the XLSX file, optionally filtered by date range.
    """
    return main(start_date, end_date)


app = FastAPI()
app.include_router(router, prefix="/api")
