from fastapi import APIRouter, FastAPI
from adms_wrapper.core.db_queries import get_attendences, get_device_logs, get_finger_log, get_migrations, get_users
from adms_wrapper.__main__ import process_attendance_summary, main
from fastapi.responses import JSONResponse

router = APIRouter()

@router.get("/attendences")
def attendences():
    """Return all attendence records as JSON."""
    return get_attendences() or []

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

@router.get("/attendance-summary/{date_range}/{times}")
def attendance_summary(date_range: str, times: str) -> None:
    """Return the processed attendance summary as JSON."""
    attendences = get_attendences() or []
    summary_df = process_attendance_summary(attendences)
    if summary_df is not None:
        return summary_df.to_dict(orient="records")
    return []

@router.get("/create_xlsx")
def create_xlsx():
    return main()

# FastAPI app for quick testing (optional, can be removed if only using as router)
app = FastAPI()
app.include_router(router, prefix="/api")
