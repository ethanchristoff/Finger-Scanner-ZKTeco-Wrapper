import io
from typing import Any

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

from adms_wrapper.__main__ import process_attendance_summary
from adms_wrapper.core.db_queries import (
    get_device_branch_mappings,
    get_employee_branch_mappings,
    get_employee_designation_mappings,
    get_employee_name_mappings,
)

NOON_HOUR = 12


def map_branch(sn: Any, mappings_df: pd.DataFrame) -> str:
    """Map device serial number to branch name."""
    if pd.isna(sn) or sn is None:
        return ""
    row = mappings_df[mappings_df["serial_number"] == str(sn)]
    if not row.empty:
        return row.iloc[0]["branch_name"]
    return ""


def map_designation(emp_id: Any, designation_df: pd.DataFrame) -> str:
    """Map employee ID to designation."""
    if pd.isna(emp_id) or emp_id is None or designation_df.empty:
        return ""
    if "employee_id" not in designation_df.columns:
        return ""
    row = designation_df[designation_df["employee_id"] == str(emp_id)]
    if not row.empty:
        return row.iloc[0]["designation"]
    return ""


def map_employee_branch(emp_id: Any, employee_branch_df: pd.DataFrame) -> str:
    """Map employee ID to branch name."""
    if pd.isna(emp_id) or emp_id is None or employee_branch_df.empty:
        return ""
    if "employee_id" not in employee_branch_df.columns:
        return ""
    row = employee_branch_df[employee_branch_df["employee_id"] == str(emp_id)]
    if not row.empty:
        return row.iloc[0]["branch_name"]
    return ""
    return ""


def map_employee_name(emp_id: Any, employee_name_df: pd.DataFrame) -> str:
    """Map employee ID to employee name."""
    if pd.isna(emp_id) or emp_id is None or employee_name_df.empty:
        return ""
    if "employee_id" not in employee_name_df.columns:
        return ""
    row = employee_name_df[employee_name_df["employee_id"] == str(emp_id)]
    if not row.empty:
        return row.iloc[0]["employee_name"]
    return ""


def determine_shift_flag(start_time: Any, end_time: Any, shift_start: Any, shift_end: Any) -> str:
    """Determine shift flag based on times."""
    flag = "on time"
    try:
        # Check for late check-in - now includes exact shift start time as late
        if pd.notna(start_time) and str(start_time) != "" and str(start_time)[11:16] >= str(shift_start):
            flag = "late in"

        if pd.notna(end_time) and str(end_time) != "":
            end_time_hour = end_time.hour if hasattr(end_time, "hour") else int(str(end_time)[11:13])
            end_time_str = str(end_time)[11:16]

            # Check for late checkout - now includes exact shift end time as late
            if end_time_str >= str(shift_end) and end_time_hour < NOON_HOUR:
                flag = "late checkout"
            elif end_time_str < str(shift_end):
                flag = "early out"
    except Exception:
        pass
    return flag


def determine_no_shift_flag(end_time: Any) -> str:
    """Determine shift flag for employees with no shift assignment."""
    flag = "on time"
    try:
        if pd.notna(end_time) and str(end_time) != "":
            end_time_str = str(end_time)[11:16]

            # Check if checkout is exactly at 12:00 or later
            if end_time_str >= "12:00":
                flag = "late checkout"
            elif end_time_str < "12:00":
                flag = "early out"
    except Exception:
        pass
    return flag


def get_shift_info_with_capped(emp_id: str, work_status: str, start_time: Any, end_time: Any, shift_df: pd.DataFrame) -> tuple[str, str]:
    """Get shift information for an employee without shift_capped parameter."""
    if shift_df.empty:
        # No shift data available - use no-shift logic
        if work_status == "absent":
            return "", "absent"
        flag = determine_no_shift_flag(end_time)
        return "", flag

    shift_row = shift_df[shift_df["user_id"] == str(emp_id)]
    if shift_row.empty:
        # Employee has no assigned shift - use no-shift logic
        if work_status == "absent":
            return "", "absent"
        flag = determine_no_shift_flag(end_time)
        return "", flag

    shift_name = shift_row.iloc[0]["shift_name"]
    shift_start = shift_row.iloc[0]["shift_start"]
    shift_end = shift_row.iloc[0]["shift_end"]

    if work_status == "absent":
        return shift_name, "absent"

    flag = determine_shift_flag(start_time, end_time, shift_start, shift_end)
    return shift_name, flag


def apply_branch_mappings(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Apply device branch mappings to summary DataFrame."""
    mappings = get_device_branch_mappings() or []
    mappings_df = pd.DataFrame(mappings)

    summary_df["start_device_sn_branch"] = summary_df.apply(lambda row: map_branch(row["start_device_sn"], mappings_df) if row["work_status"] == "worked" else "", axis=1)
    summary_df["end_device_sn_branch"] = summary_df.apply(lambda row: map_branch(row["end_device_sn"], mappings_df) if row["work_status"] == "worked" else "", axis=1)
    return summary_df


def apply_designation_mappings(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Apply designation mappings to summary DataFrame."""
    designation_mappings = get_employee_designation_mappings() or []
    designation_df = pd.DataFrame(designation_mappings)
    if not summary_df.empty and "employee_id" in summary_df.columns:
        summary_df["designation"] = summary_df["employee_id"].apply(lambda emp_id: map_designation(emp_id, designation_df))
    else:
        summary_df["designation"] = ""
    return summary_df


def apply_employee_branch_mappings(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Apply employee branch mappings to summary DataFrame."""
    employee_branch_mappings = get_employee_branch_mappings() or []
    employee_branch_df = pd.DataFrame(employee_branch_mappings)
    if not summary_df.empty and "employee_id" in summary_df.columns:
        summary_df["employee_branch"] = summary_df["employee_id"].apply(lambda emp_id: map_employee_branch(emp_id, employee_branch_df))
    else:
        summary_df["employee_branch"] = ""
    return summary_df


def apply_employee_name_mappings(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Apply employee name mappings to summary DataFrame."""
    employee_name_mappings = get_employee_name_mappings() or []
    employee_name_df = pd.DataFrame(employee_name_mappings)
    if not summary_df.empty and "employee_id" in summary_df.columns:
        summary_df["employee_name"] = summary_df["employee_id"].apply(lambda emp_id: map_employee_name(emp_id, employee_name_df))
    else:
        summary_df["employee_name"] = ""
    return summary_df


def apply_shift_mappings(summary_df: pd.DataFrame, shift_mappings: list[dict[str, Any]] | None) -> pd.DataFrame:
    """Apply shift mappings to summary DataFrame."""
    shift_df = pd.DataFrame(shift_mappings) if shift_mappings else pd.DataFrame()

    summary_df["shift_name"] = ""
    summary_df["shift_flag"] = ""

    for idx, row in summary_df.iterrows():
        shift_capped = row.get("shift_capped", False)

        if shift_capped:
            shift_name, _ = get_shift_info_with_capped(row["employee_id"], row["work_status"], row["start_time"], row["end_time"], shift_df)
            flag = "shift_capped"
        else:
            shift_name, flag = get_shift_info_with_capped(row["employee_id"], row["work_status"], row["start_time"], row["end_time"], shift_df)

        summary_df.loc[idx, "shift_name"] = shift_name
        summary_df.loc[idx, "shift_flag"] = flag

    return summary_df


def create_subtotal_rows(summary_df: pd.DataFrame) -> list[dict[str, Any]]:
    """Create subtotal rows for each employee."""
    output_rows = []

    for emp_id, group in summary_df.groupby("employee_id", sort=False):
        output_rows.extend(group.to_dict(orient="records"))

        worked_group = group[group["work_status"] == "worked"].copy()
        if not worked_group.empty:
            worked_group.loc[:, "time_spent_td"] = pd.to_timedelta(worked_group["time_spent"])
            subtotal = worked_group["time_spent_td"].sum()
            subtotal_str = str(subtotal).split(".")[0]
        else:
            subtotal_str = "0:00:00"

        subtotal_row = dict.fromkeys(summary_df.columns, "")
        subtotal_row["employee_id"] = emp_id
        if not group.empty:
            subtotal_row["designation"] = group.iloc[0]["designation"]
        subtotal_row["day"] = "Subtotal"
        subtotal_row["time_spent"] = subtotal_str
        subtotal_row["work_status"] = "subtotal"
        output_rows.append(subtotal_row)

    return output_rows


def generate_attendance_summary(
    attendences: list[dict[str, Any]],
    _device_logs: list[dict[str, Any]],
    _finger_logs: list[dict[str, Any]],
    _migration_logs: list[dict[str, Any]],
    _user_logs: list[dict[str, Any]],
    shift_mappings: list[dict[str, Any]] | None = None,
) -> pd.DataFrame:
    """Generate attendance summary with all mappings applied."""
    summary_df = process_attendance_summary(attendences)

    if summary_df is not None and not summary_df.empty:
        summary_df = apply_branch_mappings(summary_df)
        summary_df = apply_designation_mappings(summary_df)
        summary_df = apply_employee_branch_mappings(summary_df)
        summary_df = apply_employee_name_mappings(summary_df)
        summary_df = apply_shift_mappings(summary_df, shift_mappings)

        output_rows = create_subtotal_rows(summary_df)
        merged = pd.DataFrame(output_rows, columns=summary_df.columns)
    else:
        merged = summary_df if summary_df is not None else pd.DataFrame()

    return merged


def find_column_indices(ws: Any) -> tuple[int | None, int | None, int | None]:
    """Find column indices for highlighting."""
    day_col = None
    work_status_col = None
    shift_capped_col = None

    for idx, cell in enumerate(ws[1], start=1):
        if cell.value == "day":
            day_col = idx
        if cell.value == "work_status":
            work_status_col = idx
        if cell.value == "shift_capped":
            shift_capped_col = idx

    return day_col, work_status_col, shift_capped_col


def apply_subtotal_highlighting(row: list, day_col: int | None, blue_fill: PatternFill) -> None:
    """Apply highlighting for subtotal rows."""
    if day_col and row[day_col - 1].value == "Subtotal":
        for cell in row:
            cell.fill = blue_fill


def apply_status_highlighting(row: list, work_status_col: int | None, shift_capped_col: int | None, green_fill: PatternFill, red_fill: PatternFill, yellow_fill: PatternFill) -> None:
    """Apply highlighting based on work status."""
    if not work_status_col:
        return

    work_status = row[work_status_col - 1].value
    shift_capped = row[shift_capped_col - 1].value if shift_capped_col else False

    if shift_capped:
        for cell in row:
            cell.fill = yellow_fill
    elif work_status == "worked":
        for cell in row:
            cell.fill = green_fill
    elif work_status == "absent":
        for cell in row:
            cell.fill = red_fill


def apply_row_highlighting(ws: Any) -> None:
    """Apply highlighting to rows based on work status."""
    day_col, work_status_col, shift_capped_col = find_column_indices(ws)

    blue_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
    green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
    red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
    yellow_fill = PatternFill(start_color="FFEB3B", end_color="FFEB3B", fill_type="solid")

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        apply_subtotal_highlighting(row, day_col, blue_fill)
        apply_status_highlighting(row, work_status_col, shift_capped_col, green_fill, red_fill, yellow_fill)


def write_excel(
    attendences: list[dict[str, Any]], device_logs: list[dict[str, Any]], finger_logs: list[dict[str, Any]], migration_logs: list[dict[str, Any]], user_logs: list[dict[str, Any]], merged: pd.DataFrame
) -> io.BytesIO:
    """Write data to Excel file with formatting."""
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame(attendences).to_excel(writer, sheet_name="Attendences", index=False)
        pd.DataFrame(device_logs).to_excel(writer, sheet_name="DeviceLogs", index=False)
        pd.DataFrame(finger_logs).to_excel(writer, sheet_name="FingerLogs", index=False)
        pd.DataFrame(migration_logs).to_excel(writer, sheet_name="Migrations", index=False)
        pd.DataFrame(user_logs).to_excel(writer, sheet_name="Users", index=False)
        merged.to_excel(writer, sheet_name="AttendanceSummary", index=False)

    output.seek(0)
    wb = load_workbook(output)

    if "AttendanceSummary" in wb.sheetnames:
        apply_row_highlighting(wb["AttendanceSummary"])

    new_output = io.BytesIO()
    wb.save(new_output)
    new_output.seek(0)
    return new_output
