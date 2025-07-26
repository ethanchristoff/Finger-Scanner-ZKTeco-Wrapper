import pandas as pd
from openpyxl.styles import PatternFill
from openpyxl import load_workbook
from adms_wrapper.core.db_queries import get_device_branch_mappings


def generate_attendance_summary(attendences, device_logs, finger_logs, migration_logs, user_logs, shift_mappings=None):
    summary_df = process_attendance_summary(attendences)
    if summary_df is not None and not summary_df.empty:
        mappings = get_device_branch_mappings() or []
        mappings_df = pd.DataFrame(mappings)

        def map_branch(sn):
            if pd.isna(sn):
                return ""
            row = mappings_df[mappings_df["serial_number"] == sn]
            if not row.empty:
                return row.iloc[0]["branch_name"]
            return ""

        summary_df["start_device_sn_branch"] = summary_df["start_device_sn"].apply(map_branch)
        summary_df["end_device_sn_branch"] = summary_df["end_device_sn"].apply(map_branch)

        # --- Shift logic ---
        shift_df = pd.DataFrame(shift_mappings) if shift_mappings else pd.DataFrame()

        def get_shift_info(emp_id, day, start_time, end_time):
            if shift_df.empty:
                return "", "on time"
            shift_row = shift_df[shift_df["user_id"] == str(emp_id)]
            if shift_row.empty:
                return "", "no shift"
            shift_name = shift_row.iloc[0]["shift_name"]
            shift_start = shift_row.iloc[0]["shift_start"]
            shift_end = shift_row.iloc[0]["shift_end"]
            # Compare times
            flag = "on time"
            try:
                if pd.notna(start_time) and str(start_time) != "":
                    if str(start_time)[11:16] > str(shift_start):
                        flag = "late in"
                else:
                    flag = "no check-in"
                if pd.notna(end_time) and str(end_time) != "":
                    if str(end_time)[11:16] < str(shift_end):
                        flag = "early out"
                else:
                    flag = "no check-out"
            except Exception:
                pass
            return shift_name, flag

        summary_df["shift_name"] = ""
        summary_df["shift_flag"] = ""
        for idx, row in summary_df.iterrows():
            shift_name, flag = get_shift_info(row["employee_id"], row["day"], row["start_time"], row["end_time"])
            summary_df.at[idx, "shift_name"] = shift_name
            summary_df.at[idx, "shift_flag"] = flag

        agg = summary_df.copy()
        agg["date"] = pd.to_datetime(agg["day"])
        agg["date"] = agg["date"].dt.strftime("%Y-%m-%d")
        agg["serial_number"] = agg.get("start_device_sn", None)
        agg_sheet = agg.groupby(["employee_id", "date", "serial_number"]).agg(first_time=("start_time", "first"), last_time=("end_time", "last")).reset_index()

        def get_status(row):
            if pd.isna(row["first_time"]) and pd.isna(row["last_time"]):
                return "no activity"
            elif pd.isna(row["first_time"]):
                return "missing check-in"
            elif pd.isna(row["last_time"]):
                return "missing check-out"
            else:
                return "ok"

        agg_sheet["status"] = agg_sheet.apply(get_status, axis=1)
        merged = pd.merge(
            summary_df,
            agg_sheet[["employee_id", "date", "serial_number", "status"]],
            left_on=["employee_id", summary_df["day"].astype(str), "start_device_sn"],
            right_on=["employee_id", "date", "serial_number"],
            how="left",
            suffixes=("", "_agg"),
        )
        merged = merged.drop(["date", "serial_number"], axis=1)
        cols = list(summary_df.columns) + ["start_device_sn_branch", "end_device_sn_branch", "shift_name", "shift_flag", "status"]
        cols = [c for i, c in enumerate(cols) if c not in cols[:i]]
        merged = merged[cols]
        merged = merged.sort_values(by=["employee_id", "day"])
        output_rows = []
        for emp_id, group in merged.groupby("employee_id", sort=False):
            output_rows.extend(group.to_dict(orient="records"))
            group["time_spent_td"] = pd.to_timedelta(group["time_spent"])
            subtotal = group["time_spent_td"].sum()
            subtotal_str = str(subtotal).split(".")[0]
            subtotal_row = {col: "" for col in merged.columns}
            subtotal_row["employee_id"] = emp_id
            subtotal_row["day"] = "Subtotal"
            subtotal_row["time_spent"] = subtotal_str
            output_rows.append(subtotal_row)
        merged = pd.DataFrame(output_rows, columns=merged.columns)
    else:
        merged = summary_df if summary_df is not None else pd.DataFrame()
    return merged


def write_excel(attendences, device_logs, finger_logs, migration_logs, user_logs, merged):
    import io

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
    # Highlight subtotal rows in blue, and attendance rows green/red
    if "AttendanceSummary" in wb.sheetnames:
        ws_sum = wb["AttendanceSummary"]
        day_col = None
        time_spent_col = None
        first_time_col = None
        last_time_col = None
        for idx, cell in enumerate(ws_sum[1], start=1):
            if cell.value == "day":
                day_col = idx
            if cell.value == "time_spent":
                time_spent_col = idx
            if cell.value == "start_time":
                first_time_col = idx
            if cell.value == "end_time":
                last_time_col = idx
        blue_fill = PatternFill(start_color="4F81BD", end_color="4F81BD", fill_type="solid")
        green_fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
        red_fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
        for row in ws_sum.iter_rows(min_row=2, max_row=ws_sum.max_row, min_col=1, max_col=ws_sum.max_column):
            if day_col and row[day_col - 1].value == "Subtotal":
                for cell in row:
                    cell.fill = blue_fill
            else:
                if first_time_col and last_time_col:
                    start_val = row[first_time_col - 1].value
                    end_val = row[last_time_col - 1].value
                    if start_val and end_val:
                        for cell in row:
                            cell.fill = green_fill
                    else:
                        for cell in row:
                            cell.fill = red_fill
    new_output = io.BytesIO()
    wb.save(new_output)
    new_output.seek(0)
    return new_output


from adms_wrapper.__main__ import process_attendance_summary
