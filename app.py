from flask import Flask, render_template, request, send_file
import pandas as pd
from adms_wrapper.core.db_queries import get_attendences, get_device_logs, get_finger_log, get_migrations, get_users
from adms_wrapper.__main__ import process_attendance_summary

import io
import os
from openpyxl.styles import PatternFill
from openpyxl import load_workbook

app = Flask(__name__)

# Ensure the static and templates folders exist
if not os.path.exists("static"):
    os.makedirs("static")
if not os.path.exists("templates"):
    os.makedirs("templates")

@app.route("/", methods=["GET"])
def index():
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    attendences = get_attendences() or []
    # Filter attendences by date if provided
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
    summary = summary_df.to_dict(orient="records") if summary_df is not None else []
    return render_template("dashboard.html", attendences=attendences, summary=summary, start_date=start_date or '', end_date=end_date or '')

@app.route("/download_xlsx")
def download_xlsx():
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    # Use the same logic as the main function to filter and export
    output = io.BytesIO()
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
    device_logs = get_device_logs() or []
    finger_logs = get_finger_log() or []
    migration_logs = get_migrations() or []
    user_logs = get_users() or []
    summary_df = process_attendance_summary(attendences)
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame(attendences).to_excel(writer, sheet_name="Attendences", index=False)
        pd.DataFrame(device_logs).to_excel(writer, sheet_name="DeviceLogs", index=False)
        pd.DataFrame(finger_logs).to_excel(writer, sheet_name="FingerLogs", index=False)
        pd.DataFrame(migration_logs).to_excel(writer, sheet_name="Migrations", index=False)
        pd.DataFrame(user_logs).to_excel(writer, sheet_name="Users", index=False)
        if summary_df is not None:
            summary_df.to_excel(writer, sheet_name="AttendanceSummary", index=False)

        # --- Aggregate Sheet for Unique User Logins per Day ---
        if summary_df is not None and not summary_df.empty:
            agg = summary_df.copy()
            agg['date'] = pd.to_datetime(agg['day'])
            agg['date'] = agg['date'].dt.strftime('%Y-%m-%d')
            agg_sheet = agg.groupby(['employee_id', 'date']).agg(
                first_time=('start_time', 'first'),
                last_time=('end_time', 'last')
            ).reset_index()
            def get_status(row):
                if pd.isna(row['first_time']) and pd.isna(row['last_time']):
                    return 'no activity'
                elif pd.isna(row['first_time']):
                    return 'missing check-in'
                elif pd.isna(row['last_time']):
                    return 'missing check-out'
                else:
                    return 'ok'
            agg_sheet['status'] = agg_sheet.apply(get_status, axis=1)
            agg_sheet.to_excel(writer, sheet_name="Aggregate", index=False)
    output.seek(0)
    wb = load_workbook(output)
    if 'Aggregate' in wb.sheetnames:
        ws = wb['Aggregate']
        status_col = None
        for idx, cell in enumerate(ws[1], 1):
            if cell.value == 'status':
                status_col = idx
                break
        if status_col:
            green = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
            yellow = PatternFill(start_color="FFF9C4", end_color="FFF9C4", fill_type="solid")
            red = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
            for row in ws.iter_rows(min_row=2, min_col=status_col, max_col=status_col):
                cell = row[0]
                if cell.value == 'ok':
                    for c in ws[cell.row]:
                        c.fill = green
                elif cell.value == 'missing check-in' or cell.value == 'missing check-out':
                    for c in ws[cell.row]:
                        c.fill = yellow
                elif cell.value == 'no activity':
                    for c in ws[cell.row]:
                        c.fill = red
    new_output = io.BytesIO()
    wb.save(new_output)
    new_output.seek(0)
    return send_file(new_output, as_attachment=True, download_name="output.xlsx", mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)

