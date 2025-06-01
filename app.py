from flask import Flask, render_template, request, send_file, url_for
import pandas as pd
from adms_wrapper.core.db_queries import get_attendences, get_device_logs, get_finger_log, get_migrations, get_users
from adms_wrapper.__main__ import process_attendance_summary, main
import io
import os

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
    output.seek(0)
    return send_file(output, as_attachment=True, download_name="output.xlsx", mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

if __name__ == "__main__":
    app.run(debug=True, port=8080)

