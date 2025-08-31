import pandas as pd
from pprint import pprint

from adms_wrapper.core.data_processing import calculate_time_spent_and_flag

# Setup a shift that starts at 22:00 and ends at 06:00 (overnight)
shift_dict = {
    "NIGHT": {"shift_start": "22:00:00", "shift_end": "06:00:00"},
}

cases = []

day = "2025-08-30"
start_ts = pd.to_datetime(f"{day} 22:30:00")
# 1) Normal overnight checkout next day at 05:30 (before shift_end -> same window)
cases.append(("overnight_early", pd.Series({"start_time": start_ts, "end_time": pd.to_datetime(f"2025-08-31 05:30:00"), "shift": "NIGHT", "day": day})))
# 2) Overnight checkout after shift end and into overtime but before cap
cases.append(("overnight_overtime", pd.Series({"start_time": start_ts, "end_time": pd.to_datetime(f"2025-08-31 10:00:00"), "shift": "NIGHT", "day": day})))
# 3) Missing checkout -> should be capped at shift_end + 8h
cases.append(("missing_checkout", pd.Series({"start_time": start_ts, "end_time": pd.NaT, "shift": "NIGHT", "day": day})))
# 4) Checkout way after cap -> treat as capped
cases.append(("after_cap", pd.Series({"start_time": start_ts, "end_time": pd.to_datetime(f"2025-08-31 20:00:00"), "shift": "NIGHT", "day": day})))
# 5) Very long session >24h
cases.append(("multi_day", pd.Series({"start_time": pd.to_datetime(f"2025-08-30 08:00:00"), "end_time": pd.to_datetime(f"2025-09-01 10:30:00"), "shift": "NIGHT", "day": "2025-08-30"})))

# User example: start 08:00, shift end 17:30, checkout next day 01:30 -> should count full worked time (~17.5 hours)
shift_dict["EX_USER"] = {"shift_start": "08:00:00", "shift_end": "17:30:00"}
cases.append(("user_example_long_shift", pd.Series({"start_time": pd.to_datetime("2025-08-30 08:00:00"), "end_time": pd.to_datetime("2025-08-31 01:30:00"), "shift": "EX_USER", "day": "2025-08-30", "employee_id": "EX_USER"})))

print("Running calculate_time_spent_and_flag tests:\n")
for name, row in cases:
    try:
        ts, capped, effective_end = calculate_time_spent_and_flag(row, shift_dict)
    except Exception as e:
        print(name, "ERROR:", e)
        continue
    print(f"Case: {name}")
    print("  start_time:", row.get("start_time"))
    print("  end_time:", row.get("end_time"))
    print("  returned_time_spent:", ts)
    print("  shift_capped:", capped)
    print("  effective_end:", effective_end)
    print()

print("Done")
