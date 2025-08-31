import pandas as pd
from pprint import pprint

import adms_wrapper.core.data_processing as dp

# Monkeypatch get_user_shift_mappings to return a mapping for employee E1

def fake_get_user_shift_mappings():
    return [
        {"user_id": "E1", "shift_name": "DAY", "shift_start": "08:00:00", "shift_end": "17:30:00"}
    ]

# Replace the db query function
try:
    dp.get_user_shift_mappings = fake_get_user_shift_mappings
except Exception as e:
    print('Failed to monkeypatch get_user_shift_mappings:', e)

# Create attendences: start on 2025-08-30 08:10 (no checkout), then check-in on 2025-08-31 07:40 (early-in within 30m before next shift start)
attendences = [
    {"employee_id": "E1", "timestamp": "2025-08-30 08:10:00", "sn": "DEV1"},
    {"employee_id": "E1", "timestamp": "2025-08-31 07:40:00", "sn": "DEV1"},
]

res = dp.process_attendance_summary(attendences)
print('Processed summary:')
print(res.to_string(index=False))
