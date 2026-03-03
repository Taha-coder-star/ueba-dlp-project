import pandas as pd

data = [
    {
        "timestamp": "2026-03-01 09:00:00",
        "user_id": "U1",
        "device_id": "D1",
        "src_ip": "10.0.0.1",
        "event_type": "login",
        "file_sensitivity": 0,
        "bytes_out": 0,
        "dest_domain": "internal",
        "success": 1
    },
    {
        "timestamp": "2026-03-01 22:15:00",
        "user_id": "U1",
        "device_id": "D1",
        "src_ip": "10.0.0.2",
        "event_type": "file_copy",
        "file_sensitivity": 3,
        "bytes_out": 500000,
        "dest_domain": "external",
        "success": 1
    }
]

df = pd.DataFrame(data)
df.to_csv("data/logs.csv", index=False)

print("Sample logs.csv created.")