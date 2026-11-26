
import config
import os

config.load_env()

devices = {
    "WQ-BID-001": "TB_TOKEN_WQ_BID_001",
    "WQ-BID-002": "TB_TOKEN_WQ_BID_002",
    "WQ-BID-018": "TB_TOKEN_WQ_BID_018"
}

print("ThingsBoard Configuration")
print("-------------------------")
print("Host:", os.environ.get("TB_HOST"))
print("Port:", os.environ.get("TB_PORT"))

for device, token_key in devices.items():
    token = os.environ.get(token_key)
    status = "Loaded" if token and token.strip() else "Missing"
    print(f"{device}: {status}")
