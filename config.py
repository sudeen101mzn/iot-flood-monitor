"""Tiny configuration helpers: read a local .env file and per-node tokens."""
import os
from pathlib import Path


def load_env(path=".env"):
    """Load KEY=VALUE lines from a .env file without overriding real environment variables."""
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"'))


def token_for(node_id):
    """Device access token for a node, e.g. WQ-BID-001 -> TB_TOKEN_WQ_BID_001."""
    return os.environ.get("TB_TOKEN_" + node_id.replace("-", "_"))
