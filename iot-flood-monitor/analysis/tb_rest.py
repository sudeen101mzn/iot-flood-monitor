"""ThingsBoard REST helpers for the measurements that need the SERVER's view.
NOT yet tested against demo.thingsboard.io (needs your tenant login). Uses only the standard library.

  end-to-end latency : server-stamped live messages carry gen_ts_ms in their values; the stored
                       telemetry timestamp is the broker arrival time, so latency = ts - gen_ts_ms
  alarm activation   : alarm createdTime - generation time of the first breaching reading
  data loss          : rows generated vs data points present after the final sync
Credentials come from the environment: TB_USER, TB_PASSWORD (never commit them).
"""
import json
import os
import urllib.parse
import urllib.request


class TbRest:
    def __init__(self, host=None, user=None, password=None):
        self.base = "https://" + (host or os.environ.get("TB_HOST", "demo.thingsboard.io"))
        self.token = self._login(user or os.environ["TB_USER"], password or os.environ["TB_PASSWORD"])

    def _call(self, method, path, body=None, auth=True):
        req = urllib.request.Request(self.base + path, method=method,
                                     data=json.dumps(body).encode() if body is not None else None)
        req.add_header("Content-Type", "application/json")
        if auth:
            req.add_header("X-Authorization", "Bearer " + self.token)
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode() or "null")

    def _login(self, user, password):
        return self._call("POST", "/api/auth/login", {"username": user, "password": password}, auth=False)["token"]

    def device_id(self, name):
        return self._call("GET", "/api/tenant/devices?deviceName=" + urllib.parse.quote(name))["id"]["id"]

    def timeseries(self, device_id, keys, start_ts, end_ts, limit=100000):
        q = urllib.parse.urlencode({"keys": ",".join(keys), "startTs": start_ts, "endTs": end_ts,
                                    "limit": limit, "agg": "NONE", "orderBy": "ASC"})
        return self._call("GET", f"/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries?{q}")

    def alarms(self, device_id, page_size=200):
        q = urllib.parse.urlencode({"pageSize": page_size, "page": 0, "sortProperty": "createdTime", "sortOrder": "DESC"})
        return self._call("GET", f"/api/alarm/DEVICE/{device_id}?{q}")["data"]
