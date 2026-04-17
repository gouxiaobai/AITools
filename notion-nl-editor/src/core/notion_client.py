import os
import time
from typing import Any, Dict, List, Optional

import requests
from requests import exceptions as req_exc


class NotionClient:
    def __init__(self, token: str, version: str) -> None:
        self.base = "https://api.notion.com/v1"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": version,
            "Content-Type": "application/json",
        }
        self.session = requests.Session()
        self.max_retries = int(os.getenv("NOTION_HTTP_RETRIES", "4"))
        self.retry_backoff = float(os.getenv("NOTION_HTTP_RETRY_BACKOFF", "0.8"))

    def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base}{path}"
        attempts = max(self.max_retries, 1)
        for i in range(attempts):
            try:
                resp = self.session.request(method, url, headers=self.headers, json=payload, timeout=30)
                if resp.status_code >= 400:
                    raise RuntimeError(f"Notion API error {resp.status_code} {path}: {resp.text}")
                if not resp.text:
                    return {}
                return resp.json()
            except (req_exc.SSLError, req_exc.ConnectionError, req_exc.Timeout) as e:
                if i == attempts - 1:
                    raise RuntimeError(f"Notion request failed after {attempts} attempts {path}: {e}") from e
                time.sleep(self.retry_backoff * (2**i))

    def get_database(self, database_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/databases/{database_id}")

    def query_database_all(self, database_id: str, filter_obj: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        while True:
            payload: Dict[str, Any] = {"page_size": 100}
            if filter_obj:
                payload["filter"] = filter_obj
            if cursor:
                payload["start_cursor"] = cursor
            data = self._request("POST", f"/databases/{database_id}/query", payload)
            rows.extend(data.get("results", []))
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
        return rows

    def create_page(self, database_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("POST", "/pages", {"parent": {"database_id": database_id}, "properties": properties})

    def update_page(self, page_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("PATCH", f"/pages/{page_id}", {"properties": properties})
