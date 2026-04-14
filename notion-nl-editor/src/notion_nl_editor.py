import argparse
import datetime as dt
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional

import requests


def load_dotenv(path: str) -> None:
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


class NotionClient:
    def __init__(self, token: str, version: str = "2022-06-28") -> None:
        self.base = "https://api.notion.com/v1"
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": version,
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base}{path}"
        resp = requests.request(method, url, headers=self.headers, json=payload, timeout=30)
        if resp.status_code >= 400:
            raise RuntimeError(f"Notion API error {resp.status_code}: {resp.text}")
        return resp.json()

    def get_database(self, database_id: str) -> Dict[str, Any]:
        return self._request("GET", f"/databases/{database_id}")

    def query_database(self, database_id: str, filter_obj: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        while True:
            payload: Dict[str, Any] = {"page_size": 100}
            if filter_obj:
                payload["filter"] = filter_obj
            if cursor:
                payload["start_cursor"] = cursor
            data = self._request("POST", f"/databases/{database_id}/query", payload)
            results.extend(data.get("results", []))
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
        return results

    def update_page(self, page_id: str, properties: Dict[str, Any]) -> Dict[str, Any]:
        return self._request("PATCH", f"/pages/{page_id}", {"properties": properties})


def get_page_title(page: Dict[str, Any]) -> str:
    props = page.get("properties", {})
    for _, value in props.items():
        if value.get("type") == "title":
            arr = value.get("title", [])
            return "".join([x.get("plain_text", "") for x in arr]).strip() or "(no title)"
    return "(no title)"


def parse_nl_rule(text: str) -> Dict[str, Any]:
    text = text.strip()

    p1 = re.compile(
        r'^把["“](.+?)["”]为["“](.+?)["”]的记录[，,]\s*将["“](.+?)["”]改为["“](.+?)["”]$'
    )
    m1 = p1.match(text)
    if m1:
        f_prop, f_val, u_prop, u_val = m1.groups()
        return {
            "description": "Parsed from NL template 1",
            "filters": [
                {"property": f_prop, "type": "select", "op": "equals", "value": f_val}
            ],
            "updates": {
                u_prop: {"type": "select", "value": u_val}
            },
        }

    p2 = re.compile(
        r'^将["“](.+?)["”]在今天之前的记录[，,]\s*["“](.+?)["”]改为["“](.+?)["”]$'
    )
    m2 = p2.match(text)
    if m2:
        date_prop, status_prop, status_val = m2.groups()
        today = dt.date.today().isoformat()
        return {
            "description": "Parsed from NL template 2",
            "filters": [
                {"property": date_prop, "type": "date", "op": "before", "value": today}
            ],
            "updates": {
                status_prop: {"type": "status", "value": status_val}
            },
        }

    raise ValueError(
        "Unsupported NL template. Use one of: "
        "把\"属性A\"为\"值A\"的记录，将\"属性B\"改为\"值B\" or "
        "将\"日期属性\"在今天之前的记录，\"状态属性\"改为\"值\""
    )


def load_rule(rule_file: Optional[str], nl: Optional[str]) -> Dict[str, Any]:
    if bool(rule_file) == bool(nl):
        raise ValueError("Exactly one of --rule-file or --nl must be provided.")
    if nl:
        return parse_nl_rule(nl)
    assert rule_file is not None
    with open(rule_file, "r", encoding="utf-8") as f:
        return json.load(f)


def build_filter(filter_items: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not filter_items:
        return None

    clauses: List[Dict[str, Any]] = []
    for item in filter_items:
        prop = item["property"]
        typ = item["type"]
        op = item.get("op", "equals")
        value = item.get("value")

        if typ in {"select", "status", "rich_text", "number", "checkbox", "date", "title"}:
            clauses.append({"property": prop, typ: {op: value}})
        else:
            raise ValueError(f"Unsupported filter type: {typ}")

    if len(clauses) == 1:
        return clauses[0]
    return {"and": clauses}


def build_updates(update_obj: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for prop_name, conf in update_obj.items():
        typ = conf["type"]
        value = conf.get("value")

        if typ in {"select", "status"}:
            payload[prop_name] = {typ: {"name": str(value)}}
        elif typ == "rich_text":
            payload[prop_name] = {"rich_text": [{"type": "text", "text": {"content": str(value)}}]}
        elif typ == "title":
            payload[prop_name] = {"title": [{"type": "text", "text": {"content": str(value)}}]}
        elif typ == "number":
            payload[prop_name] = {"number": float(value)}
        elif typ == "checkbox":
            payload[prop_name] = {"checkbox": parse_bool(value)}
        elif typ == "date":
            payload[prop_name] = {"date": {"start": str(value)}}
        else:
            raise ValueError(f"Unsupported update type: {typ}")
    return payload


def cmd_properties(client: NotionClient, database_id: str) -> int:
    db = client.get_database(database_id)
    props = db.get("properties", {})
    print("Database properties:")
    for name, info in props.items():
        print(f"- {name}: {info.get('type')}")
    return 0


def cmd_preview(client: NotionClient, database_id: str, rule: Dict[str, Any], limit: int) -> int:
    filters = rule.get("filters", [])
    notion_filter = build_filter(filters)
    rows = client.query_database(database_id, filter_obj=notion_filter)

    print(f"Matched rows: {len(rows)}")
    for row in rows[:limit]:
        title = get_page_title(row)
        print(f"- {title} | id={row.get('id')}")

    updates = rule.get("updates", {})
    print("Planned updates:")
    print(json.dumps(updates, ensure_ascii=False, indent=2))
    return 0


def cmd_apply(client: NotionClient, database_id: str, rule: Dict[str, Any], do_apply: bool) -> int:
    if not do_apply:
        print("Dry mode. Add --yes to execute updates.")
        return 2

    filters = rule.get("filters", [])
    updates = rule.get("updates", {})
    if not updates:
        raise ValueError("Rule has no updates.")

    notion_filter = build_filter(filters)
    rows = client.query_database(database_id, filter_obj=notion_filter)
    update_payload = build_updates(updates)

    print(f"Updating {len(rows)} rows...")
    ok = 0
    for row in rows:
        page_id = row["id"]
        client.update_page(page_id, update_payload)
        ok += 1
        if ok % 10 == 0:
            print(f"  progress: {ok}/{len(rows)}")

    print(f"Done. Updated rows: {ok}")
    return 0


def main() -> int:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    root = os.path.dirname(script_dir)
    load_dotenv(os.path.join(root, ".env"))

    parser = argparse.ArgumentParser(description="Notion natural language editor (minimal)")
    parser.add_argument("command", choices=["properties", "preview", "apply"])
    parser.add_argument("--database-id", default=os.getenv("NOTION_DATABASE_ID"))
    parser.add_argument("--token", default=os.getenv("NOTION_TOKEN"))
    parser.add_argument("--notion-version", default=os.getenv("NOTION_VERSION", "2022-06-28"))
    parser.add_argument("--rule-file")
    parser.add_argument("--nl")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--yes", action="store_true")

    args = parser.parse_args()

    if not args.token:
        print("Missing token. Set NOTION_TOKEN in .env or use --token.", file=sys.stderr)
        return 1
    if not args.database_id:
        print("Missing database id. Set NOTION_DATABASE_ID in .env or use --database-id.", file=sys.stderr)
        return 1

    client = NotionClient(token=args.token, version=args.notion_version)

    if args.command == "properties":
        return cmd_properties(client, args.database_id)

    rule = load_rule(args.rule_file, args.nl)
    if args.command == "preview":
        return cmd_preview(client, args.database_id, rule, args.limit)
    if args.command == "apply":
        return cmd_apply(client, args.database_id, rule, args.yes)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
