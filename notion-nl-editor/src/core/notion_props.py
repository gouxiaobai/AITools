from typing import Any, Dict, List, Optional


def rt_plain(rt: Optional[List[Dict[str, Any]]]) -> str:
    if not rt:
        return ""
    return "".join(x.get("plain_text", "") for x in rt).strip()


def get_prop(page: Dict[str, Any], key: str) -> Dict[str, Any]:
    return page.get("properties", {}).get(key, {})


def p_title(page: Dict[str, Any], key: str) -> str:
    return rt_plain(get_prop(page, key).get("title", []))


def p_rich(page: Dict[str, Any], key: str) -> str:
    return rt_plain(get_prop(page, key).get("rich_text", []))


def p_number(page: Dict[str, Any], key: str) -> Optional[float]:
    return get_prop(page, key).get("number")


def p_date(page: Dict[str, Any], key: str) -> str:
    date_obj = get_prop(page, key).get("date")
    if not date_obj:
        return ""
    return date_obj.get("start", "")


def p_select(page: Dict[str, Any], key: str) -> str:
    obj = get_prop(page, key).get("select")
    return obj.get("name", "") if obj else ""


def p_relation_ids(page: Dict[str, Any], key: str) -> List[str]:
    rel = get_prop(page, key).get("relation", [])
    return [x.get("id") for x in rel if x.get("id")]


def p_formula_number(page: Dict[str, Any], key: str) -> Optional[float]:
    formula = get_prop(page, key).get("formula", {})
    if formula.get("type") == "number":
        return formula.get("number")
    return None


def find_title_property_name(db: Dict[str, Any]) -> str:
    for k, info in db.get("properties", {}).items():
        if info.get("type") == "title":
            return k
    raise RuntimeError("No title property in database.")


def text_prop(text: str) -> Dict[str, Any]:
    return {"rich_text": [{"type": "text", "text": {"content": text[:2000]}}]}


def title_prop(text: str) -> Dict[str, Any]:
    return {"title": [{"type": "text", "text": {"content": text[:2000]}}]}
