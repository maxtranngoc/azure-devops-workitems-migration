#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Copy parent work items + children from SOURCE ADO to TARGET ADO (stdlib only).

Config: env vars or CLI args (all required)
  ADO_SOURCE_ORG_URL, ADO_SOURCE_PROJECT, ADO_SOURCE_PAT
  ADO_TARGET_ORG_URL, ADO_TARGET_PROJECT, ADO_TARGET_PAT

Idempotence:
  Uses Custom.ReflectedWorkItemId on target.

Optional:
  --with-comments : migrates comments into System.History (fallback to revisions)
"""
from __future__ import annotations

import argparse
import sys
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote

from _common import API_VERSION, COMMENTS_API_VERSION, AdoConn, env, require, http_json

BATCH_GET_LIMIT = 200

REFLECTED = "Custom.ReflectedWorkItemId"
SRC_OWNERORG_FIELD = "Custom.OwnerOrg"
TGT_OWNERORG_FIELD = "Custom.OwnerOrg"

PARENT_TYPES = ["Work Bundle", "Workbundle", "WorkBundle"]
TARGET_PARENT_TYPE = "Work Bundle"

CHILD_TYPE_MAP = {"User Story": "User Story", "Issue": "User Story"}
CHILD_TYPE_MAP_LOWER = {k.lower(): v for k, v in CHILD_TYPE_MAP.items()}

# Optional: add mappings here (leave empty for public repo)
USER_MAP: Dict[str, str] = {}


def normalize_identity(value) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, dict):
        for k in ("uniqueName", "principalName", "mail", "email"):
            if value.get(k):
                return str(value[k]).strip() or None
        if value.get("displayName"):
            return str(value["displayName"]).strip() or None
    return str(value).strip() or None


def map_assigned_to(value) -> Optional[str]:
    norm = (normalize_identity(value) or "").lower()
    return USER_MAP.get(norm, normalize_identity(value))


def field_exists(conn_tgt: AdoConn, refname: str) -> bool:
    try:
        http_json("GET", f"{conn_tgt.org_url}/_apis/wit/fields/{refname}?api-version={API_VERSION}", conn_tgt.auth)
        return True
    except Exception:
        return False


def type_exists(conn_tgt: AdoConn, type_name: str) -> bool:
    t = quote(type_name, safe="")
    try:
        http_json("GET", f"{conn_tgt.org_url}/{conn_tgt.project}/_apis/wit/workitemtypes/{t}?api-version={API_VERSION}",
                  conn_tgt.auth)
        return True
    except Exception:
        return False


def http_patch_workitem(conn: AdoConn, workitem_id: int, patch: list):
    url = f"{conn.org_url}/{conn.project}/_apis/wit/workitems/{workitem_id}?api-version={API_VERSION}"
    return http_json("PATCH", url, conn.auth, patch, "application/json-patch+json")


def create_work_item(conn: AdoConn, type_name: str, fields: Dict[str, object], parent_id: Optional[int] = None):
    witype_q = quote(type_name, safe="")
    url = f"{conn.org_url}/{conn.project}/_apis/wit/workitems/${witype_q}?api-version={API_VERSION}"
    patch = [{"op": "add", "path": f"/fields/{k}", "value": v} for k, v in fields.items() if v is not None]
    if parent_id is not None:
        parent_url = f"{conn.org_url}/{conn.project}/_apis/wit/workItems/{parent_id}"
        patch.append({"op": "add", "path": "/relations/-",
                      "value": {"rel": "System.LinkTypes.Hierarchy-Reverse", "url": parent_url}})
    return http_json("PATCH", url, conn.auth, patch, "application/json-patch+json")


def wiql(conn: AdoConn, query: str):
    url = f"{conn.org_url}/{conn.project}/_apis/wit/wiql?api-version={API_VERSION}"
    return http_json("POST", url, conn.auth, {"query": " ".join(query.split())})


def batch_get(conn: AdoConn, ids: Sequence[int], fields: Sequence[str]):
    url = f"{conn.org_url}/_apis/wit/workitemsbatch?api-version={API_VERSION}"
    return http_json("POST", url, conn.auth, {"ids": list(ids), "fields": list(fields)})


def find_target_by_reflected(conn_tgt: AdoConn, source_id: int) -> Optional[int]:
    q = f"""
      SELECT [System.Id] FROM WorkItems
      WHERE [System.TeamProject] = '{conn_tgt.project}' AND [{REFLECTED}] = '{source_id}'
    """
    res = wiql(conn_tgt, q) or {}
    ids = [wi["id"] for wi in res.get("workItems", [])]
    return ids[0] if ids else None


def iterate_parent_ids(conn_src: AdoConn, max_items: Optional[int], start_id: Optional[int],
                      exclude_ownerorg_field: Optional[str], exclude_ownerorg_value: Optional[str]):
    last_id = (start_id - 1) if (start_id and start_id > 0) else 0
    fetched = 0
    in_types = ",".join([f"'{t}'" for t in PARENT_TYPES])
    owner_clause = ""
    if exclude_ownerorg_field and exclude_ownerorg_value:
        owner_clause = f"AND [{exclude_ownerorg_field}] <> '{exclude_ownerorg_value}'"
    while True:
        q = f"""
          SELECT [System.Id] FROM WorkItems
          WHERE [System.TeamProject] = '{conn_src.project}'
            AND [System.WorkItemType] IN ({in_types})
            {owner_clause}
            AND [System.Id] > {last_id}
          ORDER BY [System.Id] ASC
        """
        res = wiql(conn_src, q) or {}
        page = [wi["id"] for wi in res.get("workItems", [])]
        if not page:
            break
        for wid in page:
            yield wid
            last_id = wid
            fetched += 1
            if max_items and fetched >= max_items:
                return


def get_children_related(conn_src: AdoConn, parent_id: int) -> Tuple[List[int], List[int]]:
    url = f"{conn_src.org_url}/{conn_src.project}/_apis/wit/workitems/{parent_id}?$expand=relations&api-version={API_VERSION}"
    wi = http_json("GET", url, conn_src.auth) or {}
    children, related = set(), set()
    for r in wi.get("relations", []) or []:
        rel = (r.get("rel") or "").lower()
        href = r.get("url") or ""
        try:
            wid = int(href.rsplit("/", 1)[-1])
        except Exception:
            continue
        if "hierarchy-forward" in rel:
            children.add(wid)
        elif rel.endswith("related"):
            related.add(wid)
    related -= children
    return list(children), list(related)


def add_related_link(conn_tgt: AdoConn, tgt_parent: int, tgt_other: int):
    other_url = f"{conn_tgt.org_url}/{conn_tgt.project}/_apis/wit/workItems/{tgt_other}"
    patch = [{"op": "add", "path": "/relations/-", "value": {"rel": "System.LinkTypes.Related", "url": other_url}}]
    http_patch_workitem(conn_tgt, tgt_parent, patch)


def get_comments(conn_src: AdoConn, wid: int) -> List[dict]:
    candidates = [
        f"{conn_src.org_url}/_apis/wit/workItems/{wid}/comments?api-version={COMMENTS_API_VERSION}",
        f"{conn_src.org_url}/{conn_src.project}/_apis/wit/workItems/{wid}/comments?api-version={COMMENTS_API_VERSION}",
    ]
    for u in candidates:
        try:
            res = http_json("GET", u, conn_src.auth) or {}
            return res.get("comments", []) or []
        except Exception:
            pass
    # fallback: revisions history
    rev_url = f"{conn_src.org_url}/{conn_src.project}/_apis/wit/workItems/{wid}/revisions?api-version={API_VERSION}"
    res = http_json("GET", rev_url, conn_src.auth) or {}
    out = []
    for it in (res.get("value") or []):
        fields = it.get("fields") or {}
        hist = fields.get("System.History")
        if hist:
            out.append({"text": str(hist), "createdDate": it.get("revisedDate")})
    return out


def push_history(conn_tgt: AdoConn, tgt_wid: int, text: str):
    patch = [{"op": "add", "path": "/fields/System.History", "value": text}]
    http_patch_workitem(conn_tgt, tgt_wid, patch)


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-org", default=env("ADO_SOURCE_ORG_URL"))
    ap.add_argument("--source-project", default=env("ADO_SOURCE_PROJECT"))
    ap.add_argument("--source-pat", default=env("ADO_SOURCE_PAT"))
    ap.add_argument("--target-org", default=env("ADO_TARGET_ORG_URL"))
    ap.add_argument("--target-project", default=env("ADO_TARGET_PROJECT"))
    ap.add_argument("--target-pat", default=env("ADO_TARGET_PAT"))

    ap.add_argument("--max", type=int, default=None)
    ap.add_argument("--start-id", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--with-comments", action="store_true")

    ap.add_argument("--exclude-ownerorg-field", default=env("ADO_EXCLUDE_OWNERORG_FIELD"))
    ap.add_argument("--exclude-ownerorg-value", default=env("ADO_EXCLUDE_OWNERORG_VALUE"))

    ap.add_argument("--target-area-root", default=env("ADO_TARGET_AREA_ROOT"))
    ap.add_argument("--target-iteration-root", default=env("ADO_TARGET_ITERATION_ROOT"))
    ap.add_argument("--force-root", action="store_true")

    return ap.parse_args()


def remap_root(src_path: Optional[str], src_root: str, tgt_root: str) -> str:
    if not src_path:
        return tgt_root
    parts = str(src_path).split("\\")
    if parts and parts[0].lower() == src_root.lower():
        parts[0] = tgt_root
    else:
        parts.insert(0, tgt_root)
    return "\\".join(parts)


def main():
    args = parse_args()

    conn_src = AdoConn(
        org_url=require(args.source_org, "source_org", "ADO_SOURCE_ORG_URL"),
        project=require(args.source_project, "source_project", "ADO_SOURCE_PROJECT"),
        pat=require(args.source_pat, "source_pat", "ADO_SOURCE_PAT"),
    )
    conn_tgt = AdoConn(
        org_url=require(args.target_org, "target_org", "ADO_TARGET_ORG_URL"),
        project=require(args.target_project, "target_project", "ADO_TARGET_PROJECT"),
        pat=require(args.target_pat, "target_pat", "ADO_TARGET_PAT"),
    )

    area_root = (args.target_area_root or "").strip() or conn_tgt.project
    iter_root = (args.target_iteration_root or "").strip() or conn_tgt.project

    # Warnings for missing types/fields (non-fatal)
    if not type_exists(conn_tgt, TARGET_PARENT_TYPE):
        print(f"[WARN] Target type missing: {TARGET_PARENT_TYPE}")

    parent_fields = [
        "System.Id", "System.Title", "System.Description", "System.State", "System.Tags",
        "System.AreaPath", "System.IterationPath", "System.AssignedTo", SRC_OWNERORG_FIELD
    ]
    child_fields = [
        "System.Id", "System.WorkItemType", "System.Title", "System.Description", "System.State", "System.Tags",
        "System.AreaPath", "System.IterationPath", "System.AssignedTo"
    ]

    created_parent = created_other = related_links = 0

    for pid in iterate_parent_ids(conn_src, args.max, args.start_id, args.exclude_ownerorg_field, args.exclude_ownerorg_value):
        parent_data = batch_get(conn_src, [pid], parent_fields).get("value", [])
        if not parent_data:
            continue
        f = parent_data[0].get("fields", {}) or {}

        tgt_parent = find_target_by_reflected(conn_tgt, pid)
        if not tgt_parent:
            fields = {
                "System.Title": f.get("System.Title", f"Migrated {pid}"),
                "System.Description": (f.get("System.Description") or ""),
                "System.Tags": (f.get("System.Tags") or ""),
                REFLECTED: str(pid),
            }
            if args.force_root:
                fields["System.AreaPath"] = area_root
                fields["System.IterationPath"] = iter_root
            else:
                fields["System.AreaPath"] = remap_root(f.get("System.AreaPath"), conn_src.project, area_root)
                fields["System.IterationPath"] = remap_root(f.get("System.IterationPath"), conn_src.project, iter_root)

            if args.dry_run:
                tgt_parent = -1
            else:
                created = create_work_item(conn_tgt, TARGET_PARENT_TYPE, fields)
                tgt_parent = int(created.get("id"))
            created_parent += 1
            print(f"+ parent #{pid} -> #{tgt_parent}")
        else:
            print(f"= parent #{pid} already exists -> #{tgt_parent}")

        if args.with_comments and (not args.dry_run) and int(tgt_parent) > 0:
            for c in get_comments(conn_src, pid):
                txt = c.get("text") or ""
                if not txt:
                    continue
                push_history(conn_tgt, int(tgt_parent), txt)

        children, related = get_children_related(conn_src, pid)

        # children
        for cid in children:
            cdata = batch_get(conn_src, [cid], child_fields).get("value", [])
            if not cdata:
                continue
            cf = (cdata[0].get("fields") or {})
            src_type = (cf.get("System.WorkItemType") or "").strip()
            tgt_type = CHILD_TYPE_MAP_LOWER.get(src_type.lower())
            if not tgt_type:
                continue

            existing = find_target_by_reflected(conn_tgt, cid)
            if existing:
                tgt_child = existing
            else:
                cfields = {
                    "System.Title": cf.get("System.Title", f"Migrated {cid}"),
                    "System.Description": (cf.get("System.Description") or ""),
                    "System.Tags": (cf.get("System.Tags") or ""),
                    REFLECTED: str(cid),
                    "System.AreaPath": area_root if args.force_root else remap_root(cf.get("System.AreaPath"), conn_src.project, area_root),
                    "System.IterationPath": iter_root if args.force_root else remap_root(cf.get("System.IterationPath"), conn_src.project, iter_root),
                }
                if args.dry_run:
                    tgt_child = -1
                else:
                    created = create_work_item(conn_tgt, tgt_type, cfields, parent_id=int(tgt_parent) if int(tgt_parent) > 0 else None)
                    tgt_child = int(created.get("id"))
                created_other += 1
            if args.with_comments and (not args.dry_run) and int(tgt_child) > 0:
                for c in get_comments(conn_src, cid):
                    txt = c.get("text") or ""
                    if txt:
                        push_history(conn_tgt, int(tgt_child), txt)

        # related
        for rid in related:
            rdata = batch_get(conn_src, [rid], child_fields).get("value", [])
            if not rdata:
                continue
            rf = (rdata[0].get("fields") or {})
            src_type = (rf.get("System.WorkItemType") or "").strip()
            tgt_type = CHILD_TYPE_MAP_LOWER.get(src_type.lower())
            if not tgt_type:
                continue

            tgt_rel = find_target_by_reflected(conn_tgt, rid)
            if not tgt_rel:
                rfields = {
                    "System.Title": rf.get("System.Title", f"Migrated {rid}"),
                    "System.Description": (rf.get("System.Description") or ""),
                    "System.Tags": (rf.get("System.Tags") or ""),
                    REFLECTED: str(rid),
                    "System.AreaPath": area_root if args.force_root else remap_root(rf.get("System.AreaPath"), conn_src.project, area_root),
                    "System.IterationPath": iter_root if args.force_root else remap_root(rf.get("System.IterationPath"), conn_src.project, iter_root),
                }
                if args.dry_run:
                    tgt_rel = -1
                else:
                    created = create_work_item(conn_tgt, tgt_type, rfields)
                    tgt_rel = int(created.get("id"))
                created_other += 1

            if (not args.dry_run) and int(tgt_parent) > 0 and int(tgt_rel) > 0:
                add_related_link(conn_tgt, int(tgt_parent), int(tgt_rel))
                related_links += 1

    print("\n----- SUMMARY -----")
    print(f"Parents created: {created_parent}")
    print(f"Children/Related created: {created_other}")
    print(f"Related links added: {related_links}")


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print("ERROR:", ex)
        sys.exit(1)
