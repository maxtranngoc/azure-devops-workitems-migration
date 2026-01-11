#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Link target Work Bundles to other existing target work items using their mapping to source IDs.

This script does NOT create work items; it only adds Related links in the target, based on:
- target Work Bundle has Custom.ReflectedWorkItemId = source WB id
- we read source WB relations, keep only allowed types, then find corresponding target ids by Custom.ReflectedWorkItemId
- we add Related links in target WB

Required env/args:
  ADO_SOURCE_ORG_URL, ADO_SOURCE_PROJECT, ADO_SOURCE_PAT
  ADO_TARGET_ORG_URL, ADO_TARGET_PROJECT, ADO_TARGET_PAT

Config:
  Edit ALLOWED_LINK_TYPES_LOWER in this file for your process types.
"""
from __future__ import annotations

import argparse
import sys
from typing import Optional, Set, List
from urllib.parse import quote

from _common import API_VERSION, AdoConn, env, require, http_json

REFLECTED = "Custom.ReflectedWorkItemId"
TARGET_WB_TYPE = "Work Bundle"

ALLOWED_LINK_TYPES_LOWER: Set[str] = {
    "bug",
    "incident",
    # add more if needed
}


def wiql(conn: AdoConn, query: str):
    url = f"{conn.org_url}/{conn.project}/_apis/wit/wiql?api-version={API_VERSION}"
    return http_json("POST", url, conn.auth, {"query": " ".join(query.split())}) or {}


def find_target_by_reflected(conn_tgt: AdoConn, source_id: int) -> Optional[int]:
    q = f"""
      SELECT [System.Id] FROM WorkItems
      WHERE [System.TeamProject] = '{conn_tgt.project}' AND [{REFLECTED}] = '{source_id}'
    """
    res = wiql(conn_tgt, q) or {}
    ids = [wi["id"] for wi in res.get("workItems", [])]
    return ids[0] if ids else None


def get_source_related_ids(conn_src: AdoConn, wb_id: int) -> List[int]:
    url = f"{conn_src.org_url}/{conn_src.project}/_apis/wit/workitems/{wb_id}?$expand=relations&api-version={API_VERSION}"
    wi = http_json("GET", url, conn_src.auth) or {}
    related = []
    for r in wi.get("relations", []) or []:
        rel = (r.get("rel") or "").lower()
        if not rel.endswith("related"):
            continue
        href = r.get("url") or ""
        try:
            rid = int(href.rsplit("/", 1)[-1])
            related.append(rid)
        except Exception:
            pass
    return related


def get_types(conn_src: AdoConn, ids: List[int]):
    if not ids:
        return {}
    url = f"{conn_src.org_url}/_apis/wit/workitemsbatch?api-version={API_VERSION}"
    res = http_json("POST", url, conn_src.auth, {"ids": ids, "fields": ["System.WorkItemType"]}) or {}
    out = {}
    for it in (res.get("value") or []):
        wid = it.get("id")
        wtype = ((it.get("fields") or {}).get("System.WorkItemType") or "").strip()
        if wid:
            out[int(wid)] = wtype
    return out


def add_related_link(conn_tgt: AdoConn, tgt_parent: int, tgt_other: int):
    other_url = f"{conn_tgt.org_url}/{conn_tgt.project}/_apis/wit/workItems/{tgt_other}"
    patch = [{"op":"add","path":"/relations/-","value":{"rel":"System.LinkTypes.Related","url":other_url}}]
    url = f"{conn_tgt.org_url}/{conn_tgt.project}/_apis/wit/workitems/{tgt_parent}?api-version={API_VERSION}"
    http_json("PATCH", url, conn_tgt.auth, patch, "application/json-patch+json")


def get_target_wb_ids(conn_tgt: AdoConn, max_items=None):
    q = f"""
      SELECT [System.Id] FROM WorkItems
      WHERE [System.TeamProject] = '{conn_tgt.project}'
        AND [System.WorkItemType] = '{TARGET_WB_TYPE}'
      ORDER BY [System.Id]
    """
    res = wiql(conn_tgt, q) or {}
    ids = [wi["id"] for wi in res.get("workItems", [])]
    return ids[:max_items] if max_items else ids


def read_workitem(conn: AdoConn, wid: int):
    url = f"{conn.org_url}/{conn.project}/_apis/wit/workitems/{wid}?api-version={API_VERSION}"
    return http_json("GET", url, conn.auth) or {}


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-org", default=env("ADO_SOURCE_ORG_URL"))
    ap.add_argument("--source-project", default=env("ADO_SOURCE_PROJECT"))
    ap.add_argument("--source-pat", default=env("ADO_SOURCE_PAT"))
    ap.add_argument("--target-org", default=env("ADO_TARGET_ORG_URL"))
    ap.add_argument("--target-project", default=env("ADO_TARGET_PROJECT"))
    ap.add_argument("--target-pat", default=env("ADO_TARGET_PAT"))
    ap.add_argument("--max", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    return ap.parse_args()


def main():
    a = parse_args()
    src = AdoConn(require(a.source_org,"source_org","ADO_SOURCE_ORG_URL"),
                 require(a.source_project,"source_project","ADO_SOURCE_PROJECT"),
                 require(a.source_pat,"source_pat","ADO_SOURCE_PAT"))
    tgt = AdoConn(require(a.target_org,"target_org","ADO_TARGET_ORG_URL"),
                 require(a.target_project,"target_project","ADO_TARGET_PROJECT"),
                 require(a.target_pat,"target_pat","ADO_TARGET_PAT"))

    wb_ids = get_target_wb_ids(tgt, a.max)
    total = 0
    for tgt_wb in wb_ids:
        wi = read_workitem(tgt, tgt_wb)
        src_wb = (wi.get("fields") or {}).get(REFLECTED)
        if not src_wb:
            continue
        try:
            src_wb = int(str(src_wb))
        except Exception:
            continue

        rel_ids = get_source_related_ids(src, src_wb)
        types = get_types(src, rel_ids)
        for rid in rel_ids:
            tname = (types.get(rid) or "").lower()
            if tname not in ALLOWED_LINK_TYPES_LOWER:
                continue
            tgt_id = find_target_by_reflected(tgt, rid)
            if not tgt_id:
                continue
            if a.dry_run:
                print(f"Would link WB#{tgt_wb} -> {tname}#{tgt_id}")
            else:
                add_related_link(tgt, tgt_wb, tgt_id)
                total += 1

    print(f"Related links added: {total}")


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print("ERROR:", ex)
        sys.exit(1)
