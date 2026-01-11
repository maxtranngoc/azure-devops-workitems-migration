#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Copy the N most recently created Work Bundles from source -> target (stdlib only).

Required env/args:
  ADO_SOURCE_ORG_URL, ADO_SOURCE_PROJECT, ADO_SOURCE_PAT
  ADO_TARGET_ORG_URL, ADO_TARGET_PROJECT, ADO_TARGET_PAT

Idempotence: checks Custom.ReflectedWorkItemId in target.
"""
from __future__ import annotations

import argparse
import sys
from urllib.parse import quote

from _common import API_VERSION, AdoConn, env, require, http_json

REFLECTED = "Custom.ReflectedWorkItemId"
SRC_WB_TYPES = ("Work Bundle", "Workbundle", "WorkBundle")
TARGET_WB_TYPE = "Work Bundle"


def wiql_created_wb(conn: AdoConn):
    in_types = ",".join([f"'{t}'" for t in SRC_WB_TYPES])
    q = f"""
      SELECT [System.Id] FROM WorkItems
      WHERE [System.TeamProject] = '{conn.project}'
        AND [System.WorkItemType] IN ({in_types})
      ORDER BY [System.CreatedDate] DESC
    """
    url = f"{conn.org_url}/{conn.project}/_apis/wit/wiql?api-version={API_VERSION}"
    return http_json("POST", url, conn.auth, {"query": " ".join(q.split())}) or {}


def batch_get(conn: AdoConn, ids):
    url = f"{conn.org_url}/_apis/wit/workitemsbatch?api-version={API_VERSION}"
    fields = ["System.Id","System.Title","System.Description","System.State","System.Tags","System.AreaPath","System.IterationPath"]
    return http_json("POST", url, conn.auth, {"ids": list(ids), "fields": fields}) or {}


def find_target_by_reflected(conn_tgt: AdoConn, source_id: int):
    q = f"""
      SELECT [System.Id] FROM WorkItems
      WHERE [System.TeamProject] = '{conn_tgt.project}' AND [{REFLECTED}] = '{source_id}'
    """
    url = f"{conn_tgt.org_url}/{conn_tgt.project}/_apis/wit/wiql?api-version={API_VERSION}"
    res = http_json("POST", url, conn_tgt.auth, {"query": " ".join(q.split())}) or {}
    ids = [wi["id"] for wi in res.get("workItems", [])]
    return ids[0] if ids else None


def create_work_item(conn_tgt: AdoConn, fields: dict):
    t = quote(TARGET_WB_TYPE, safe="")
    url = f"{conn_tgt.org_url}/{conn_tgt.project}/_apis/wit/workitems/${t}?api-version={API_VERSION}"
    patch = [{"op":"add","path":f"/fields/{k}","value":v} for k,v in fields.items() if v is not None]
    return http_json("PATCH", url, conn_tgt.auth, patch, "application/json-patch+json") or {}


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-org", default=env("ADO_SOURCE_ORG_URL"))
    ap.add_argument("--source-project", default=env("ADO_SOURCE_PROJECT"))
    ap.add_argument("--source-pat", default=env("ADO_SOURCE_PAT"))
    ap.add_argument("--target-org", default=env("ADO_TARGET_ORG_URL"))
    ap.add_argument("--target-project", default=env("ADO_TARGET_PROJECT"))
    ap.add_argument("--target-pat", default=env("ADO_TARGET_PAT"))
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--area", default=env("ADO_TARGET_AREA_ROOT"))
    ap.add_argument("--iteration", default=env("ADO_TARGET_ITERATION_ROOT"))
    return ap.parse_args()


def main():
    a = parse_args()
    src = AdoConn(require(a.source_org,"source_org","ADO_SOURCE_ORG_URL"),
                 require(a.source_project,"source_project","ADO_SOURCE_PROJECT"),
                 require(a.source_pat,"source_pat","ADO_SOURCE_PAT"))
    tgt = AdoConn(require(a.target_org,"target_org","ADO_TARGET_ORG_URL"),
                 require(a.target_project,"target_project","ADO_TARGET_PROJECT"),
                 require(a.target_pat,"target_pat","ADO_TARGET_PAT"))
    area = (a.area or "").strip() or tgt.project
    itr = (a.iteration or "").strip() or tgt.project

    ids = [wi["id"] for wi in (wiql_created_wb(src).get("workItems") or [])][:a.top]
    if not ids:
        print("No work items found.")
        return

    items = batch_get(src, ids).get("value", []) or []
    created = skipped = 0
    for wi in items:
        sid = int(wi["id"])
        if find_target_by_reflected(tgt, sid):
            skipped += 1
            continue
        f = wi.get("fields") or {}
        fields = {
            "System.Title": f.get("System.Title", f"Migrated {sid}"),
            "System.Description": f.get("System.Description") or "",
            "System.Tags": f.get("System.Tags") or "",
            REFLECTED: str(sid),
            "System.AreaPath": area,
            "System.IterationPath": itr,
        }
        print(f"+ create {sid}")
        if not a.dry_run:
            res = create_work_item(tgt, fields)
            print(f"  -> target #{res.get('id')}")
        created += 1

    print("\n----- SUMMARY -----")
    print(f"Selected: {len(items)} | Created: {created} | Skipped: {skipped}")


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print("ERROR:", ex)
        sys.exit(1)
