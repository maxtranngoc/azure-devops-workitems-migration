#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Copy a single work item by ID from source -> target (stdlib only).

Required env/args:
  ADO_SOURCE_ORG_URL, ADO_SOURCE_PROJECT, ADO_SOURCE_PAT
  ADO_TARGET_ORG_URL, ADO_TARGET_PROJECT, ADO_TARGET_PAT
"""
from __future__ import annotations

import argparse
import sys
from urllib.parse import quote

from _common import API_VERSION, AdoConn, env, require, http_json

REFLECTED = "Custom.ReflectedWorkItemId"
TARGET_WB_TYPE = "Work Bundle"
SRC_WB_TYPES = ("Work Bundle","Workbundle","WorkBundle")


def get_work_item(conn: AdoConn, wid: int):
    url = f"{conn.org_url}/{conn.project}/_apis/wit/workitems/{wid}?api-version={API_VERSION}"
    return http_json("GET", url, conn.auth) or {}


def create_work_item(conn: AdoConn, type_name: str, fields: dict):
    t = quote(type_name, safe="")
    url = f"{conn.org_url}/{conn.project}/_apis/wit/workitems/${t}?api-version={API_VERSION}"
    patch = [{"op":"add","path":f"/fields/{k}","value":v} for k,v in fields.items() if v is not None]
    return http_json("PATCH", url, conn.auth, patch, "application/json-patch+json") or {}


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-org", default=env("ADO_SOURCE_ORG_URL"))
    ap.add_argument("--source-project", default=env("ADO_SOURCE_PROJECT"))
    ap.add_argument("--source-pat", default=env("ADO_SOURCE_PAT"))
    ap.add_argument("--target-org", default=env("ADO_TARGET_ORG_URL"))
    ap.add_argument("--target-project", default=env("ADO_TARGET_PROJECT"))
    ap.add_argument("--target-pat", default=env("ADO_TARGET_PAT"))
    ap.add_argument("--id", type=int, required=True)
    ap.add_argument("--target-type", default=TARGET_WB_TYPE, help="Target work item type")
    ap.add_argument("--area", default=env("ADO_TARGET_AREA_ROOT"))
    ap.add_argument("--iteration", default=env("ADO_TARGET_ITERATION_ROOT"))
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
    area = (a.area or "").strip() or tgt.project
    itr = (a.iteration or "").strip() or tgt.project

    wi = get_work_item(src, a.id)
    f = wi.get("fields") or {}
    fields = {
        "System.Title": f.get("System.Title", f"Migrated {a.id}"),
        "System.Description": f.get("System.Description") or "",
        "System.Tags": f.get("System.Tags") or "",
        REFLECTED: str(a.id),
        "System.AreaPath": area,
        "System.IterationPath": itr,
    }
    if a.dry_run:
        print(fields)
        return
    res = create_work_item(tgt, a.target_type, fields)
    print(f"Source #{a.id} -> Target #{res.get('id')} ({a.target_type})")


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print("ERROR:", ex)
        sys.exit(1)
