#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Upload local attachments to TARGET work items.

Expected local structure (created by download_attachments.py):
  <ATTACH_DIR>/<tgtId>_from_<srcId>/*

Required env/args:
  ADO_TARGET_ORG_URL, ADO_TARGET_PROJECT, ADO_TARGET_PAT
  ADO_ATTACHMENTS_DIR (or --attach-dir)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from urllib.parse import quote

from _common import API_VERSION, AdoConn, env, require, http_json, http_binary

REFLECTED = "Custom.ReflectedWorkItemId"


def get_target_ids(conn_tgt: AdoConn, max_items=None):
    query = f"""
    SELECT [System.Id]
    FROM WorkItems
    WHERE [System.TeamProject] = '{conn_tgt.project}'
    ORDER BY [System.Id]
    """
    url = f"{conn_tgt.org_url}/{conn_tgt.project}/_apis/wit/wiql?api-version={API_VERSION}"
    res = http_json("POST", url, conn_tgt.auth, {"query": query}) or {}
    ids = [wi["id"] for wi in res.get("workItems", [])]
    return ids[:max_items] if max_items else ids


def get_workitem_with_rels(conn: AdoConn, wid: int):
    url = f"{conn.org_url}/{conn.project}/_apis/wit/workitems/{wid}?$expand=relations&api-version={API_VERSION}"
    return http_json("GET", url, conn.auth) or {}


def upload_attachment(conn_tgt: AdoConn, file_path: Path) -> str:
    fname_enc = quote(file_path.name, safe="")
    url = f"{conn_tgt.org_url}/{conn_tgt.project}/_apis/wit/attachments?fileName={fname_enc}&api-version={API_VERSION}"
    raw = http_binary("POST", url, conn_tgt.auth, body=file_path.read_bytes(), content_type="application/octet-stream")
    res = {} if not raw else __import__("json").loads(raw.decode("utf-8"))
    href = res.get("url")
    if not href:
        raise RuntimeError("No attachment url returned by ADO.")
    return href


def attach_to_workitem(conn_tgt: AdoConn, tgt_id: int, attachment_url: str, comment: str):
    patch = [{
        "op": "add",
        "path": "/relations/-",
        "value": {"rel": "AttachedFile", "url": attachment_url, "attributes": {"comment": comment}},
    }]
    url = f"{conn_tgt.org_url}/{conn_tgt.project}/_apis/wit/workitems/{tgt_id}?api-version={API_VERSION}"
    http_json("PATCH", url, conn_tgt.auth, patch, "application/json-patch+json")


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target-org", default=env("ADO_TARGET_ORG_URL"))
    ap.add_argument("--target-project", default=env("ADO_TARGET_PROJECT"))
    ap.add_argument("--target-pat", default=env("ADO_TARGET_PAT"))
    ap.add_argument("--attach-dir", default=env("ADO_ATTACHMENTS_DIR"))
    ap.add_argument("--max", type=int, default=None)
    return ap.parse_args()


def main():
    a = parse_args()
    tgt = AdoConn(require(a.target_org,"target_org","ADO_TARGET_ORG_URL"),
                 require(a.target_project,"target_project","ADO_TARGET_PROJECT"),
                 require(a.target_pat,"target_pat","ADO_TARGET_PAT"))
    attach_dir = Path(require(a.attach_dir, "attach_dir", "ADO_ATTACHMENTS_DIR")).expanduser().resolve()
    if not attach_dir.exists():
        raise SystemExit(f"Attachments folder does not exist: {attach_dir}")

    ids = get_target_ids(tgt, a.max)
    total = 0
    for tgt_id in ids:
        wi = get_workitem_with_rels(tgt, tgt_id)
        src_id = (wi.get("fields") or {}).get(REFLECTED)
        if not src_id:
            continue
        try:
            src_id = int(str(src_id))
        except Exception:
            continue

        folder = attach_dir / f"{tgt_id}_from_{src_id}"
        if not folder.exists():
            continue

        existing_names = set()
        for r in wi.get("relations", []) or []:
            rel = (r.get("rel") or "").lower()
            if "attachedfile" in rel or "attachedimage" in rel:
                name = (r.get("attributes") or {}).get("name")
                if name:
                    existing_names.add(name)

        for f in folder.iterdir():
            if not f.is_file():
                continue
            if f.name in existing_names:
                continue
            href = upload_attachment(tgt, f)
            attach_to_workitem(tgt, tgt_id, href, comment=f"Migrated attachment from source #{src_id}")
            total += 1

    print(f"Total attached files: {total}")


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print("ERROR:", ex)
        sys.exit(1)
