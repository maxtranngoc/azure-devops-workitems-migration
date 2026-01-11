#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Download attachments/images from SOURCE work items that correspond to TARGET work items
mapped via Custom.ReflectedWorkItemId.

Writes files to: <OUT_DIR>/<targetId>_from_<sourceId>/

Required env/args:
  ADO_SOURCE_ORG_URL, ADO_SOURCE_PROJECT, ADO_SOURCE_PAT
  ADO_TARGET_ORG_URL, ADO_TARGET_PROJECT, ADO_TARGET_PAT

Output directory:
  --out-dir or env ADO_ATTACHMENTS_DIR (required)
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from _common import API_VERSION, AdoConn, env, require, http_json, http_binary

REFLECTED = "Custom.ReflectedWorkItemId"


def sanitize_filename(name: str) -> str:
    return "".join(c for c in name if c not in r'<>:"/\|?*')


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


def get_workitem_with_relations(conn: AdoConn, wid: int):
    url = f"{conn.org_url}/{conn.project}/_apis/wit/workitems/{wid}?$expand=relations&api-version={API_VERSION}"
    return http_json("GET", url, conn.auth) or {}


def download_for_pair(conn_src: AdoConn, conn_tgt: AdoConn, out_dir: Path, tgt_id: int, src_id: int) -> int:
    wi_src = get_workitem_with_relations(conn_src, src_id)
    fields = wi_src.get("fields", {}) or {}
    rels = wi_src.get("relations", []) or []

    attachments = {}  # url -> filename

    # 1) attachments via relations
    for r in rels:
        rel_type = (r.get("rel") or "").lower()
        if "attachedfile" in rel_type or "attachedimage" in rel_type:
            href = r.get("url")
            if not href:
                continue
            name = r.get("attributes", {}).get("name") or f"attachment_{src_id}"
            attachments[href] = sanitize_filename(name)

    # 2) attachments URLs found inside HTML fields
    pattern = re.compile(r"https://dev\.azure\.com/[^\"'<> ]+/_apis/wit/attachments/[^\"'<> ]+", re.IGNORECASE)
    for v in fields.values():
        if isinstance(v, str):
            for m in pattern.findall(v):
                url = m
                if url not in attachments:
                    if "fileName=" in url:
                        name = url.split("fileName=")[-1]
                    else:
                        name = url.rsplit("/", 1)[-1]
                    attachments[url] = sanitize_filename(name)

    if not attachments:
        return 0

    folder = out_dir / f"{tgt_id}_from_{src_id}"
    folder.mkdir(parents=True, exist_ok=True)

    count = 0
    for url, fname in attachments.items():
        dest = folder / fname
        if dest.exists():
            continue
        blob = http_binary("GET", url, conn_src.auth)
        dest.write_bytes(blob)
        count += 1
    return count


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-org", default=env("ADO_SOURCE_ORG_URL"))
    ap.add_argument("--source-project", default=env("ADO_SOURCE_PROJECT"))
    ap.add_argument("--source-pat", default=env("ADO_SOURCE_PAT"))
    ap.add_argument("--target-org", default=env("ADO_TARGET_ORG_URL"))
    ap.add_argument("--target-project", default=env("ADO_TARGET_PROJECT"))
    ap.add_argument("--target-pat", default=env("ADO_TARGET_PAT"))
    ap.add_argument("--out-dir", default=env("ADO_ATTACHMENTS_DIR"))
    ap.add_argument("--max", type=int, default=None)
    return ap.parse_args()


def main():
    a = parse_args()
    src = AdoConn(require(a.source_org,"source_org","ADO_SOURCE_ORG_URL"),
                 require(a.source_project,"source_project","ADO_SOURCE_PROJECT"),
                 require(a.source_pat,"source_pat","ADO_SOURCE_PAT"))
    tgt = AdoConn(require(a.target_org,"target_org","ADO_TARGET_ORG_URL"),
                 require(a.target_project,"target_project","ADO_TARGET_PROJECT"),
                 require(a.target_pat,"target_pat","ADO_TARGET_PAT"))
    out_dir = Path(require(a.out_dir, "out_dir", "ADO_ATTACHMENTS_DIR")).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    ids = get_target_ids(tgt, a.max)
    total = 0
    for tgt_id in ids:
        wi_tgt = http_json("GET", f"{tgt.org_url}/{tgt.project}/_apis/wit/workitems/{tgt_id}?api-version={API_VERSION}", tgt.auth) or {}
        src_id = (wi_tgt.get("fields") or {}).get(REFLECTED)
        if not src_id:
            continue
        try:
            src_id = int(str(src_id))
        except Exception:
            continue
        total += download_for_pair(src, tgt, out_dir, tgt_id, src_id)

    print(f"Downloaded files: {total} into {out_dir}")


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print("ERROR:", ex)
        sys.exit(1)
