#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compare fields between SOURCE and TARGET work item types.

Example:
  python scripts/diagnostic_fields.py --type "Work Bundle" --type "User Story"

Required env/args:
  ADO_SOURCE_ORG_URL, ADO_SOURCE_PROJECT, ADO_SOURCE_PAT
  ADO_TARGET_ORG_URL, ADO_TARGET_PROJECT, ADO_TARGET_PAT
"""
from __future__ import annotations

import argparse
import sys
from typing import Dict
from urllib.parse import quote

from _common import API_VERSION, AdoConn, env, require, http_json


def list_type_fields(conn: AdoConn, witype: str) -> Dict[str, str]:
    t = quote(witype, safe="")
    url = f"{conn.org_url}/{conn.project}/_apis/wit/workitemtypes/{t}?api-version={API_VERSION}"
    res = http_json("GET", url, conn.auth) or {}
    fields = res.get("fields") or []
    out: Dict[str, str] = {}
    for f in fields:
        ref = f.get("referenceName")
        name = f.get("name")
        if ref:
            out[ref] = name or ref
    return out


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-org", default=env("ADO_SOURCE_ORG_URL"))
    ap.add_argument("--source-project", default=env("ADO_SOURCE_PROJECT"))
    ap.add_argument("--source-pat", default=env("ADO_SOURCE_PAT"))
    ap.add_argument("--target-org", default=env("ADO_TARGET_ORG_URL"))
    ap.add_argument("--target-project", default=env("ADO_TARGET_PROJECT"))
    ap.add_argument("--target-pat", default=env("ADO_TARGET_PAT"))
    ap.add_argument("--type", action="append", dest="types", required=True, help="Work item type (repeatable)")
    return ap.parse_args()


def main():
    a = parse_args()
    src = AdoConn(require(a.source_org,"source_org","ADO_SOURCE_ORG_URL"),
                 require(a.source_project,"source_project","ADO_SOURCE_PROJECT"),
                 require(a.source_pat,"source_pat","ADO_SOURCE_PAT"))
    tgt = AdoConn(require(a.target_org,"target_org","ADO_TARGET_ORG_URL"),
                 require(a.target_project,"target_project","ADO_TARGET_PROJECT"),
                 require(a.target_pat,"target_pat","ADO_TARGET_PAT"))

    for t in a.types:
        sf = list_type_fields(src, t)
        tf = list_type_fields(tgt, t)

        sset, tset = set(sf.keys()), set(tf.keys())
        only_src = sorted(sset - tset)
        only_tgt = sorted(tset - sset)
        both = sorted(sset & tset)

        print("\n" + "=" * 80)
        print(f"TYPE: {t}")
        print(f"Source fields: {len(sset)} | Target fields: {len(tset)} | Common: {len(both)}")
        print("-" * 80)

        if only_src:
            print("\nOnly in SOURCE:")
            for ref in only_src:
                print(f"  - {ref} ({sf.get(ref)})")

        if only_tgt:
            print("\nOnly in TARGET:")
            for ref in only_tgt:
                print(f"  - {ref} ({tf.get(ref)})")

        print("\nCommon (sample 30):")
        for ref in both[:30]:
            print(f"  - {ref}")

    print("\nDone.")


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        print("ERROR:", ex)
        sys.exit(1)
