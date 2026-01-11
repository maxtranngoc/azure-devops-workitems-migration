# Usage examples

This document provides common usage examples for the Azure DevOps work items migration scripts.

All scripts rely on **environment variables** for configuration.
You can also pass values explicitly via CLI arguments.

---

## Environment variables

Required for most scripts:

- `ADO_SOURCE_ORG_URL`
- `ADO_SOURCE_PROJECT`
- `ADO_SOURCE_PAT`
- `ADO_TARGET_ORG_URL`
- `ADO_TARGET_PROJECT`
- `ADO_TARGET_PAT`

Optional:
- `ADO_ATTACHMENTS_DIR` (for attachments scripts)

---

## PowerShell examples (Windows)

### Configure environment

```powershell
$env:ADO_SOURCE_ORG_URL="https://dev.azure.com/sourceOrg"
$env:ADO_SOURCE_PROJECT="SourceProject"
$env:ADO_SOURCE_PAT="xxxxx"

$env:ADO_TARGET_ORG_URL="https://dev.azure.com/targetOrg"
$env:ADO_TARGET_PROJECT="TargetProject"
$env:ADO_TARGET_PAT="yyyyy"

$env:ADO_ATTACHMENTS_DIR="C:\temp\ado_attachments"

```bash
export ADO_SOURCE_ORG_URL="https://dev.azure.com/sourceOrg"
export ADO_SOURCE_PROJECT="SourceProject"
export ADO_SOURCE_PAT="xxxxx"

export ADO_TARGET_ORG_URL="https://dev.azure.com/targetOrg"
export ADO_TARGET_PROJECT="TargetProject"
export ADO_TARGET_PAT="yyyyy"

export ADO_ATTACHMENTS_DIR="$HOME/ado_attachments"

## Copy parent work items with children
python scripts/copy_parent_workitems_with_children.py --max 10 --with-comments
Dry run (no creation): python scripts/copy_parent_workitems_with_children.py --max 10 --dry-run

## Copy the last N work bundles
python scripts/copy_last_workbundles.py --top 5

## Copy a single work item
python scripts/copy_single_workitem.py --id 12345

## Compare source / target fields
python scripts/diagnostic_fields.py --type "Work Bundle" --type "User Story"

## Download attachments
python scripts/download_attachments.py --max 100

## Upload attachments to target
python scripts/upload_attachments.py

## Link related work items to work bundles
python scripts/link_workbundles_related.py --max 50

## Copy parent work items with children
python scripts/copy_parent_workitems_with_children.py --max 10 --with-comments

## Copy a single work item
python scripts/copy_single_workitem.py --id 12345

