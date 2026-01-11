# Changelog

All notable changes to this project will be documented in this file.

The format is inspired by *Keep a Changelog* and this project follows semantic versioning.

---

## [1.0.0] – 2026-01-11

### Added
- Initial public release of Azure DevOps work items migration scripts
- Copy parent work items with children and related links
- Copy last N work bundles
- Copy a single work item by ID
- Diagnostic tool to compare source/target process fields
- Download attachments from source work items
- Upload attachments to target work items
- Link existing work items to target work bundles
- Support for comments migration via `System.History`
- Stdlib-only implementation (no external dependencies)

### Security
- Removed all hardcoded PATs, org URLs, and project names
- Enforced configuration via environment variables or CLI arguments
- Added `.gitignore` to prevent secrets and local artifacts from being committed
