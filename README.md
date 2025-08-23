# pco-fibery-sync

A tiny AWS Lambda (Node 20) that syncs People & Households between Planning Center and Fibery.

## Features
- Polls both systems for changes since last run
- Upserts by external IDs (PCO Person ID, Household ID)
- Keeps People↔Household relationship in sync
- Stores cursors in SSM Parameter Store
- EventBridge scheduled (e.g., every 15 minutes)

## Prereqs
- AWS account with SAM CLI **or** deploy via Console
- PCO Personal Access Token (APP ID & SECRET)
- Fibery host (e.g., `yourcompany.fibery.io`), Space name (e.g., `Planning Center Sync`), API token

## Quick start (SAM CLI)

```bash
# 1) Clone
git clone <your fork>
cd pco-fibery-sync

# 2) Configure environment (copy and edit)
cp env.example.json .env.json
# set: PCO_APP_ID, PCO_SECRET, FIBERY_HOST, FIBERY_SPACE, FIBERY_TOKEN, SSM_PATH

# 3) Build & deploy (guided the first time)
sam build
sam deploy --guided
```

During guided deploy, set environment variables from your `env.json` (or input them interactively). The SAM template creates:

* One Lambda function (Node 20)
* An EventBridge rule (disabled by default; enable after testing)
* Minimal IAM for SSM:Get/PutParameter + Logs

## Configure schedule

* After first successful test, enable the rule and set `rate(15 minutes)` (or your preference).

## Environment variables

* `PCO_APP_ID` – Planning Center Application ID (Personal Access Token ID)
* `PCO_SECRET` – Planning Center Secret
* `FIBERY_HOST` – e.g. `yourcompany.fibery.io`
* `FIBERY_SPACE` – e.g. `Planning Center Sync`
* `FIBERY_TOKEN` – Fibery personal API token
* `SSM_PATH` – e.g. `/pco-fibery-sync` (SSM param path prefix)

## First run

* Invoke the Lambda from the Console (Test). Check CloudWatch Logs for stats. Two SSM parameters will be created: `{SSM_PATH}/pcoLastSync` and `{SSM_PATH}/fiberyLastSync`.

## Notes on schemas

* **Fibery**: Ensure your Space/DB names & field API names match your Fibery setup:

  * People: `Name` (title), `Person ID` (text), `Household` (single relation)
  * Household: `Name` (title), `Household ID` (text), `Members` (collection of People)
* **PCO**: This repo assumes PCO People/Households endpoints exist under `/people/v2`. Confirm your org has permission. Update payloads in `src/pco.js` where marked.

## Safety & limits

* Idempotent upserts by ID fields
* Exponential backoff for HTTP 429
* Guardrails: per-run max work batch (configurable in code)

## Deletions

* By default, deletes are **not** propagated. Consider adding soft-archive semantics later.

## License

MIT
