# ProjectAI - MDRM Loader (Spring Boot)

## Overview
ProjectAI is a Spring Boot service that ingests MDRM CSV data into PostgreSQL with run tracking, error logging, a master table for reporting, and precomputed run summaries for fast UI exploration.

## Latest Updates (2026-02-28)
- Discovery-first UX with AG Grid search results and pagination.
- Search results simplified to core business columns:
  - MDRM Code
  - Description (with tooltip for long text)
  - Reporting Form
- Favorites/bookmarks system with user-specific persistence:
  - Save MDRMs to default list or groups
  - Create/rename/delete groups
  - Move/remove bookmarks
  - Dedicated `Bookmarks` view
- Authentication upgrades:
  - Login, Sign Up, Logout
  - Reset Password (email + new password)
  - Mode-based auth UI (fields shown per selected action)
  - Password visibility toggle and confirm-password checks
- Dark/light theme improvements with reliable switching and readable contrast.
- Header/nav redesign with icon pills and theme-safe color treatment.
- Dashboard KPI refresh:
  - `Unique Reports`
  - `MDRMs`
  - `Current View`
  - locale-formatted counts (e.g., `12,345`)

Current ingestion flow (`POST /api/mdrm/load`):
1. Clean all MDRM tables (`mdrm_run_summary`, `mdrm_run_error`, `mdrm_master`, `mdrm_run_master`, and rebuild `mdrm_staging`)
2. Discover files by pattern `mdrm.migration-file-pattern` and keep only `MDRM_mmyy.csv`
3. Sort files oldest-first by `mmyy` (year then month)
4. For each file, create `run_id` and insert one row in `mdrm_run_master` with `file_name`
5. Ignore CSV line 1 (`PUBLIC` metadata)
6. Use CSV line 2 as headers
7. Recreate `mdrm_staging` from sanitized CSV headers (+ `run_id`)
8. Batch load source rows to staging
9. Promote staging rows to `mdrm_master` (PostgreSQL stored function path)
10. Write invalid rows to `mdrm_run_error`
11. Update run counts in `mdrm_run_master` and refresh `mdrm_run_summary` (including `file_name`)

## Data Model
- `mdrm_run_master`
  - `run_id` (PK)
  - `run_datetime`
  - `file_name`
  - `num_file_records`
  - `num_records_ingested`
  - `num_records_error`
- `mdrm_staging`
  - recreated every run from source headers
  - includes `run_id`
- `mdrm_master`
  - keeps source columns + derived fields:
    - `start_date_utc`
    - `end_date_utc`
    - `mdrm_code` (`mnemonic + item_code`)
    - `is_active` (`Y/N`)
- `mdrm_run_error`
  - `run_id`
  - `raw_record`
  - `error_description`
- `mdrm_run_summary`
  - `run_id` + `reporting_form` (composite PK)
  - `file_name`
  - `total_unique_mdrms`
  - `active_mdrms`
  - `inactive_mdrms`
  - `updated_mdrms`
  - populated during each successful load and backfilled for older runs at startup

## Derived Rules
- Date parsing format: `M/d/yyyy h:mm:ss a`
- `is_active = 'Y'` when:
  - end-date year is `9999`, or
  - end date is greater than current UTC time
- otherwise `is_active = 'N'`
- Rows with null/invalid dates are skipped from `mdrm_master` and logged to `mdrm_run_error`
- Run summary classification by unique `mdrm_code` per run/report:
  - `active`: has only `is_active = 'Y'`
  - `inactive`: has only `is_active = 'N'`
  - `updated`: has both active and inactive records
  - `total_unique_mdrms`: count of distinct `mdrm_code`

## Tech Stack
- Java 17
- Spring Boot 3.3.5
- Spring JDBC
- PostgreSQL (runtime)
- H2 (tests)
- Apache Commons CSV
- Maven

## Project Structure
- Backend logic: `src/main/java/com/projectai/projectai/mdrm`
- Config: `src/main/resources/application.properties`
- Static UI: `src/main/resources/static`
- Tests: `src/test/java`
- Prompt transcripts: `src/main/resources/prompts`

## Configuration
Set in `application.properties`:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/projectAI?reWriteBatchedInserts=true
spring.datasource.username=postgres
spring.datasource.password=...
spring.datasource.driver-class-name=org.postgresql.Driver

mdrm.local-file-path=FedFiles/MDRM_0226.csv
mdrm.migration-file-pattern=FedFiles/MDRM_*.csv
mdrm.staging-table=mdrm_staging
mdrm.cron=0 0 1 * * *
```

## Run
```bash
mvn test
mvn spring-boot:run
```

App starts on:
- `http://localhost:8080`

## API Endpoints
Base path: `/api/mdrm`

- `POST /load`
  - Runs fresh migration load (truncate + sequential `MDRM_mmyy` processing) and returns:
  - `{"sourceFileName":"<latest file>","loadedRows":<total>, "runsProcessed":<count>, "processedFiles":[...]}` 
- `POST /upload` (multipart/form-data)
  - Uploads one file (`file`) and validates:
  - file name format: `MDRM_ddyy.csv`
  - headers must match current staging structure (no missing/new columns)
  - no-op files (no incremental changes from previous run) are rejected
- `GET /reporting-forms`
  - Returns distinct `reporting_form` values from `mdrm_master`
- `GET /data?reportingForm=<value>`
  - Returns rows from `mdrm_master` in tabular JSON for selected form from **latest run only**
- `GET /run-history?reportingForm=<value>`
  - Returns run summary rows for a report form:
  - `run_id`, `run_datetime`, `file_name`, `total_unique_mdrms`, `active_mdrms`, `inactive_mdrms`, `updated_mdrms`, `added_mdrms`, `modified_mdrms`, `deleted_mdrms`
- `GET /run-mdrms?reportingForm=<value>&runId=<runId>&bucket=<TOTAL|ACTIVE|INACTIVE|UPDATED>`
  - Returns MDRM code list for drill-down of a selected run/bucket
- `GET /file-runs`
  - Returns file-level run metrics for load-page monitoring
- `GET /incremental-summary?runId=<runId>`
  - Returns report-level incremental counts (`added/modified/deleted`) for one run
- `GET /run-incremental-mdrms?reportingForm=<value>&runId=<runId>&changeType=<ADDED|MODIFIED|DELETED>`
  - Returns MDRM codes for one incremental bucket

## UI
Single-page menu console:
- `http://localhost:8080/`

Main menu:
- **Load MDRM**
- **Reporting Viewer**

Reporting Viewer:
- full-width split layout
- left pane:
  - mode selector with `Reports (N)` and placeholder `MDRMs`
  - search filter + scrollable report list
- right pane:
  - selected report heading
  - tabbed sections: `Run Summary` and `Regular Details`
  - run-summary count hyperlinks for MDRM drill-down

Legacy pages redirect to `/`:
- `/mdrm-ui.html`
- `/mdrm-reporting.html`

## Local MDRM Files Note
Migration reads `src/main/resources/FedFiles/MDRM_mmyy.csv` files in chronological order.
Set `mdrm.migration-file-pattern` if you keep files in a different classpath location.

## Documentation
Generate JavaDocs:

```bash
mvn javadoc:javadoc
```

Open:
- `target/site/apidocs/index.html`

## Notes
- Core constants/messages are centralized in `MdrmConstants`
- Startup schema initialization creates required MDRM tables/indexes
- PostgreSQL promotion function is created/updated at startup for DB-side staging-to-master processing
- Promotion from staging to master is PostgreSQL-only (no Java fallback path)
- MDRM integration tests are PostgreSQL-gated; when test DB is H2, those tests are skipped
- Bootstrap CDN is used for base UI styling in static pages
