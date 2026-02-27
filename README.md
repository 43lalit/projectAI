# ProjectAI - MDRM Loader (Spring Boot)

## Overview
ProjectAI is a Spring Boot service that ingests MDRM CSV data into PostgreSQL with run tracking, error logging, a master table for reporting, and precomputed run summaries for fast UI exploration.

Current ingestion flow (`POST /api/mdrm/load`):
1. Create `run_id` (UTC epoch milliseconds) and insert a row in `mdrm_run_master`
2. Read MDRM source from local resources (`mdrm.local-file-path`)
3. Ignore CSV line 1 (`PUBLIC` metadata)
4. Use CSV line 2 as headers
5. Recreate `mdrm_staging` from sanitized CSV headers (+ `run_id`)
6. Batch load source rows to staging
7. Promote staging rows to `mdrm_master` (PostgreSQL stored function path)
8. Write invalid rows to `mdrm_run_error`
9. Update run counts in `mdrm_run_master`

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

mdrm.local-file-path=FedFiles/MDRM_CSV.csv
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
  - Runs full ingestion and returns:
  - `{"sourceFileName":"...","loadedRows":<count>}`
- `GET /reporting-forms`
  - Returns distinct `reporting_form` values from `mdrm_master`
- `GET /data?reportingForm=<value>`
  - Returns rows from `mdrm_master` in tabular JSON for selected form from **latest run only**
- `GET /run-history?reportingForm=<value>`
  - Returns run summary rows for a report form:
  - `run_id`, `run_datetime`, `file_name`, `total_unique_mdrms`, `active_mdrms`, `inactive_mdrms`, `updated_mdrms`
- `GET /run-mdrms?reportingForm=<value>&runId=<runId>&bucket=<TOTAL|ACTIVE|INACTIVE|UPDATED>`
  - Returns MDRM code list for drill-down of a selected run/bucket

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

## Local MDRM CSV Note
Large MDRM CSV is intentionally not tracked in Git.

Ignored path:
- `src/main/resources/FedFiles/MDRM_CSV.csv`

Place your local CSV there (or change `mdrm.local-file-path`).

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
