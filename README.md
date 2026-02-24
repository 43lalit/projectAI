# ProjectAI - MDRM Loader (Spring Boot)

## Overview
ProjectAI is a Spring Boot application that ingests MDRM data into a PostgreSQL staging table.

Current flow:
1. Read MDRM source file from local resources path (`mdrm.local-file-path`)
2. Ignore CSV line 1 (metadata)
3. Treat CSV line 2 as headers
4. Recreate staging table from sanitized header names on every load
5. Batch insert all data rows

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

## Configuration
Set in `application.properties`:

```properties
spring.datasource.url=jdbc:postgresql://localhost:5432/projectAI
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
  - Triggers full MDRM load (drop + recreate staging table + insert)
- `GET /reporting-forms`
  - Returns distinct `reporting_form` values from staging table
- `GET /data?reportingForm=<value>`
  - Returns rows for selected reporting form in tabular JSON

## UI
Single page console (menu-based):
- `http://localhost:8080/`

Menu options:
- **Load MDRM**: trigger ingestion
- **Reporting Viewer**: select reporting form and render rows in table format

Legacy pages now redirect to `/`:
- `/mdrm-ui.html`
- `/mdrm-reporting.html`

## Local MDRM CSV Note
Large MDRM CSV is intentionally not tracked in Git.

Ignored path:
- `src/main/resources/FedFiles/MDRM_CSV.csv`

Place your local MDRM CSV at that path (or update `mdrm.local-file-path`).

## Documentation
Generate JavaDocs:

```bash
mvn javadoc:javadoc
```

Open:
- `target/site/apidocs/index.html`

## Maintainer Notes
- Error messages and key literals are centralized in `MdrmConstants`
- MDRM classes include JavaDoc comments for maintainability
