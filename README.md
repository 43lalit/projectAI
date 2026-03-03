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

## Latest Updates (2026-03-01)
- Guided onboarding (`Guide Me`) added in header:
  - First-visit auto-start tour with skip/back/next controls
  - Manual relaunch via `Guide Me` button
  - Smooth shade fade-in/fade-out transitions
  - Spotlight + arrowed helper card with per-step context
  - Includes intro step describing product purpose before navigation steps
  - Now covers Discovery, Search input, Bookmarks, Reporting, Load MDRM, Theme toggle, and Profile menu
- Guide assistant visuals:
  - Professional full-body female guide avatar support
  - Step-based static expression transitions
  - Custom user-provided avatar asset support from `/static/guide`
- Recent Items behavior refined:
  - Recent Items section is hidden when empty
  - Clearing recent items now immediately hides the section
  - Section shows only when items exist (and discovery view is active)
- Item type display normalization:
  - `item_type` code values are translated to business-friendly labels in MDRM query/search output.
  - Discovery grid includes `MDRM Type` and supports fallback mapping in UI.
- Discovery UX refinements:
  - Favorites-first result ordering on first render.
  - `Favorite` column naming and sorting support.
  - Cleaner pre-search behavior with results hidden until first search.
- Dark mode readability fixes:
  - Improved table/grid text and stripe contrast, especially for Load MDRM run-metrics tables.
- Navigation/UI consistency:
  - Icon-based menu style propagated across pages.
  - Removed dedicated MDRM top-nav entry.
- Recent Items feature:
  - Phase 1: local recent history cards (up to 10), quick reopen.
  - Phase 2: server-backed recent history per logged-in user (`/api/recent/items`), with local fallback.
  - Recent cards include type icons and bookmark star toggle.
- MDRM Profile experience:
  - New dedicated profile API: `GET /api/mdrm/profile?mdrm=<code>`
  - Tabbed MDRM view with:
    - overview snapshot
    - timeline across runs/files
    - report association mapping
    - related MDRM graph
  - Timeline upgrades:
    - visual timeline cards
    - per-run change detection
    - first-vs-latest field delta section
    - expandable field-level previous/current values
    - change/no-change color cues
  - Relationship visualization:
    - D3-based concentric graph:
      - center: selected MDRM
      - ring 1: related reports
      - ring 2: related MDRMs under each report
    - right-side details panel updates on node click
    - report node click lists related MDRMs in-panel
    - default graph filter shows active-only with `Include inactive` toggle
- Navigation/auth consistency:
  - MDRM page now uses the same profile auth controls (login/sign-up/reset/logout) as main console.
  - Discovery -> MDRM profile links preserve journey context and support return to prior search state.
- Status readability:
  - Global active/inactive capsule style:
    - `Active` green capsule
    - `Inactive` red capsule
  - Applied across MDRM profile and reporting table status fields.

## Latest Updates (2026-03-02)
- Implemented full run-context "time travel" behavior across the application:
  - Header run selector now drives as-of context globally.
  - Backend endpoints support optional `runId` context for discovery/reporting/profile surfaces.
  - As-of semantics enforce no future data beyond selected run (`run_id <= selectedRunId` where applicable).
- Run context UX improvements:
  - Dropdown labels now display `As of <Month>, <Year>`, inferred from file naming (`MDRM_MMDD.csv`) with run-time year.
  - Latest run is shown as a normal option with `(Latest)` suffix (no separate "latest" pseudo-option).
- Reporting metadata enhancements:
  - Metadata refresh and extraction improved to pull form title/full name and description from Federal Reserve reporting-form pages.
  - Description handling supports clean wrapping and read-more/read-less expansion for long text.
- Reporting viewer redesign finalized:
  - KPI-driven interaction model (`Runs`, `Run Compare`, `Active MDRMs`, `Net Change`).
  - AG Grid used for active/net-change drilldowns with filter + pagination.
  - Empty state centered with icon/message when no report is selected.
  - Left nav and report header/status UX tightened for clarity.
- Report status model updates:
  - Active/Inactive report status now represented and persisted via summary/status tables.
  - Status capsules used consistently.
- Role-based experience updates:
  - Admin-only access for `Load Data` (previously `Load MDRM`).
  - Guest/non-admin guide steps now hide load-related onboarding steps.
- Recent activity robustness:
  - Added reliable recent tracking on MDRM profile page load (not only click interception from discovery).
  - Recent tracking is run-context-aware.
  - Fixed race condition in `recent_item` upsert using atomic PostgreSQL `ON CONFLICT DO UPDATE`.
- Header/branding/navigation polish:
  - FedMDRM brand (icon + text) is clickable and routes home.
  - Header layout fixed to avoid unintended multi-row wrapping.
  - Menu text updated:
    - `Reporting Viewer` -> `Reports`
    - `Load MDRM` -> `Load Data`
  - Guide CTA updated to `📖 Quick Tour`.
  - KPI labels now include icons for scanability.
- Command palette beta added to main console:
  - `Cmd/Ctrl + K` opens quick actions with keyboard navigation (arrows + Enter + Escape).
  - Intent actions support:
    - open report by typed report code/name
    - run discovery search from free text
    - open MDRM profile by typed MDRM code
  - Natural-language stop words are ignored for better matching (e.g. "go to FFIEC041").
  - Guide tour now includes command-palette step.
- Search productivity enhancements:
  - `Recent Searches` in Discovery with click-to-rerun and clear action.
  - `Saved Searches` in Discovery with save/apply/remove behavior.
  - Search state includes query + filters + run context for reproducible workflows.
  - Saved/recent searches are surfaced in command palette actions.
- Deep-link sharing improvements:
  - Discovery view has share link copy (query/filter/run-context aware).
  - Reports view has share link copy for selected report (run-context aware).
  - MDRM profile page has share link copy (mdrm/run-context/return-journey aware).
  - Copy interactions show quick floating "Link copied" confirmation near the clicked control.

## Latest Updates (2026-03-03)
- Header/action controls visual cleanup:
  - Removed border/background treatment from `Command`, `Quick Tour`, and theme toggle controls.
  - Removed remaining dark-theme shaded background for `Quick Tour`.
- Global shell/layout refinements:
  - Full-width application layout behavior consolidated.
  - Square-corner styling applied consistently across screens.
- Bookmarks UX refresh (bookmarks view only):
  - Reduced visual clutter by trimming extra sectional borders.
  - Improved card density so more bookmark cards fit side-by-side.
- Bookmarking coverage expanded:
  - Bookmark toggle on MDRM profile page.
  - Bookmark toggle for reports in reporting view.
  - Bookmark toggle for MDRMs directly from reporting AG Grid tables.
- Recent items panel refinement:
  - Adjusted card sizing/layout to avoid horizontal scrollbar for the 10-item view.
- Functional specification artifacts generated from repository and stored in `docs/`:
  - `docs/ProjectAI_Functional_Specification.docx`
  - `docs/ProjectAI_Functional_Specification.html`
  - `docs/ProjectAI_Functional_Specification.txt`

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
- Session prompt logs: `prompts`

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
- `GET /semantic-search?q=<text>`
  - Discovery search endpoint used by landing-page AG Grid results
- `GET /profile?mdrm=<MDRM_CODE>`
  - Returns MDRM-centric profile payload (snapshot, timeline, associations, related graph data)

## UI
Main console:
- `http://localhost:8080/`
- Primary views:
  - **Discovery** (AG Grid search, quick filter, favorites)
  - **Bookmarks** (groups + saved MDRMs)
  - **Reports** (run summary + details + drill-down)
  - **Load Data** (upload/load + file/run metrics)

MDRM profile page:
- `http://localhost:8080/mdrm.html?mdrm=<code>`
- Includes:
  - overview + latest snapshot
  - timeline with change details
  - report association table
  - D3 relationship graph with detail panel

Legacy pages:
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
