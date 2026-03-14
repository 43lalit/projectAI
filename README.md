# ProjectAI - MDRM Loader (Spring Boot)

## Overview
ProjectAI is a Spring Boot service that ingests MDRM CSV data into PostgreSQL with run tracking, error logging, a master table for reporting, and precomputed run summaries for fast UI exploration.

Link to Demo Video: [MDRM Tool Demo Video
](https://drive.google.com/file/d/1xUCP9r_Qzo5YlIT02bHGUTkBL1Nvd5OI/view?usp=share_link)
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

## Latest Updates (2026-03-04)
- Ontology graph experience introduced and refined:
  - Interactive node-based flow now supports `Reports -> MDRM -> Active/Inactive -> Type`.
  - Relationship edges are labeled for readability (e.g., `comprises of`, `can be of`).
  - Nodes with zero count are hidden from view.
  - Type nodes are derived from active/inactive branches and edges render only for valid relationships.
- Ontology filtering and controls:
  - Node visibility controls with multi-select checkboxes.
  - `Reset Filter` and `Clear View` actions moved into the ontology filter widget.
  - Summary header text/boxes removed to maximize graph real estate.
- Ontology reference support:
  - Report and MDRM reference widgets retained as table-style helpers while working from the graph.
  - MDRM and Financial/Reported Item Type nodes include info hints (`i`) with business definitions.
- Run-context consistency:
  - KPI MDRM count aligned to ontology summary context behavior for selected run month.
- Quick Tour updates:
  - Added ontology-focused onboarding steps for opening ontology, interacting with graph nodes, and filtering node visibility.
  - Onboarding version key incremented so returning users receive the updated guide.
- MDRM management from ontology:
  - Added admin-only MDRM management APIs:
    - `POST /api/mdrm/manage/add`
    - `POST /api/mdrm/manage/edit`
    - `POST /api/mdrm/manage/delete` (soft delete -> inactive)
  - Ontology MDRM nodes now expose an admin-only pen icon that opens a management modal.
  - Modal supports add/edit/delete with confirmation before execution.
  - Edit flow constrained to description updates (MDRM code locked).
  - Edit/Delete use searchable MDRM dropdown; if MDRMs are selected in ontology scope, dropdown is limited to that selected set.
  - Mutation operations are limited to latest run context.
- Ontology interaction/UI updates:
  - Removed bottom Node Details section from ontology page.
  - Ontology status/errors now render as top-left in-graph overlay messages.
  - Selected Reports and MDRMs tables are clickable and open a right-side slide-in detail drawer (no forced navigation).
  - Drawer includes "View Full Details" actions to open dedicated report/MDRM pages when needed.
- Cross-screen in-place detail navigation:
  - Discovery grid now opens drawer for MDRM/report clicks.
  - Recent items for report/MDRM open drawer instead of immediate redirect.
  - Reporting page MDRM lists/tables open drawer for MDRM/report drilldown.
- Branding refresh:
  - Added logo set in `src/main/resources/static/brand/`:
    - `logo-networked-ledger.svg` (active)
    - `logo-federal-compass.svg`
    - `logo-shield-nodes.svg`
  - Header logo switched from gradient square to SVG brand mark and resized for better visibility.
- Reporting header action polish:
  - Replaced text-based `Refresh Metadata` button with icon-based refresh control while preserving tooltip/ARIA labeling.

## Latest Updates (2026-03-12)
- Frontend modularization pass completed:
  - Large `index.html` behaviors were split into focused JS modules under `src/main/resources/static/js/`
  - Current modules cover shell, auth/bookmarks, discovery, reporting, load, ontology, and rules
  - Main console remains static HTML/JS, but feature logic is no longer concentrated in one oversized page file
- Generic rules ingestion subsystem added:
  - New Spring Boot services/controllers support workbook ingestion, rule search, rule detail, report/MDRM association, lineage graph, and discrepancy analysis
  - The core data model is generic (`rules`, `rule_dependencies`, `rule_loads`, `rule_import_warnings`) even though the first loaded source is FR Y-9C edits
  - `rule_category` supports future non-regulatory sources such as internal rules

## Latest Updates (2026-03-13)
- MDRM profile redesign and lineage-first workflow:
  - MDRM page restructured into a cleaner 3-pane layout with a left support drawer, right analysis tabs, and a bottom rules/detail pane
  - Rule lineage is the primary investigation surface, with `Rule Based Lineage` and `Related MDRM` as analysis tabs
  - Drawer-launched MDRM analysis now opens in modal workspaces so users stay on the current screen
- Lineage interaction and completeness improvements:
  - Fullscreen/expand behavior stabilized for lineage interactions
  - Node completeness dots added and aligned to associated-rule validity
  - `PARSE_WARNING` now propagates into lineage graph rule-node status
  - Selected lineage-node rules panel now focuses on primary MDRM rules to better match node completeness
  - Resizable detail pane, graph overlays, and modal expand/restore controls refined
- Rules ingestion and discrepancy accuracy:
  - Quarter-suffixed MDRM references like `BHCK2170-Q4` normalize to the base MDRM code
  - Rules data was reprocessed and relinked after normalization changes
  - One-shot backend utility added to reload rule SQL helper functions without a full app restart
- Grid and drawer consistency:
  - Additional rules surfaces converted to AG Grid with compact multiline cells and `Read more / Read less`
  - Console-side MDRM references in rule grids open the shared MDRM drawer again
  - Report rules grid lifecycle fixes applied so rule panels recover correctly when containers are rebuilt
- Console stabilization follow-up:
  - Fixed a stale `reportRulesDiscrepancyOnly` bootstrap reference that was breaking console startup in the browser
  - Disabled unintended AG Grid selection checkboxes in the report rules grid
  - Added a favicon link and refreshed static asset version strings for safer browser cache invalidation
- Session notes for this work were saved in [prompts/2026-03-13-session-notes.md](/Users/lalitgupta/Desktop/2024/JavaMasterclass/spring-boot-rest/ProjectAI/prompts/2026-03-13-session-notes.md)
- User prompts for this debugging session were saved in [prompts/2026-03-13-chat.md](/Users/lalitgupta/Desktop/2024/JavaMasterclass/spring-boot-rest/ProjectAI/prompts/2026-03-13-chat.md)
- Rules workbook ingestion behavior:
  - Reads the configured workbook from `mdrm.rules-file-path`
  - Stores `Edit Test` as business-facing rule text
  - Stores `Alg Edit Test` as canonical rule expression
  - Derives secondary MDRM dependencies from the canonical expression using configurable regex extraction
  - Tracks non-fatal import warnings instead of failing the entire load on malformed rows
- New rules APIs:
  - `POST /api/mdrm/rules/load`
  - `GET /api/mdrm/rules/load-history`
  - `GET /api/mdrm/rules/search`
  - `GET /api/mdrm/rules/by-report`
  - `GET /api/mdrm/rules/by-mdrm`
  - `GET /api/mdrm/rules/by-scope`
  - `GET /api/mdrm/rules/{ruleId}`
  - `GET /api/mdrm/rules/graph`
  - `GET /api/mdrm/rules/discrepancies`
- Reporting and discovery integration:
  - Discovery search now includes rules as a first-class result type
  - Reports view includes rules alongside existing reporting analysis surfaces
  - Load Data includes rules workbook load/history support
- Ontology redesign:
  - Ontology now emphasizes active reports, active MDRMs, and scoped rules counts
  - Rules are integrated into the summarized ontology flow instead of a separate rules-only graph mode
  - Persistent ontology inspector tabs added for `Selection`, `Reports`, `MDRMs`, and `Rules`
  - Ontology graph and inspector panes are vertically resizable
- MDRM profile rules lineage:
  - MDRM profile now includes a rules tab with dependency-based lineage exploration
  - Graph is MDRM-only and shows immediate parent dependencies first
  - Users can expand/collapse deeper dependencies, refocus the lineage root, move back through traversal history, zoom in/out, reset zoom, and open the graph pane in fullscreen
  - Associated rules for the currently selected dependency node render below the graph

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
- `rule_loads`
  - `load_id` (PK)
  - `source_system`
  - `report_series`
  - `source_file_name`
  - `file_checksum`
  - `loaded_at`
  - `load_status`
  - workbook row/dependency/warning/error counts
- `rules`
  - `rule_id` (PK)
  - `load_id`
  - `rule_category`
  - `report_series`
  - `reporting_form`
  - `schedule_name`
  - `rule_type`
  - `rule_number`
  - `primary_mdrm_code`
  - `effective_start_date`
  - `effective_end_date`
  - `change_status`
  - `rule_text`
  - `rule_expression`
  - `source_sheet`
  - `source_row_number`
- `rule_dependencies`
  - `dependency_id` (PK)
  - `rule_id`
  - `primary_mdrm_code`
  - `secondary_token_raw`
  - `secondary_mdrm_code`
  - `dependency_kind`
  - `qualifier_detail`
  - `parse_confidence`
  - `is_self_reference`
- `rule_import_warnings`
  - `warning_id` (PK)
  - `load_id`
  - `rule_id`
  - `source_sheet`
  - `source_row_number`
  - `warning_type`
  - `warning_message`

## Derived Rules
- Date parsing format: `M/d/yyyy h:mm:ss a`
- `is_active = 'Y'` when:
  - end-date year is `9999`, or
  - end date is greater than current UTC time
- otherwise `is_active = 'N'`
- Rule dependency derivation:
  - Primary MDRM comes from the rule target row
  - Secondary MDRMs are derived from workbook dependency columns when present, otherwise from `Alg Edit Test`
  - MDRM tokens are normalized by trimming and uppercasing
  - Self-references are skipped
  - Duplicate dependency edges within a rule are removed
  - Dependency discrepancies such as inactive or missing secondary MDRMs are computed against selected run context rather than stored as permanent source truth

## Configuration
- `mdrm.local-file-path`
  - current single-file MDRM load source
- `mdrm.migration-file-pattern`
  - MDRM historical migration pattern
- `mdrm.rules-file-path`
  - configured rules workbook path
- `mdrm.rules-source-system`
  - source system label stored in `rule_loads`
- `mdrm.rules-default-category`
  - default category assigned during rules ingestion
- `mdrm.rules-expression-regex`
  - regex used to derive dependency MDRM tokens from canonical rule expression text
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
