package com.projectai.projectai.mdrm;

import jakarta.annotation.PostConstruct;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.stream.Collectors;

/**
 * Orchestrates MDRM file ingestion: reads source file, rebuilds staging table from CSV headers,
 * persists run metadata, transforms records to master table, and logs row-level ingestion errors.
 */
@Service
public class MdrmLoadService {

    private static final Pattern UPLOAD_FILE_PATTERN = Pattern.compile("^MDRM_([0-2][0-9]|3[01])(\\d{2})\\.csv$");
    private static final Set<String> SEARCH_STOP_WORDS = Set.of(
            "i", "want", "to", "search", "all", "for", "the", "and", "or", "of", "in", "on",
            "related", "report", "reports", "mdrm", "mdrms", "show", "me"
    );

    private final MdrmDownloader mdrmDownloader;
    private final MdrmProperties mdrmProperties;
    private final JdbcTemplate jdbcTemplate;

    public MdrmLoadService(MdrmDownloader mdrmDownloader, MdrmProperties mdrmProperties, JdbcTemplate jdbcTemplate) {
        this.mdrmDownloader = mdrmDownloader;
        this.mdrmProperties = mdrmProperties;
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Creates MDRM operational tables at application startup so schema is visible before first load.
     */
    @PostConstruct
    public void initializeSchema() {
        ensureRunMasterTable();
        ensureRunErrorTable();
        ensureBaseStagingTable();
        ensureBaseMasterTable();
        ensureRunSummaryTable();
        ensureRunIncrementalTable();
        ensureFileSummaryTable();
        ensureMasterIndexes();
        ensureRunSummaryIndexes();
        ensureRunIncrementalIndexes();
        ensureFileSummaryIndexes();
        ensurePostgresPromotionFunction();
        backfillMissingRunSummaries();
        backfillMissingRunIncrementals();
        backfillMissingFileSummaries();
    }

    /**
     * Executes a full MDRM load cycle and returns source file name with successfully ingested row count.
     */
    public MdrmLoadResult loadLatestMdrm() {
        return runSingleLoad(mdrmDownloader.download());
    }

    /**
     * Validates and loads one user-uploaded MDRM file.
     */
    public MdrmLoadResult loadUploadedMdrm(String fileName, byte[] content) {
        validateUploadFileName(fileName);
        validateUploadedHeaders(content);
        return runSingleLoad(new DownloadedMdrm(fileName, content));
    }

    /**
     * Cleans MDRM tables and then processes all classpath files matching the MDRM_mmyy convention, oldest first.
     */
    public MdrmLoadResult loadFreshMigration() {
        ensureRunMasterTable();
        ensureRunErrorTable();
        ensureRunSummaryTable();
        ensureRunIncrementalTable();
        ensureFileSummaryTable();
        ensureBaseMasterTable();
        ensureBaseStagingTable();
        truncateAllMdrmTables();

        List<DownloadedMdrm> filesToProcess = mdrmDownloader.downloadAllForMigration();
        int totalLoadedRows = 0;
        List<String> processedFiles = new ArrayList<>(filesToProcess.size());
        String lastFileName = null;

        for (DownloadedMdrm downloadedMdrm : filesToProcess) {
            MdrmLoadResult singleRun = runSingleLoad(downloadedMdrm);
            totalLoadedRows += singleRun.loadedRows();
            lastFileName = singleRun.sourceFileName();
            processedFiles.add(singleRun.sourceFileName());
        }

        return new MdrmLoadResult(lastFileName, totalLoadedRows, processedFiles.size(), processedFiles);
    }

    private MdrmLoadResult runSingleLoad(DownloadedMdrm downloadedMdrm) {
        ZipContent content = extractContent(downloadedMdrm);

        ensureRunMasterTable();
        ensureRunErrorTable();
        ensureRunSummaryTable();
        ensureRunIncrementalTable();
        ensureFileSummaryTable();
        long runId = generateRunId();
        String stagingTableName = sanitizeTableName(mdrmProperties.getStagingTable());
        createRunMasterRow(runId, content.fileName());
        int stagedRowCount = 0;

        try {
            stagedRowCount = parseCsvAndLoadToStaging(content.content(), stagingTableName, runId);
            ensureMasterTable(stagingTableName);
            IngestionStats ingestionStats = transformFromStagingToMaster(stagingTableName, runId);

            updateRunMasterCounts(runId, stagedRowCount, ingestionStats.ingestedCount(), ingestionStats.errorCount());
            refreshRunSummary(runId);
            refreshRunIncremental(runId);
            refreshFileSummary(runId);
            if (hasPreviousRun(runId) && !hasAnyIncrementalChange(runId)) {
                deleteRunData(runId, stagingTableName);
                throw new IllegalStateException("Uploaded file has no incremental changes from latest available data");
            }
            return new MdrmLoadResult(content.fileName(), ingestionStats.ingestedCount());
        } catch (RuntimeException ex) {
            int errorRows = countRunErrors(runId);
            updateRunMasterCounts(runId, stagedRowCount, 0, errorRows);
            refreshFileSummary(runId);
            throw ex;
        }
    }

    /**
     * Promotes staged rows into master/error using PostgreSQL stored function.
     */
    private IngestionStats transformFromStagingToMaster(String stagingTableName, long runId) {
        List<String> stagingColumns = getTableColumns(stagingTableName);
        ensureRequiredColumnExists(stagingColumns, MdrmConstants.COLUMN_START_DATE, stagingTableName);
        ensureRequiredColumnExists(stagingColumns, MdrmConstants.COLUMN_END_DATE, stagingTableName);
        ensureRequiredColumnExists(stagingColumns, MdrmConstants.COLUMN_MNEMONIC, stagingTableName);
        ensureRequiredColumnExists(stagingColumns, MdrmConstants.COLUMN_ITEM_CODE, stagingTableName);
        ensureRequiredColumnExists(stagingColumns, MdrmConstants.COLUMN_REPORTING_FORM, stagingTableName);

        if (!isPostgreSql()) {
            throw new IllegalStateException("MDRM promotion requires PostgreSQL");
        }
        return promoteFromStagingUsingProcedure(stagingTableName, runId);
    }

    /**
     * Returns all distinct reporting form values available in the master table.
     */
    public List<String> getReportingForms() {
        ensureReportingFormColumnExists(MdrmConstants.DEFAULT_MASTER_TABLE);
        return jdbcTemplate.queryForList(
                MdrmConstants.SQL_SELECT_DISTINCT_REPORTING_FORM_PREFIX + MdrmConstants.DEFAULT_MASTER_TABLE
                        + MdrmConstants.SQL_SELECT_DISTINCT_REPORTING_FORM_SUFFIX,
                String.class
        );
    }

    /**
     * Returns master rows for a given reporting form from only that form's latest run.
     */
    public MdrmTableResponse getRowsByReportingForm(String reportingForm) {
        ensureReportingFormColumnExists(MdrmConstants.DEFAULT_MASTER_TABLE);
        String selectList = buildMasterSelectList("m");
        String sql = "SELECT " + selectList + " FROM " + MdrmConstants.DEFAULT_MASTER_TABLE + " m"
                + " WHERE m.reporting_form = ? AND m.run_id = (SELECT MAX(run_id) FROM "
                + MdrmConstants.DEFAULT_MASTER_TABLE + " WHERE reporting_form = ?)";

        return jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ps.setString(1, reportingForm);
                    ps.setString(2, reportingForm);
                    return ps;
                },
                this::extractTableResponse
        );
    }

    /**
     * Returns run history for a reporting form with unique MDRM counts by status category.
     */
    public MdrmRunHistoryResponse getRunHistoryByReportingForm(String reportingForm) {
        ensureReportingFormColumnExists(MdrmConstants.DEFAULT_MASTER_TABLE);
        ensureRunSummaryTable();
        ensureRunIncrementalTable();
        String sql = """
                SELECT
                    s.run_id,
                    COALESCE(rm.run_datetime, s.run_id) AS run_datetime,
                    COALESCE(s.file_name, rm.file_name, '') AS file_name,
                    s.total_unique_mdrms,
                    s.active_mdrms,
                    s.inactive_mdrms,
                    s.updated_mdrms,
                    COALESCE(i.added_mdrms, 0) AS added_mdrms,
                    COALESCE(i.modified_mdrms, 0) AS modified_mdrms,
                    COALESCE(i.deleted_mdrms, 0) AS deleted_mdrms
                FROM __RUN_SUMMARY_TABLE__ s
                LEFT JOIN __RUN_MASTER_TABLE__ rm ON rm.run_id = s.run_id
                LEFT JOIN __RUN_INCREMENTAL_TABLE__ i
                  ON i.run_id = s.run_id AND i.reporting_form = s.reporting_form
                WHERE s.reporting_form = ?
                ORDER BY s.run_id DESC
                """
                .replace("__RUN_SUMMARY_TABLE__", MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE)
                .replace("__RUN_MASTER_TABLE__", MdrmConstants.DEFAULT_RUN_MASTER_TABLE)
                .replace("__RUN_INCREMENTAL_TABLE__", MdrmConstants.DEFAULT_RUN_INCREMENTAL_TABLE);

        List<MdrmRunSummary> runs = jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ps.setString(1, reportingForm);
                    return ps;
                },
                (rs, rowNum) -> new MdrmRunSummary(
                        rs.getLong("run_id"),
                        rs.getLong("run_datetime"),
                        rs.getString("file_name"),
                        rs.getInt("total_unique_mdrms"),
                        rs.getInt("active_mdrms"),
                        rs.getInt("inactive_mdrms"),
                        rs.getInt("updated_mdrms"),
                        rs.getInt("added_mdrms"),
                        rs.getInt("modified_mdrms"),
                        rs.getInt("deleted_mdrms")
                )
        );

        return new MdrmRunHistoryResponse(reportingForm, runs);
    }

    /**
     * Returns MDRM codes for one run/category bucket for drill-down in the reporting UI.
     */
    public MdrmCodeListResponse getMdrmCodesForRunBucket(String reportingForm, long runId, String bucket) {
        ensureReportingFormColumnExists(MdrmConstants.DEFAULT_MASTER_TABLE);
        String normalizedBucket = normalizeBucket(bucket);
        String sql = """
                WITH code_flags AS (
                    SELECT
                        m.mdrm_code,
                        MAX(CASE WHEN m.is_active = 'Y' THEN 1 ELSE 0 END) AS has_active,
                        MAX(CASE WHEN m.is_active = 'N' THEN 1 ELSE 0 END) AS has_inactive
                    FROM __MASTER_TABLE__ m
                    WHERE m.reporting_form = ?
                      AND m.run_id = ?
                      AND m.mdrm_code IS NOT NULL
                      AND btrim(m.mdrm_code) <> ''
                    GROUP BY m.mdrm_code
                )
                SELECT c.mdrm_code
                FROM code_flags c
                WHERE __BUCKET_CONDITION__
                ORDER BY c.mdrm_code
                """
                .replace("__MASTER_TABLE__", MdrmConstants.DEFAULT_MASTER_TABLE)
                .replace("__BUCKET_CONDITION__", bucketConditionSql(normalizedBucket));

        List<String> mdrmCodes = jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ps.setString(1, reportingForm);
                    ps.setLong(2, runId);
                    return ps;
                },
                (rs, rowNum) -> rs.getString("mdrm_code")
        );

        return new MdrmCodeListResponse(reportingForm, runId, normalizedBucket, mdrmCodes);
    }

    /**
     * Returns file-level run metrics for load-screen monitoring.
     */
    public List<MdrmFileRunSummary> getFileRunHistory() {
        ensureFileSummaryTable();
        String sql = """
                SELECT
                    fs.run_id,
                    fs.run_datetime,
                    COALESCE(fs.file_name, '') AS file_name,
                    fs.num_file_records,
                    fs.num_records_ingested,
                    fs.num_records_error,
                    fs.reports_count,
                    fs.total_unique_mdrms,
                    fs.active_mdrms,
                    fs.inactive_mdrms,
                    fs.updated_mdrms
                FROM __FILE_SUMMARY_TABLE__ fs
                ORDER BY fs.run_id DESC
                """
                .replace("__FILE_SUMMARY_TABLE__", MdrmConstants.DEFAULT_FILE_SUMMARY_TABLE);

        return jdbcTemplate.query(
                sql,
                (rs, rowNum) -> new MdrmFileRunSummary(
                        rs.getLong("run_id"),
                        rs.getLong("run_datetime"),
                        rs.getString("file_name"),
                        rs.getInt("num_file_records"),
                        rs.getInt("num_records_ingested"),
                        rs.getInt("num_records_error"),
                        rs.getInt("reports_count"),
                        rs.getInt("total_unique_mdrms"),
                        rs.getInt("active_mdrms"),
                        rs.getInt("inactive_mdrms"),
                        rs.getInt("updated_mdrms")
                )
        );
    }

    /**
     * Returns report-level incremental counts for one run.
     */
    public List<MdrmIncrementalSummary> getIncrementalSummaryForRun(long runId) {
        ensureRunIncrementalTable();
        String sql = "SELECT reporting_form, run_id, added_mdrms, modified_mdrms, deleted_mdrms FROM "
                + MdrmConstants.DEFAULT_RUN_INCREMENTAL_TABLE
                + " WHERE run_id = ? AND (added_mdrms + modified_mdrms + deleted_mdrms) > 0"
                + " ORDER BY reporting_form";

        return jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ps.setLong(1, runId);
                    return ps;
                },
                (rs, rowNum) -> new MdrmIncrementalSummary(
                        rs.getString("reporting_form"),
                        rs.getLong("run_id"),
                        rs.getInt("added_mdrms"),
                        rs.getInt("modified_mdrms"),
                        rs.getInt("deleted_mdrms")
                )
        );
    }

    /**
     * Returns MDRM codes for one incremental change bucket.
     */
    public MdrmIncrementalCodeListResponse getIncrementalCodesForRun(
            String reportingForm, long runId, String changeType) {
        String normalized = normalizeIncrementalChangeType(changeType);
        long previousRunId = findPreviousRunForReportingForm(reportingForm, runId);
        if (previousRunId <= 0) {
            return new MdrmIncrementalCodeListResponse(reportingForm, runId, normalized, List.of());
        }

        String sql = """
                WITH current_code AS (
                    SELECT
                        m.mdrm_code,
                        COUNT(*) AS row_count,
                        MAX(CASE WHEN m.is_active = 'Y' THEN 1 ELSE 0 END) AS has_active,
                        MAX(CASE WHEN m.is_active = 'N' THEN 1 ELSE 0 END) AS has_inactive,
                        MIN(m.start_date_utc) AS min_start,
                        MAX(m.end_date_utc) AS max_end
                    FROM __MASTER_TABLE__ m
                    WHERE m.run_id = ?
                      AND m.reporting_form = ?
                      AND m.mdrm_code IS NOT NULL
                      AND btrim(m.mdrm_code) <> ''
                    GROUP BY m.mdrm_code
                ),
                previous_code AS (
                    SELECT
                        m.mdrm_code,
                        COUNT(*) AS row_count,
                        MAX(CASE WHEN m.is_active = 'Y' THEN 1 ELSE 0 END) AS has_active,
                        MAX(CASE WHEN m.is_active = 'N' THEN 1 ELSE 0 END) AS has_inactive,
                        MIN(m.start_date_utc) AS min_start,
                        MAX(m.end_date_utc) AS max_end
                    FROM __MASTER_TABLE__ m
                    WHERE m.run_id = ?
                      AND m.reporting_form = ?
                      AND m.mdrm_code IS NOT NULL
                      AND btrim(m.mdrm_code) <> ''
                    GROUP BY m.mdrm_code
                )
                SELECT COALESCE(c.mdrm_code, p.mdrm_code) AS mdrm_code
                FROM current_code c
                FULL OUTER JOIN previous_code p ON p.mdrm_code = c.mdrm_code
                WHERE __CHANGE_CONDITION__
                ORDER BY COALESCE(c.mdrm_code, p.mdrm_code)
                """
                .replace("__MASTER_TABLE__", MdrmConstants.DEFAULT_MASTER_TABLE)
                .replace("__CHANGE_CONDITION__", incrementalConditionSql(normalized));

        List<String> codes = jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ps.setLong(1, runId);
                    ps.setString(2, reportingForm);
                    ps.setLong(3, previousRunId);
                    ps.setString(4, reportingForm);
                    return ps;
                },
                (rs, rowNum) -> rs.getString("mdrm_code")
        );

        return new MdrmIncrementalCodeListResponse(reportingForm, runId, normalized, codes);
    }

    /**
     * Interprets a natural language query and searches MDRMs (latest run, optionally narrowed by inferred report).
     */
    public MdrmSemanticSearchResponse semanticSearch(String query) {
        String rawQuery = query == null ? "" : query.trim();
        if (rawQuery.isEmpty()) {
            return new MdrmSemanticSearchResponse(rawQuery, null, List.of(), 0, new MdrmTableResponse(List.of(), List.of()));
        }

        String interpretedReportingForm = inferReportingForm(rawQuery);
        List<String> keywords = extractSearchKeywords(rawQuery, interpretedReportingForm);
        List<String> columns = getSemanticSearchColumns();

        if (columns.isEmpty()) {
            throw new IllegalStateException("No searchable text columns found in mdrm_master");
        }

        String textExpr = columns.stream()
                .map(column -> semanticSearchColumnExpression(column, "m"))
                .collect(Collectors.joining(" || ' ' || "));

        String selectList = buildMasterSelectList("m");
        StringBuilder sql = new StringBuilder("SELECT ").append(selectList)
                .append(" FROM ").append(MdrmConstants.DEFAULT_MASTER_TABLE).append(" m WHERE ");
        List<Object> params = new ArrayList<>();

        if (interpretedReportingForm != null && !interpretedReportingForm.isBlank()) {
            sql.append("m.reporting_form = ? AND m.run_id = (SELECT MAX(run_id) FROM ")
                    .append(MdrmConstants.DEFAULT_MASTER_TABLE)
                    .append(" WHERE reporting_form = ?) AND ");
            params.add(interpretedReportingForm);
            params.add(interpretedReportingForm);
        } else {
            sql.append("m.run_id = (SELECT MAX(run_id) FROM ").append(MdrmConstants.DEFAULT_MASTER_TABLE).append(") AND ");
        }

        if (!keywords.isEmpty()) {
            for (String keyword : keywords) {
                sql.append("(").append(textExpr).append(") ILIKE ? AND ");
                params.add("%" + keyword + "%");
            }
        } else {
            sql.append("(1 = 1) AND ");
        }

        sql.append("m.mdrm_code IS NOT NULL AND btrim(m.mdrm_code) <> '' ORDER BY m.reporting_form, m.mdrm_code LIMIT 500");

        MdrmTableResponse table = jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql.toString());
                    for (int i = 0; i < params.size(); i++) {
                        ps.setObject(i + 1, params.get(i));
                    }
                    return ps;
                },
                this::extractTableResponse
        );

        int total = table == null || table.rows() == null ? 0 : table.rows().size();
        return new MdrmSemanticSearchResponse(
                rawQuery,
                interpretedReportingForm,
                keywords,
                total,
                table == null ? new MdrmTableResponse(List.of(), List.of()) : table
        );
    }

    /**
     * Creates one run metadata row before ingestion starts.
     */
    private void createRunMasterRow(long runId, String fileName) {
        String sql = "INSERT INTO " + MdrmConstants.DEFAULT_RUN_MASTER_TABLE
                + " (run_id, run_datetime, file_name, num_file_records, num_records_ingested, num_records_error) "
                + "VALUES (?, ?, ?, 0, 0, 0)";
        jdbcTemplate.update(sql, runId, runId, fileName);
    }

    /**
     * Updates run totals after transformation is complete.
     */
    private void updateRunMasterCounts(long runId, int fileRecords, int ingestedRecords, int errorRecords) {
        String sql = "UPDATE " + MdrmConstants.DEFAULT_RUN_MASTER_TABLE
                + " SET num_file_records = ?, num_records_ingested = ?, num_records_error = ? WHERE run_id = ?";
        jdbcTemplate.update(sql, fileRecords, ingestedRecords, errorRecords, runId);
    }

    /**
     * Ensures run metadata table exists.
     */
    private void ensureRunMasterTable() {
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS " + MdrmConstants.DEFAULT_RUN_MASTER_TABLE + " ("
                + "run_id BIGINT PRIMARY KEY, "
                + "run_datetime BIGINT NOT NULL, "
                + "file_name TEXT, "
                + "num_file_records INT NOT NULL, "
                + "num_records_ingested INT NOT NULL, "
                + "num_records_error INT NOT NULL"
                + ")");
    }

    /**
     * Ensures row-level error table exists.
     */
    private void ensureRunErrorTable() {
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS " + MdrmConstants.DEFAULT_RUN_ERROR_TABLE + " ("
                + "error_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, "
                + "run_id BIGINT NOT NULL, "
                + "raw_record TEXT, "
                + "error_description TEXT"
                + ")");
    }

    /**
     * Stores precomputed run/reporting-form summary counts for fast UI reads.
     */
    private void ensureRunSummaryTable() {
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS " + MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE + " ("
                + "run_id BIGINT NOT NULL, "
                + "reporting_form TEXT NOT NULL, "
                + "file_name TEXT, "
                + "total_unique_mdrms INT NOT NULL, "
                + "active_mdrms INT NOT NULL, "
                + "inactive_mdrms INT NOT NULL, "
                + "updated_mdrms INT NOT NULL, "
                + "PRIMARY KEY (run_id, reporting_form)"
                + ")");
        ensureColumnExists(MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE, "file_name", "TEXT");
        syncRunSummaryFileNames();
    }

    private void ensureRunIncrementalTable() {
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS " + MdrmConstants.DEFAULT_RUN_INCREMENTAL_TABLE + " ("
                + "run_id BIGINT NOT NULL, "
                + "reporting_form TEXT NOT NULL, "
                + "added_mdrms INT NOT NULL, "
                + "modified_mdrms INT NOT NULL, "
                + "deleted_mdrms INT NOT NULL, "
                + "PRIMARY KEY (run_id, reporting_form)"
                + ")");
    }

    private void ensureFileSummaryTable() {
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS " + MdrmConstants.DEFAULT_FILE_SUMMARY_TABLE + " ("
                + "run_id BIGINT PRIMARY KEY, "
                + "run_datetime BIGINT NOT NULL, "
                + "file_name TEXT, "
                + "num_file_records INT NOT NULL, "
                + "num_records_ingested INT NOT NULL, "
                + "num_records_error INT NOT NULL, "
                + "reports_count INT NOT NULL, "
                + "total_unique_mdrms INT NOT NULL, "
                + "active_mdrms INT NOT NULL, "
                + "inactive_mdrms INT NOT NULL, "
                + "updated_mdrms INT NOT NULL"
                + ")");
    }

    /**
     * Ensures master table exists and contains current staging columns plus derived columns.
     */
    private void ensureMasterTable(String stagingTableName) {
        ensureBaseMasterTable();

        List<String> masterColumns = getTableColumns(MdrmConstants.DEFAULT_MASTER_TABLE);
        List<String> stagingColumns = getTableColumns(stagingTableName);
        for (String stagingColumn : stagingColumns) {
            if (!MdrmConstants.COLUMN_RUN_ID.equals(stagingColumn) && !masterColumns.contains(stagingColumn)) {
                jdbcTemplate.execute("ALTER TABLE " + MdrmConstants.DEFAULT_MASTER_TABLE
                        + " ADD COLUMN " + stagingColumn + " TEXT");
            }
        }

        ensureMasterIndexes();
    }

    /**
     * Creates a lightweight staging table at startup; load will rebuild it from CSV headers.
     */
    private void ensureBaseStagingTable() {
        String stagingTableName = sanitizeTableName(mdrmProperties.getStagingTable());
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS " + stagingTableName + " (run_id BIGINT)");
    }

    /**
     * Creates master table with core columns so reporting APIs can target it before first load.
     */
    private void ensureBaseMasterTable() {
        jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS " + MdrmConstants.DEFAULT_MASTER_TABLE + " ("
                + "master_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, "
                + "run_id BIGINT NOT NULL, "
                + "reporting_form TEXT, "
                + "start_date_utc TIMESTAMP, "
                + "end_date_utc TIMESTAMP, "
                + "mdrm_code TEXT, "
                + "is_active CHAR(1)"
                + ")");
    }

    /**
     * Ensures required reporting/performance indexes exist on the master table.
     */
    private void ensureMasterIndexes() {
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_mdrm_master_mdrm_code ON "
                + MdrmConstants.DEFAULT_MASTER_TABLE + " (mdrm_code)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_mdrm_master_reporting_form ON "
                + MdrmConstants.DEFAULT_MASTER_TABLE + " (reporting_form)");
    }

    /**
     * Ensures summary lookup indexes exist for reporting-form and run filters.
     */
    private void ensureRunSummaryIndexes() {
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_mdrm_run_summary_form_run ON "
                + MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE + " (reporting_form, run_id DESC)");
    }

    private void ensureRunIncrementalIndexes() {
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_mdrm_run_incremental_form_run ON "
                + MdrmConstants.DEFAULT_RUN_INCREMENTAL_TABLE + " (reporting_form, run_id DESC)");
    }

    private void ensureFileSummaryIndexes() {
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_mdrm_file_summary_datetime ON "
                + MdrmConstants.DEFAULT_FILE_SUMMARY_TABLE + " (run_datetime DESC)");
    }

    /**
     * Clears all MDRM tables to support a production-style fresh migration run.
     */
    private void truncateAllMdrmTables() {
        jdbcTemplate.execute("DELETE FROM " + MdrmConstants.DEFAULT_FILE_SUMMARY_TABLE);
        jdbcTemplate.execute("DELETE FROM " + MdrmConstants.DEFAULT_RUN_INCREMENTAL_TABLE);
        jdbcTemplate.execute("DELETE FROM " + MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE);
        jdbcTemplate.execute("DELETE FROM " + MdrmConstants.DEFAULT_RUN_ERROR_TABLE);
        jdbcTemplate.execute("DELETE FROM " + MdrmConstants.DEFAULT_MASTER_TABLE);
        jdbcTemplate.execute("DELETE FROM " + MdrmConstants.DEFAULT_RUN_MASTER_TABLE);
        String stagingTableName = sanitizeTableName(mdrmProperties.getStagingTable());
        jdbcTemplate.execute(MdrmConstants.SQL_DROP_TABLE_IF_EXISTS + stagingTableName);
        ensureBaseStagingTable();
    }

    private void ensureColumnExists(String tableName, String columnName, String sqlType) {
        List<String> columns = getTableColumns(tableName);
        if (!columns.contains(columnName)) {
            jdbcTemplate.execute("ALTER TABLE " + tableName + " ADD COLUMN " + columnName + " " + sqlType);
        }
    }

    private void syncRunSummaryFileNames() {
        String sql = """
                UPDATE __RUN_SUMMARY_TABLE__ s
                SET file_name = rm.file_name
                FROM __RUN_MASTER_TABLE__ rm
                WHERE s.run_id = rm.run_id
                  AND (s.file_name IS NULL OR btrim(s.file_name) = '')
                """
                .replace("__RUN_SUMMARY_TABLE__", MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE)
                .replace("__RUN_MASTER_TABLE__", MdrmConstants.DEFAULT_RUN_MASTER_TABLE);
        jdbcTemplate.execute(sql);
    }

    /**
     * Recomputes summary rows for one run across all reporting forms in that run.
     */
    private void refreshRunSummary(long runId) {
        String deleteSql = "DELETE FROM " + MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE + " WHERE run_id = ?";
        jdbcTemplate.update(deleteSql, runId);

        String insertSql = """
                WITH code_flags AS (
                    SELECT
                        m.run_id,
                        m.reporting_form,
                        m.mdrm_code,
                        MAX(CASE WHEN m.is_active = 'Y' THEN 1 ELSE 0 END) AS has_active,
                        MAX(CASE WHEN m.is_active = 'N' THEN 1 ELSE 0 END) AS has_inactive
                    FROM __MASTER_TABLE__ m
                    WHERE m.run_id = ?
                      AND m.reporting_form IS NOT NULL
                      AND btrim(m.reporting_form) <> ''
                      AND m.mdrm_code IS NOT NULL
                      AND btrim(m.mdrm_code) <> ''
                    GROUP BY m.run_id, m.reporting_form, m.mdrm_code
                )
                INSERT INTO __RUN_SUMMARY_TABLE__ (
                    run_id, reporting_form, file_name, total_unique_mdrms, active_mdrms, inactive_mdrms, updated_mdrms
                )
                SELECT
                    c.run_id,
                    c.reporting_form,
                    (SELECT rm.file_name FROM __RUN_MASTER_TABLE__ rm WHERE rm.run_id = ?) AS file_name,
                    COUNT(*) AS total_unique_mdrms,
                    SUM(CASE WHEN c.has_active = 1 AND c.has_inactive = 0 THEN 1 ELSE 0 END) AS active_mdrms,
                    SUM(CASE WHEN c.has_active = 0 AND c.has_inactive = 1 THEN 1 ELSE 0 END) AS inactive_mdrms,
                    SUM(CASE WHEN c.has_active = 1 AND c.has_inactive = 1 THEN 1 ELSE 0 END) AS updated_mdrms
                FROM code_flags c
                GROUP BY c.run_id, c.reporting_form
                """
                .replace("__MASTER_TABLE__", MdrmConstants.DEFAULT_MASTER_TABLE)
                .replace("__RUN_SUMMARY_TABLE__", MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE)
                .replace("__RUN_MASTER_TABLE__", MdrmConstants.DEFAULT_RUN_MASTER_TABLE);

        jdbcTemplate.update(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(insertSql);
                    ps.setLong(1, runId);
                    ps.setLong(2, runId);
                    return ps;
                }
        );
    }

    /**
     * Backfills summary rows only for run/form combinations that do not yet exist.
     */
    private void backfillMissingRunSummaries() {
        ensureRunSummaryTable();
        String sql = """
                WITH code_flags AS (
                    SELECT
                        m.run_id,
                        m.reporting_form,
                        m.mdrm_code,
                        MAX(CASE WHEN m.is_active = 'Y' THEN 1 ELSE 0 END) AS has_active,
                        MAX(CASE WHEN m.is_active = 'N' THEN 1 ELSE 0 END) AS has_inactive
                    FROM __MASTER_TABLE__ m
                    WHERE m.reporting_form IS NOT NULL
                      AND btrim(m.reporting_form) <> ''
                      AND m.mdrm_code IS NOT NULL
                      AND btrim(m.mdrm_code) <> ''
                    GROUP BY m.run_id, m.reporting_form, m.mdrm_code
                ),
                aggregated AS (
                    SELECT
                        c.run_id,
                        c.reporting_form,
                        COUNT(*) AS total_unique_mdrms,
                        SUM(CASE WHEN c.has_active = 1 AND c.has_inactive = 0 THEN 1 ELSE 0 END) AS active_mdrms,
                        SUM(CASE WHEN c.has_active = 0 AND c.has_inactive = 1 THEN 1 ELSE 0 END) AS inactive_mdrms,
                        SUM(CASE WHEN c.has_active = 1 AND c.has_inactive = 1 THEN 1 ELSE 0 END) AS updated_mdrms
                    FROM code_flags c
                    GROUP BY c.run_id, c.reporting_form
                )
                INSERT INTO __RUN_SUMMARY_TABLE__ (
                    run_id, reporting_form, file_name, total_unique_mdrms, active_mdrms, inactive_mdrms, updated_mdrms
                )
                SELECT
                    a.run_id,
                    a.reporting_form,
                    rm.file_name,
                    a.total_unique_mdrms,
                    a.active_mdrms,
                    a.inactive_mdrms,
                    a.updated_mdrms
                FROM aggregated a
                LEFT JOIN __RUN_MASTER_TABLE__ rm ON rm.run_id = a.run_id
                LEFT JOIN __RUN_SUMMARY_TABLE__ s
                  ON s.run_id = a.run_id AND s.reporting_form = a.reporting_form
                WHERE s.run_id IS NULL
                """
                .replace("__MASTER_TABLE__", MdrmConstants.DEFAULT_MASTER_TABLE)
                .replace("__RUN_SUMMARY_TABLE__", MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE)
                .replace("__RUN_MASTER_TABLE__", MdrmConstants.DEFAULT_RUN_MASTER_TABLE);

        jdbcTemplate.execute(sql);
    }

    /**
     * Recomputes incremental added/modified/deleted counts for a run against each reporting form's previous run.
     */
    private void refreshRunIncremental(long runId) {
        ensureRunIncrementalTable();
        jdbcTemplate.update("DELETE FROM " + MdrmConstants.DEFAULT_RUN_INCREMENTAL_TABLE + " WHERE run_id = ?", runId);

        String sql = """
                WITH current_code AS (
                    SELECT
                        m.reporting_form,
                        m.mdrm_code,
                        COUNT(*) AS row_count,
                        MAX(CASE WHEN m.is_active = 'Y' THEN 1 ELSE 0 END) AS has_active,
                        MAX(CASE WHEN m.is_active = 'N' THEN 1 ELSE 0 END) AS has_inactive,
                        MIN(m.start_date_utc) AS min_start,
                        MAX(m.end_date_utc) AS max_end
                    FROM __MASTER_TABLE__ m
                    WHERE m.run_id = ?
                      AND m.reporting_form IS NOT NULL
                      AND btrim(m.reporting_form) <> ''
                      AND m.mdrm_code IS NOT NULL
                      AND btrim(m.mdrm_code) <> ''
                    GROUP BY m.reporting_form, m.mdrm_code
                ),
                forms AS (
                    SELECT DISTINCT reporting_form FROM current_code
                ),
                previous_run AS (
                    SELECT
                        f.reporting_form,
                        (SELECT MAX(s.run_id)
                         FROM __RUN_SUMMARY_TABLE__ s
                         WHERE s.reporting_form = f.reporting_form
                           AND s.run_id < ?) AS prev_run_id
                    FROM forms f
                ),
                previous_code AS (
                    SELECT
                        m.reporting_form,
                        m.mdrm_code,
                        COUNT(*) AS row_count,
                        MAX(CASE WHEN m.is_active = 'Y' THEN 1 ELSE 0 END) AS has_active,
                        MAX(CASE WHEN m.is_active = 'N' THEN 1 ELSE 0 END) AS has_inactive,
                        MIN(m.start_date_utc) AS min_start,
                        MAX(m.end_date_utc) AS max_end
                    FROM __MASTER_TABLE__ m
                    JOIN previous_run p ON p.reporting_form = m.reporting_form AND p.prev_run_id = m.run_id
                    WHERE m.mdrm_code IS NOT NULL
                      AND btrim(m.mdrm_code) <> ''
                    GROUP BY m.reporting_form, m.mdrm_code
                ),
                merged AS (
                    SELECT
                        COALESCE(c.reporting_form, p.reporting_form) AS reporting_form,
                        c.mdrm_code AS current_code,
                        p.mdrm_code AS previous_code,
                        c.row_count AS c_row_count,
                        p.row_count AS p_row_count,
                        c.has_active AS c_has_active,
                        p.has_active AS p_has_active,
                        c.has_inactive AS c_has_inactive,
                        p.has_inactive AS p_has_inactive,
                        c.min_start AS c_min_start,
                        p.min_start AS p_min_start,
                        c.max_end AS c_max_end,
                        p.max_end AS p_max_end
                    FROM current_code c
                    FULL OUTER JOIN previous_code p
                      ON p.reporting_form = c.reporting_form
                     AND p.mdrm_code = c.mdrm_code
                )
                INSERT INTO __RUN_INCREMENTAL_TABLE__ (run_id, reporting_form, added_mdrms, modified_mdrms, deleted_mdrms)
                SELECT
                    ? AS run_id,
                    m.reporting_form,
                    SUM(CASE WHEN m.current_code IS NOT NULL AND m.previous_code IS NULL THEN 1 ELSE 0 END) AS added_mdrms,
                    SUM(CASE WHEN m.current_code IS NOT NULL AND m.previous_code IS NOT NULL
                               AND (m.c_row_count <> m.p_row_count
                                 OR m.c_has_active <> m.p_has_active
                                 OR m.c_has_inactive <> m.p_has_inactive
                                 OR m.c_min_start <> m.p_min_start
                                 OR m.c_max_end <> m.p_max_end)
                             THEN 1 ELSE 0 END) AS modified_mdrms,
                    SUM(CASE WHEN m.current_code IS NULL AND m.previous_code IS NOT NULL THEN 1 ELSE 0 END) AS deleted_mdrms
                FROM merged m
                GROUP BY m.reporting_form
                """
                .replace("__MASTER_TABLE__", MdrmConstants.DEFAULT_MASTER_TABLE)
                .replace("__RUN_SUMMARY_TABLE__", MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE)
                .replace("__RUN_INCREMENTAL_TABLE__", MdrmConstants.DEFAULT_RUN_INCREMENTAL_TABLE);

        jdbcTemplate.update(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ps.setLong(1, runId);
                    ps.setLong(2, runId);
                    ps.setLong(3, runId);
                    return ps;
                }
        );
    }

    private void refreshFileSummary(long runId) {
        ensureFileSummaryTable();
        jdbcTemplate.update("DELETE FROM " + MdrmConstants.DEFAULT_FILE_SUMMARY_TABLE + " WHERE run_id = ?", runId);

        String sql = """
                INSERT INTO __FILE_SUMMARY_TABLE__ (
                    run_id, run_datetime, file_name, num_file_records, num_records_ingested, num_records_error,
                    reports_count, total_unique_mdrms, active_mdrms, inactive_mdrms, updated_mdrms
                )
                SELECT
                    rm.run_id,
                    rm.run_datetime,
                    rm.file_name,
                    rm.num_file_records,
                    rm.num_records_ingested,
                    rm.num_records_error,
                    COALESCE(COUNT(rs.reporting_form), 0) AS reports_count,
                    COALESCE(SUM(rs.total_unique_mdrms), 0) AS total_unique_mdrms,
                    COALESCE(SUM(rs.active_mdrms), 0) AS active_mdrms,
                    COALESCE(SUM(rs.inactive_mdrms), 0) AS inactive_mdrms,
                    COALESCE(SUM(rs.updated_mdrms), 0) AS updated_mdrms
                FROM __RUN_MASTER_TABLE__ rm
                LEFT JOIN __RUN_SUMMARY_TABLE__ rs ON rs.run_id = rm.run_id
                WHERE rm.run_id = ?
                GROUP BY rm.run_id, rm.run_datetime, rm.file_name, rm.num_file_records, rm.num_records_ingested, rm.num_records_error
                """
                .replace("__FILE_SUMMARY_TABLE__", MdrmConstants.DEFAULT_FILE_SUMMARY_TABLE)
                .replace("__RUN_MASTER_TABLE__", MdrmConstants.DEFAULT_RUN_MASTER_TABLE)
                .replace("__RUN_SUMMARY_TABLE__", MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE);

        jdbcTemplate.update(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ps.setLong(1, runId);
                    return ps;
                }
        );
    }

    private void backfillMissingRunIncrementals() {
        ensureRunIncrementalTable();
        String sql = """
                SELECT rm.run_id
                FROM __RUN_MASTER_TABLE__ rm
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM __RUN_INCREMENTAL_TABLE__ ri
                    WHERE ri.run_id = rm.run_id
                )
                ORDER BY rm.run_id
                """
                .replace("__RUN_MASTER_TABLE__", MdrmConstants.DEFAULT_RUN_MASTER_TABLE)
                .replace("__RUN_INCREMENTAL_TABLE__", MdrmConstants.DEFAULT_RUN_INCREMENTAL_TABLE);

        List<Long> runIds = jdbcTemplate.query(sql, (rs, rowNum) -> rs.getLong("run_id"));
        for (Long runId : runIds) {
            refreshRunIncremental(runId);
        }
    }

    private void backfillMissingFileSummaries() {
        ensureFileSummaryTable();
        String sql = """
                SELECT rm.run_id
                FROM __RUN_MASTER_TABLE__ rm
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM __FILE_SUMMARY_TABLE__ fs
                    WHERE fs.run_id = rm.run_id
                )
                ORDER BY rm.run_id
                """
                .replace("__RUN_MASTER_TABLE__", MdrmConstants.DEFAULT_RUN_MASTER_TABLE)
                .replace("__FILE_SUMMARY_TABLE__", MdrmConstants.DEFAULT_FILE_SUMMARY_TABLE);

        List<Long> runIds = jdbcTemplate.query(sql, (rs, rowNum) -> rs.getLong("run_id"));
        for (Long runId : runIds) {
            refreshFileSummary(runId);
        }
    }

    /**
     * Creates PostgreSQL promotion function used to process staged rows inside the database.
     */
    private void ensurePostgresPromotionFunction() {
        if (!isPostgreSql()) {
            return;
        }

        String sql = """
                CREATE OR REPLACE FUNCTION __PROMOTE_FN__(p_run_id BIGINT, p_staging_table TEXT)
                RETURNS TABLE(ingested_count INT, error_count INT)
                LANGUAGE plpgsql
                AS $$
                DECLARE
                    data_columns TEXT;
                    valid_condition TEXT;
                    insert_sql TEXT;
                    error_sql TEXT;
                    inserted_rows INT := 0;
                    error_rows INT := 0;
                BEGIN
                    SELECT string_agg(quote_ident(column_name), ', ' ORDER BY ordinal_position)
                    INTO data_columns
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = p_staging_table
                      AND column_name <> 'run_id';

                    IF data_columns IS NULL THEN
                        data_columns := '';
                    END IF;

                    valid_condition := '('
                        || 'start_date IS NOT NULL AND btrim(start_date) <> '''' '
                        || 'AND end_date IS NOT NULL AND btrim(end_date) <> '''' '
                        || 'AND start_date ~ ''^([1-9]|0[1-9]|1[0-2])/([1-9]|0[1-9]|[12][0-9]|3[01])/[0-9]{4} ([1-9]|0[1-9]|1[0-2]):[0-5][0-9]:[0-5][0-9] (AM|PM)$'' '
                        || 'AND end_date ~ ''^([1-9]|0[1-9]|1[0-2])/([1-9]|0[1-9]|[12][0-9]|3[01])/[0-9]{4} ([1-9]|0[1-9]|1[0-2]):[0-5][0-9]:[0-5][0-9] (AM|PM)$'''
                        || ')';

                    error_sql := format(
                        'INSERT INTO %I (run_id, raw_record, error_description) '
                        || 'SELECT run_id, row_to_json(s)::text, '
                        || 'CASE '
                        || 'WHEN start_date IS NULL OR btrim(start_date) = '''' OR end_date IS NULL OR btrim(end_date) = '''' '
                        || 'THEN ''Date value is null/blank'' '
                        || 'ELSE ''Invalid date format'' '
                        || 'END '
                        || 'FROM %I s '
                        || 'WHERE run_id = $1 AND NOT %s',
                        '__RUN_ERROR_TABLE__',
                        p_staging_table,
                        valid_condition
                    );
                    EXECUTE error_sql USING p_run_id;
                    GET DIAGNOSTICS error_rows = ROW_COUNT;

                    insert_sql := format(
                        'INSERT INTO %I (run_id%sstart_date_utc, end_date_utc, mdrm_code, is_active) '
                        || 'SELECT run_id%s'
                        || 'to_timestamp(start_date, ''MM/DD/YYYY HH12:MI:SS AM'') AT TIME ZONE ''UTC'', '
                        || 'to_timestamp(end_date, ''MM/DD/YYYY HH12:MI:SS AM'') AT TIME ZONE ''UTC'', '
                        || 'NULLIF(COALESCE(mnemonic, '''') || COALESCE(item_code, ''''), ''''), '
                        || 'CASE '
                        || 'WHEN EXTRACT(YEAR FROM (to_timestamp(end_date, ''MM/DD/YYYY HH12:MI:SS AM'') AT TIME ZONE ''UTC'')) = 9999 '
                        || 'OR (to_timestamp(end_date, ''MM/DD/YYYY HH12:MI:SS AM'') AT TIME ZONE ''UTC'') > (NOW() AT TIME ZONE ''UTC'') '
                        || 'THEN ''Y'' ELSE ''N'' END '
                        || 'FROM %I '
                        || 'WHERE run_id = $1 AND %s',
                        '__MASTER_TABLE__',
                        CASE WHEN data_columns = '' THEN '' ELSE ', ' || data_columns || ', ' END,
                        CASE WHEN data_columns = '' THEN '' ELSE ', ' || data_columns || ', ' END,
                        p_staging_table,
                        valid_condition
                    );
                    EXECUTE insert_sql USING p_run_id;
                    GET DIAGNOSTICS inserted_rows = ROW_COUNT;

                    RETURN QUERY SELECT inserted_rows, error_rows;
                END;
                $$;
                """
                .replace("__PROMOTE_FN__", MdrmConstants.DEFAULT_PROMOTE_FUNCTION)
                .replace("__RUN_ERROR_TABLE__", MdrmConstants.DEFAULT_RUN_ERROR_TABLE)
                .replace("__MASTER_TABLE__", MdrmConstants.DEFAULT_MASTER_TABLE);

        jdbcTemplate.execute(sql);
    }

    /**
     * Executes the PostgreSQL promotion function for a run and returns processed counts.
     */
    private IngestionStats promoteFromStagingUsingProcedure(String stagingTableName, long runId) {
        String sql = "SELECT ingested_count, error_count FROM " + MdrmConstants.DEFAULT_PROMOTE_FUNCTION + "(?, ?)";
        return jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ps.setLong(1, runId);
                    ps.setString(2, stagingTableName);
                    return ps;
                },
                rs -> {
                    if (!rs.next()) {
                        return new IngestionStats(0, 0);
                    }
                    return new IngestionStats(rs.getInt("ingested_count"), rs.getInt("error_count"));
                }
        );
    }


    /**
     * Drops and recreates staging table using sanitized CSV headers plus run_id.
     */
    private void recreateStagingTable(String tableName, List<String> columns) {
        if (columns.isEmpty()) {
            throw new IllegalStateException(MdrmConstants.MSG_NO_HEADERS_FOR_TABLE);
        }

        jdbcTemplate.execute(MdrmConstants.SQL_DROP_TABLE_IF_EXISTS + tableName);

        List<String> stagingColumns = new ArrayList<>(columns.size() + 1);
        stagingColumns.add(MdrmConstants.COLUMN_RUN_ID + " BIGINT");
        for (String column : columns) {
            stagingColumns.add(column + " TEXT");
        }

        String columnSql = String.join(", ", stagingColumns);
        jdbcTemplate.execute(MdrmConstants.SQL_CREATE_TABLE_PREFIX + tableName + " (" + columnSql + MdrmConstants.SQL_CLOSE_PAREN);
    }

    /**
     * Returns CSV payload bytes either directly from file content or by extracting first text entry from ZIP.
     */
    private ZipContent extractContent(DownloadedMdrm downloadedMdrm) {
        if (downloadedMdrm.fileName() != null
                && downloadedMdrm.fileName().toLowerCase(Locale.ROOT).endsWith(MdrmConstants.FILE_EXT_ZIP)) {
            return extractFromZip(downloadedMdrm.content());
        }

        return new ZipContent(downloadedMdrm.fileName(), downloadedMdrm.content());
    }

    /**
     * Extracts the first supported text-like entry from a ZIP payload.
     */
    private ZipContent extractFromZip(byte[] zipBytes) {
        try (ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (!entry.isDirectory() && isTextFile(entry.getName())) {
                    return new ZipContent(entry.getName(), zis.readAllBytes());
                }
            }
        } catch (IOException ex) {
            throw new IllegalStateException(MdrmConstants.MSG_ZIP_READ_FAILED, ex);
        }

        throw new IllegalStateException(MdrmConstants.MSG_ZIP_NO_TEXT_FILE);
    }

    /**
     * Parses MDRM CSV where line 1 is metadata and line 2 is the header row, then loads rows into staging.
     */
    private int parseCsvAndLoadToStaging(byte[] content, String tableName, long runId) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content), StandardCharsets.UTF_8))) {
            String metadataLine = reader.readLine();
            if (metadataLine == null) {
                throw new IllegalStateException(MdrmConstants.MSG_CSV_EMPTY);
            }

            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setTrim(true)
                    .setAllowMissingColumnNames(true)
                    .build();

            try (CSVParser parser = csvFormat.parse(reader)) {
                List<String> headerNames = parser.getHeaderNames();
                if (headerNames.isEmpty()) {
                    throw new IllegalStateException(MdrmConstants.MSG_CSV_HEADER_NOT_FOUND);
                }

                List<String> columns = sanitizeCsvHeaders(headerNames);
                recreateStagingTable(tableName, columns);

                String placeholders = String.join(", ", Collections.nCopies(columns.size() + 1, "?"));
                String sql = MdrmConstants.SQL_INSERT_VALUES_PREFIX + tableName
                        + MdrmConstants.SQL_INSERT_VALUES_INFIX + placeholders + MdrmConstants.SQL_CLOSE_PAREN;

                int insertedRows = 0;

                try (Connection connection = jdbcTemplate.getDataSource().getConnection();
                     PreparedStatement ps = connection.prepareStatement(sql)) {
                    int batchCount = 0;

                    for (CSVRecord record : parser) {
                        ps.setLong(1, runId);
                        for (int i = 0; i < headerNames.size(); i++) {
                            ps.setString(i + 2, record.isSet(i) ? record.get(i) : null);
                        }
                        ps.addBatch();
                        batchCount++;
                        insertedRows++;

                        if (batchCount >= MdrmConstants.CSV_STAGING_BATCH_SIZE) {
                            ps.executeBatch();
                            batchCount = 0;
                        }
                    }

                    if (batchCount > 0) {
                        ps.executeBatch();
                    }
                }

                return insertedRows;
            }
        } catch (IOException | SQLException ex) {
            throw new IllegalStateException(MdrmConstants.MSG_CSV_PARSE_LOAD_FAILED, ex);
        }
    }

    private int countRunErrors(long runId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + MdrmConstants.DEFAULT_RUN_ERROR_TABLE + " WHERE run_id = ?",
                Integer.class,
                runId
        );
        return count == null ? 0 : count;
    }

    private void validateUploadFileName(String fileName) {
        if (fileName == null || !UPLOAD_FILE_PATTERN.matcher(fileName).matches()) {
            throw new IllegalArgumentException(MdrmConstants.MSG_INVALID_UPLOAD_FILE_NAME);
        }
    }

    private void validateUploadedHeaders(byte[] content) {
        List<String> uploadedHeaders = extractSanitizedHeaders(content);
        List<String> currentHeaders = getCurrentStagingHeaders();
        if (currentHeaders.isEmpty()) {
            return;
        }

        Set<String> uploadedSet = new HashSet<>(uploadedHeaders);
        Set<String> currentSet = new HashSet<>(currentHeaders);
        if (uploadedSet.equals(currentSet)) {
            return;
        }

        List<String> missing = new ArrayList<>();
        for (String expected : currentHeaders) {
            if (!uploadedSet.contains(expected)) {
                missing.add(expected);
            }
        }

        List<String> unexpected = new ArrayList<>();
        for (String uploaded : uploadedHeaders) {
            if (!currentSet.contains(uploaded)) {
                unexpected.add(uploaded);
            }
        }

        throw new IllegalArgumentException("Uploaded file headers do not match expected structure. Missing="
                + missing + ", Unexpected=" + unexpected);
    }

    private List<String> extractSanitizedHeaders(byte[] content) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(content), StandardCharsets.UTF_8))) {
            String metadataLine = reader.readLine();
            if (metadataLine == null) {
                throw new IllegalStateException(MdrmConstants.MSG_CSV_EMPTY);
            }

            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setTrim(true)
                    .setAllowMissingColumnNames(true)
                    .build();

            try (CSVParser parser = csvFormat.parse(reader)) {
                List<String> headerNames = parser.getHeaderNames();
                if (headerNames.isEmpty()) {
                    throw new IllegalStateException(MdrmConstants.MSG_CSV_HEADER_NOT_FOUND);
                }
                return sanitizeCsvHeaders(headerNames);
            }
        } catch (IOException ex) {
            throw new IllegalStateException(MdrmConstants.MSG_CSV_PARSE_LOAD_FAILED, ex);
        }
    }

    private List<String> getCurrentStagingHeaders() {
        String stagingTableName = sanitizeTableName(mdrmProperties.getStagingTable());
        List<String> headers = new ArrayList<>();
        try {
            for (String column : getTableColumns(stagingTableName)) {
                if (!MdrmConstants.COLUMN_RUN_ID.equals(column)) {
                    headers.add(column);
                }
            }
            return headers;
        } catch (Exception ex) {
            return List.of();
        }
    }

    private boolean hasPreviousRun(long runId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + MdrmConstants.DEFAULT_RUN_MASTER_TABLE + " WHERE run_id < ?",
                Integer.class,
                runId
        );
        return count != null && count > 0;
    }

    private boolean hasAnyIncrementalChange(long runId) {
        Integer total = jdbcTemplate.queryForObject(
                "SELECT COALESCE(SUM(added_mdrms + modified_mdrms + deleted_mdrms), 0) FROM "
                        + MdrmConstants.DEFAULT_RUN_INCREMENTAL_TABLE + " WHERE run_id = ?",
                Integer.class,
                runId
        );
        return total != null && total > 0;
    }

    private void deleteRunData(long runId, String stagingTableName) {
        jdbcTemplate.update("DELETE FROM " + stagingTableName + " WHERE run_id = ?", runId);
        jdbcTemplate.update("DELETE FROM " + MdrmConstants.DEFAULT_FILE_SUMMARY_TABLE + " WHERE run_id = ?", runId);
        jdbcTemplate.update("DELETE FROM " + MdrmConstants.DEFAULT_RUN_INCREMENTAL_TABLE + " WHERE run_id = ?", runId);
        jdbcTemplate.update("DELETE FROM " + MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE + " WHERE run_id = ?", runId);
        jdbcTemplate.update("DELETE FROM " + MdrmConstants.DEFAULT_RUN_ERROR_TABLE + " WHERE run_id = ?", runId);
        jdbcTemplate.update("DELETE FROM " + MdrmConstants.DEFAULT_MASTER_TABLE + " WHERE run_id = ?", runId);
        jdbcTemplate.update("DELETE FROM " + MdrmConstants.DEFAULT_RUN_MASTER_TABLE + " WHERE run_id = ?", runId);
    }

    /**
     * Determines whether a ZIP entry is a supported text data file.
     */
    private boolean isTextFile(String name) {
        String lower = name.toLowerCase(Locale.ROOT);
        return lower.endsWith(MdrmConstants.FILE_EXT_TXT)
                || lower.endsWith(MdrmConstants.FILE_EXT_CSV)
                || lower.endsWith(MdrmConstants.FILE_EXT_DAT);
    }

    /**
     * Protects dynamic SQL usage by allowing only safe identifier characters.
     */
    private String sanitizeTableName(String input) {
        if (input == null || !input.matches(MdrmConstants.VALID_TABLE_NAME_REGEX)) {
            throw new IllegalArgumentException(MdrmConstants.MSG_INVALID_STAGING_TABLE.formatted(input));
        }
        return input;
    }

    /**
     * Normalizes CSV headers to DB-safe, unique snake_case column names.
     */
    private List<String> sanitizeCsvHeaders(List<String> headers) {
        List<String> sanitized = new ArrayList<>(headers.size());
        Map<String, Integer> seen = new LinkedHashMap<>();

        for (int i = 0; i < headers.size(); i++) {
            String header = headers.get(i) == null ? "" : headers.get(i).trim();
            String value = header.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]", "_");
            value = value.replaceAll("_+", "_");
            value = value.replaceAll("^_+|_+$", "");
            if (value.isEmpty()) {
                value = MdrmConstants.CSV_DEFAULT_COLUMN_PREFIX + (i + 1);
            }
            if (!Character.isLetter(value.charAt(0)) && value.charAt(0) != '_') {
                value = MdrmConstants.CSV_DEFAULT_COLUMN_PREFIX + value;
            }

            int count = seen.getOrDefault(value, 0);
            seen.put(value, count + 1);
            sanitized.add(count == 0 ? value : value + "_" + (count + 1));
        }
        return sanitized;
    }

    private MdrmTableResponse extractTableResponse(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        List<String> columns = new ArrayList<>(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            columns.add(metaData.getColumnLabel(i));
        }

        List<Map<String, String>> rows = new ArrayList<>();
        while (rs.next()) {
            Map<String, String> row = new LinkedHashMap<>();
            for (String column : columns) {
                row.put(column, rs.getString(column));
            }
            rows.add(row);
        }
        return new MdrmTableResponse(columns, rows);
    }

    private void ensureReportingFormColumnExists(String tableName) {
        try {
            MdrmTableResponse tableResponse = jdbcTemplate.query(
                    MdrmConstants.SQL_SELECT_ALL_BY_REPORTING_FORM_PREFIX + tableName + MdrmConstants.SQL_SELECT_SINGLE_ROW_SUFFIX,
                    this::extractTableResponse
            );
            if (tableResponse == null || !tableResponse.columns().contains(MdrmConstants.COLUMN_REPORTING_FORM)) {
                throw new IllegalStateException(MdrmConstants.MSG_REPORTING_FORM_COLUMN_MISSING.formatted(tableName));
            }
        } catch (Exception ex) {
            throw new IllegalStateException(MdrmConstants.MSG_REPORTING_FORM_COLUMN_MISSING.formatted(tableName), ex);
        }
    }

    private void ensureRequiredColumnExists(List<String> columns, String requiredColumn, String tableName) {
        if (!columns.contains(requiredColumn)) {
            throw new IllegalStateException(MdrmConstants.MSG_REQUIRED_COLUMN_MISSING.formatted(requiredColumn, tableName));
        }
    }

    private List<String> getTableColumns(String tableName) {
        return jdbcTemplate.query(
                MdrmConstants.SQL_SELECT_ALL_BY_REPORTING_FORM_PREFIX + tableName + MdrmConstants.SQL_SELECT_SINGLE_ROW_SUFFIX,
                rs -> {
                    ResultSetMetaData meta = rs.getMetaData();
                    List<String> columns = new ArrayList<>();
                    for (int i = 1; i <= meta.getColumnCount(); i++) {
                        columns.add(meta.getColumnLabel(i));
                    }
                    return columns;
                }
        );
    }

    private boolean isPostgreSql() {
        try (Connection connection = jdbcTemplate.getDataSource().getConnection()) {
            String productName = connection.getMetaData().getDatabaseProductName();
            return productName != null && productName.toLowerCase(Locale.ROOT).contains("postgresql");
        } catch (SQLException ex) {
            throw new IllegalStateException("Unable to determine database product", ex);
        }
    }

    private long generateRunId() {
        long candidate = Instant.now().toEpochMilli();
        for (int attempt = 0; attempt < 1000; attempt++) {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM " + MdrmConstants.DEFAULT_RUN_MASTER_TABLE + " WHERE run_id = ?",
                    Integer.class,
                    candidate
            );
            if (count == null || count == 0) {
                return candidate;
            }
            candidate++;
        }
        throw new IllegalStateException(MdrmConstants.MSG_RUN_ID_NOT_GENERATED);
    }

    private String normalizeBucket(String bucket) {
        String normalized = bucket == null ? "" : bucket.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "TOTAL", "ACTIVE", "INACTIVE", "UPDATED" -> normalized;
            default -> throw new IllegalArgumentException("Unsupported bucket: " + bucket);
        };
    }

    private String bucketConditionSql(String bucket) {
        return switch (bucket) {
            case "TOTAL" -> "1 = 1";
            case "ACTIVE" -> "c.has_active = 1 AND c.has_inactive = 0";
            case "INACTIVE" -> "c.has_active = 0 AND c.has_inactive = 1";
            case "UPDATED" -> "c.has_active = 1 AND c.has_inactive = 1";
            default -> throw new IllegalArgumentException("Unsupported bucket: " + bucket);
        };
    }

    private String normalizeIncrementalChangeType(String changeType) {
        String normalized = changeType == null ? "" : changeType.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case "ADDED", "MODIFIED", "DELETED" -> normalized;
            default -> throw new IllegalArgumentException("Unsupported incremental change type: " + changeType);
        };
    }

    private String incrementalConditionSql(String normalizedChangeType) {
        return switch (normalizedChangeType) {
            case "ADDED" -> "c.mdrm_code IS NOT NULL AND p.mdrm_code IS NULL";
            case "DELETED" -> "c.mdrm_code IS NULL AND p.mdrm_code IS NOT NULL";
            case "MODIFIED" -> "c.mdrm_code IS NOT NULL AND p.mdrm_code IS NOT NULL AND ("
                    + "c.row_count <> p.row_count OR "
                    + "c.has_active <> p.has_active OR "
                    + "c.has_inactive <> p.has_inactive OR "
                    + "c.min_start <> p.min_start OR "
                    + "c.max_end <> p.max_end)";
            default -> throw new IllegalArgumentException("Unsupported incremental change type: " + normalizedChangeType);
        };
    }

    private long findPreviousRunForReportingForm(String reportingForm, long runId) {
        Long previous = jdbcTemplate.queryForObject(
                "SELECT MAX(run_id) FROM " + MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE
                        + " WHERE reporting_form = ? AND run_id < ?",
                Long.class,
                reportingForm,
                runId
        );
        return previous == null ? 0 : previous;
    }

    private String inferReportingForm(String query) {
        String normalizedQuery = normalizeReportKey(query);
        if (normalizedQuery.isBlank()) {
            return null;
        }

        List<String> forms = getReportingForms();
        String best = null;
        int bestScore = -1;
        for (String form : forms) {
            String formKey = normalizeReportKey(form);
            if (formKey.isBlank()) {
                continue;
            }
            if (normalizedQuery.contains(formKey)) {
                int score = formKey.length();
                if (score > bestScore) {
                    bestScore = score;
                    best = form;
                }
            }
        }
        return best;
    }

    private List<String> extractSearchKeywords(String query, String interpretedReportingForm) {
        String cleaned = query == null ? "" : query.toLowerCase(Locale.ROOT);
        if (interpretedReportingForm != null && !interpretedReportingForm.isBlank()) {
            cleaned = cleaned.replace(interpretedReportingForm.toLowerCase(Locale.ROOT), " ");
            cleaned = cleaned.replace(normalizeReportKey(interpretedReportingForm), " ");
        }

        List<String> tokens = new ArrayList<>();
        for (String token : cleaned.split("[^a-z0-9]+")) {
            if (token == null || token.isBlank()) {
                continue;
            }
            if (SEARCH_STOP_WORDS.contains(token)) {
                continue;
            }
            tokens.add(token);
        }
        return tokens;
    }

    private String normalizeReportKey(String value) {
        if (value == null) {
            return "";
        }
        return value.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]", "");
    }

    private List<String> getSemanticSearchColumns() {
        List<String> available = getTableColumns(MdrmConstants.DEFAULT_MASTER_TABLE);
        List<String> preferred = List.of(
                "description",
                "definition",
                "reporting_form",
                "item_type",
                "mdrm_code",
                "mnemonic",
                "item_code"
        );
        List<String> selected = new ArrayList<>();
        for (String column : preferred) {
            if (available.contains(column)) {
                selected.add(column);
            }
        }
        return selected;
    }

    private String semanticSearchColumnExpression(String column, String alias) {
        String qualified = alias + "." + column;
        if ("item_type".equals(column)) {
            return "COALESCE(" + itemTypeValueExpression(qualified) + ", '')";
        }
        return "COALESCE(" + qualified + ", '')";
    }

    private String buildMasterSelectList(String alias) {
        List<String> columns = getTableColumns(MdrmConstants.DEFAULT_MASTER_TABLE);
        if (columns.isEmpty()) {
            return alias + ".*";
        }
        List<String> selected = new ArrayList<>(columns.size());
        for (String column : columns) {
            String qualified = alias + "." + column;
            if ("item_type".equals(column)) {
                selected.add(itemTypeValueExpression(qualified) + " AS item_type");
            } else {
                selected.add(qualified + " AS " + column);
            }
        }
        return String.join(", ", selected);
    }

    private String itemTypeValueExpression(String columnRef) {
        return """
                CASE UPPER(TRIM(%s))
                    WHEN 'J' THEN 'Projected'
                    WHEN 'D' THEN 'Derived'
                    WHEN 'F' THEN 'Financial/reported'
                    WHEN 'R' THEN 'Rate'
                    WHEN 'S' THEN 'Structure'
                    WHEN 'E' THEN 'Examination/supervision'
                    WHEN 'P' THEN 'Percentage'
                    ELSE %s
                END
                """.formatted(columnRef, columnRef).trim();
    }


    /**
     * Internal value object for file name + parsed content bytes.
     */
    private record ZipContent(String fileName, byte[] content) {
    }

    /**
     * Ingestion counters grouped for run finalization.
     */
    private record IngestionStats(int ingestedCount, int errorCount) {
    }
}
