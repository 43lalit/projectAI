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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Orchestrates MDRM file ingestion: reads source file, rebuilds staging table from CSV headers,
 * persists run metadata, transforms records to master table, and logs row-level ingestion errors.
 */
@Service
public class MdrmLoadService {

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
        ensureMasterIndexes();
        ensureRunSummaryIndexes();
        ensurePostgresPromotionFunction();
        backfillMissingRunSummaries();
    }

    /**
     * Executes a full MDRM load cycle and returns source file name with successfully ingested row count.
     */
    public MdrmLoadResult loadLatestMdrm() {
        DownloadedMdrm downloadedMdrm = mdrmDownloader.download();
        ZipContent content = extractContent(downloadedMdrm);

        ensureRunMasterTable();
        ensureRunErrorTable();
        ensureRunSummaryTable();
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
            return new MdrmLoadResult(content.fileName(), ingestionStats.ingestedCount());
        } catch (RuntimeException ex) {
            int errorRows = countRunErrors(runId);
            updateRunMasterCounts(runId, stagedRowCount, 0, errorRows);
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
        String sql = MdrmConstants.SQL_SELECT_ALL_BY_REPORTING_FORM_PREFIX + MdrmConstants.DEFAULT_MASTER_TABLE
                + " WHERE reporting_form = ? AND run_id = (SELECT MAX(run_id) FROM "
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
        String sql = """
                SELECT
                    s.run_id,
                    COALESCE(rm.run_datetime, s.run_id) AS run_datetime,
                    COALESCE(rm.file_name, '') AS file_name,
                    s.total_unique_mdrms,
                    s.active_mdrms,
                    s.inactive_mdrms,
                    s.updated_mdrms
                FROM __RUN_SUMMARY_TABLE__ s
                LEFT JOIN __RUN_MASTER_TABLE__ rm ON rm.run_id = s.run_id
                WHERE s.reporting_form = ?
                ORDER BY s.run_id DESC
                """
                .replace("__RUN_SUMMARY_TABLE__", MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE)
                .replace("__RUN_MASTER_TABLE__", MdrmConstants.DEFAULT_RUN_MASTER_TABLE);

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
                        rs.getInt("updated_mdrms")
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
                + "total_unique_mdrms INT NOT NULL, "
                + "active_mdrms INT NOT NULL, "
                + "inactive_mdrms INT NOT NULL, "
                + "updated_mdrms INT NOT NULL, "
                + "PRIMARY KEY (run_id, reporting_form)"
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
                    run_id, reporting_form, total_unique_mdrms, active_mdrms, inactive_mdrms, updated_mdrms
                )
                SELECT
                    c.run_id,
                    c.reporting_form,
                    COUNT(*) AS total_unique_mdrms,
                    SUM(CASE WHEN c.has_active = 1 AND c.has_inactive = 0 THEN 1 ELSE 0 END) AS active_mdrms,
                    SUM(CASE WHEN c.has_active = 0 AND c.has_inactive = 1 THEN 1 ELSE 0 END) AS inactive_mdrms,
                    SUM(CASE WHEN c.has_active = 1 AND c.has_inactive = 1 THEN 1 ELSE 0 END) AS updated_mdrms
                FROM code_flags c
                GROUP BY c.run_id, c.reporting_form
                """
                .replace("__MASTER_TABLE__", MdrmConstants.DEFAULT_MASTER_TABLE)
                .replace("__RUN_SUMMARY_TABLE__", MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE);

        jdbcTemplate.update(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(insertSql);
                    ps.setLong(1, runId);
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
                    run_id, reporting_form, total_unique_mdrms, active_mdrms, inactive_mdrms, updated_mdrms
                )
                SELECT
                    a.run_id, a.reporting_form, a.total_unique_mdrms, a.active_mdrms, a.inactive_mdrms, a.updated_mdrms
                FROM aggregated a
                LEFT JOIN __RUN_SUMMARY_TABLE__ s
                  ON s.run_id = a.run_id AND s.reporting_form = a.reporting_form
                WHERE s.run_id IS NULL
                """
                .replace("__MASTER_TABLE__", MdrmConstants.DEFAULT_MASTER_TABLE)
                .replace("__RUN_SUMMARY_TABLE__", MdrmConstants.DEFAULT_RUN_SUMMARY_TABLE);

        jdbcTemplate.execute(sql);
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
