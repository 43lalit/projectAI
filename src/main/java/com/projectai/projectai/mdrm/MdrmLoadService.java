package com.projectai.projectai.mdrm;

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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Orchestrates MDRM file ingestion: read source file, rebuild staging table from CSV headers,
 * and load all data rows into the configured staging table.
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
     * Executes a full MDRM load cycle and returns the source file name with inserted row count.
     */
    public MdrmLoadResult loadLatestMdrm() {
        DownloadedMdrm downloadedMdrm = mdrmDownloader.download();
        ZipContent content = extractContent(downloadedMdrm);

        String tableName = sanitizeTableName(mdrmProperties.getStagingTable());
        int rows = parseCsvAndLoad(content.content(), tableName);

        return new MdrmLoadResult(content.fileName(), rows);
    }

    /**
     * Returns all distinct reporting form values available in the current staging table.
     */
    public List<String> getReportingForms() {
        String tableName = sanitizeTableName(mdrmProperties.getStagingTable());
        ensureReportingFormColumnExists(tableName);
        return jdbcTemplate.queryForList(
                MdrmConstants.SQL_SELECT_DISTINCT_REPORTING_FORM_PREFIX + tableName
                        + MdrmConstants.SQL_SELECT_DISTINCT_REPORTING_FORM_SUFFIX,
                String.class
        );
    }

    /**
     * Returns all rows for a given reporting form with dynamic columns for table rendering.
     */
    public MdrmTableResponse getRowsByReportingForm(String reportingForm) {
        String tableName = sanitizeTableName(mdrmProperties.getStagingTable());
        ensureReportingFormColumnExists(tableName);
        String sql = MdrmConstants.SQL_SELECT_ALL_BY_REPORTING_FORM_PREFIX + tableName
                + MdrmConstants.SQL_SELECT_ALL_BY_REPORTING_FORM_SUFFIX;

        return jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ps.setString(1, reportingForm);
                    return ps;
                },
                this::extractTableResponse
        );
    }

    /**
     * Drops and recreates the staging table using sanitized CSV header names as TEXT columns.
     */
    private void recreateStagingTable(String tableName, List<String> columns) {
        if (columns.isEmpty()) {
            throw new IllegalStateException(MdrmConstants.MSG_NO_HEADERS_FOR_TABLE);
        }

        jdbcTemplate.execute(MdrmConstants.SQL_DROP_TABLE_IF_EXISTS + tableName);

        String columnSql = String.join(", ", columns.stream().map(c -> c + " TEXT").toList());
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
     * Parses MDRM CSV where line 1 is metadata and line 2 is the header row, then batch loads rows.
     */
    private int parseCsvAndLoad(byte[] content, String tableName) {
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

                String placeholders = String.join(", ", java.util.Collections.nCopies(columns.size(), "?"));
                String sql = MdrmConstants.SQL_INSERT_VALUES_PREFIX + tableName
                        + MdrmConstants.SQL_INSERT_VALUES_INFIX + placeholders + MdrmConstants.SQL_CLOSE_PAREN;

                int insertedRows = 0;
                int batchCount = 0;

                try (PreparedStatement ps = jdbcTemplate.getDataSource().getConnection().prepareStatement(sql)) {
                    for (CSVRecord record : parser) {
                        for (int i = 0; i < headerNames.size(); i++) {
                            ps.setString(i + 1, record.isSet(i) ? record.get(i) : null);
                        }
                        ps.addBatch();
                        batchCount++;
                        insertedRows++;

                        if (batchCount >= MdrmConstants.CSV_BATCH_SIZE) {
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

    /**
     * Internal value object for file name + parsed content bytes.
     */
    private record ZipContent(String fileName, byte[] content) {
    }
}
