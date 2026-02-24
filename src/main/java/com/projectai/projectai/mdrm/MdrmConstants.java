package com.projectai.projectai.mdrm;

/**
 * Shared constants for MDRM ingestion flow.
 */
public final class MdrmConstants {

    private MdrmConstants() {
    }

    public static final String DEFAULT_LOCAL_FILE_PATH = "FedFiles/MDRM_CSV.csv";
    public static final String DEFAULT_STAGING_TABLE = "mdrm_staging";
    public static final String DEFAULT_CRON = "0 0 1 * * *";

    public static final String FILE_EXT_ZIP = ".zip";
    public static final String FILE_EXT_TXT = ".txt";
    public static final String FILE_EXT_CSV = ".csv";
    public static final String FILE_EXT_DAT = ".dat";

    public static final int CSV_BATCH_SIZE = 1000;
    public static final String CSV_DEFAULT_COLUMN_PREFIX = "col_";

    public static final String VALID_TABLE_NAME_REGEX = "[a-zA-Z_][a-zA-Z0-9_]*";

    public static final String SQL_DROP_TABLE_IF_EXISTS = "DROP TABLE IF EXISTS ";
    public static final String SQL_CREATE_TABLE_PREFIX = "CREATE TABLE ";
    public static final String SQL_INSERT_VALUES_PREFIX = "INSERT INTO ";
    public static final String SQL_INSERT_VALUES_INFIX = " VALUES (";
    public static final String SQL_CLOSE_PAREN = ")";
    public static final String SQL_SELECT_DISTINCT_REPORTING_FORM_PREFIX = "SELECT DISTINCT reporting_form FROM ";
    public static final String SQL_SELECT_DISTINCT_REPORTING_FORM_SUFFIX = " WHERE reporting_form IS NOT NULL AND reporting_form <> '' ORDER BY reporting_form";
    public static final String SQL_SELECT_ALL_BY_REPORTING_FORM_PREFIX = "SELECT * FROM ";
    public static final String SQL_SELECT_ALL_BY_REPORTING_FORM_SUFFIX = " WHERE reporting_form = ?";
    public static final String SQL_SELECT_SINGLE_ROW_SUFFIX = " LIMIT 1";

    public static final String MSG_RESOURCE_NOT_FOUND = "MDRM file not found in resources: %s";
    public static final String MSG_RESOURCE_EMPTY = "MDRM file is empty: %s";
    public static final String MSG_RESOURCE_READ_FAILED = "Failed to read MDRM file from resources: %s";
    public static final String MSG_NO_HEADERS_FOR_TABLE = "No CSV headers found for staging table creation";
    public static final String MSG_ZIP_READ_FAILED = "Unable to read MDRM ZIP content";
    public static final String MSG_ZIP_NO_TEXT_FILE = "No readable text file found inside MDRM ZIP";
    public static final String MSG_CSV_EMPTY = "CSV file is empty";
    public static final String MSG_CSV_HEADER_NOT_FOUND = "CSV header row not found on line 2";
    public static final String MSG_CSV_PARSE_LOAD_FAILED = "Unable to parse and load MDRM CSV content";
    public static final String MSG_INVALID_STAGING_TABLE = "Invalid staging table name: %s";
    public static final String MSG_REPORTING_FORM_COLUMN_MISSING = "Column reporting_form not found in staging table: %s";
    public static final String COLUMN_REPORTING_FORM = "reporting_form";
}
