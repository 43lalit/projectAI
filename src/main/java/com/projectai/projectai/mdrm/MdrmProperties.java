package com.projectai.projectai.mdrm;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Externalized MDRM ingestion configuration bound from {@code application.properties}.
 */
@ConfigurationProperties(prefix = "mdrm")
public class MdrmProperties {

    private String localFilePath = MdrmConstants.DEFAULT_LOCAL_FILE_PATH;
    private String migrationFilePattern = MdrmConstants.DEFAULT_MIGRATION_FILE_PATTERN;
    private String stagingTable = MdrmConstants.DEFAULT_STAGING_TABLE;
    private String cron = MdrmConstants.DEFAULT_CRON;

    public String getLocalFilePath() {
        return localFilePath;
    }

    public void setLocalFilePath(String localFilePath) {
        this.localFilePath = localFilePath;
    }

    public String getStagingTable() {
        return stagingTable;
    }

    public String getMigrationFilePattern() {
        return migrationFilePattern;
    }

    public void setMigrationFilePattern(String migrationFilePattern) {
        this.migrationFilePattern = migrationFilePattern;
    }

    public void setStagingTable(String stagingTable) {
        this.stagingTable = stagingTable;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }
}
