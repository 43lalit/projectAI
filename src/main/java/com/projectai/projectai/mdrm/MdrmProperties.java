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
    private String rulesFilePath = MdrmConstants.DEFAULT_RULES_FILE_PATH;
    private String rulesSourceSystem = MdrmConstants.DEFAULT_RULES_SOURCE_SYSTEM;
    private String rulesDefaultCategory = MdrmConstants.DEFAULT_RULES_CATEGORY;
    private String rulesExpressionRegex = MdrmConstants.DEFAULT_RULES_EXPRESSION_REGEX;
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

    public String getRulesFilePath() {
        return rulesFilePath;
    }

    public void setRulesFilePath(String rulesFilePath) {
        this.rulesFilePath = rulesFilePath;
    }

    public String getRulesSourceSystem() {
        return rulesSourceSystem;
    }

    public void setRulesSourceSystem(String rulesSourceSystem) {
        this.rulesSourceSystem = rulesSourceSystem;
    }

    public String getRulesDefaultCategory() {
        return rulesDefaultCategory;
    }

    public void setRulesDefaultCategory(String rulesDefaultCategory) {
        this.rulesDefaultCategory = rulesDefaultCategory;
    }

    public String getRulesExpressionRegex() {
        return rulesExpressionRegex;
    }

    public void setRulesExpressionRegex(String rulesExpressionRegex) {
        this.rulesExpressionRegex = rulesExpressionRegex;
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
