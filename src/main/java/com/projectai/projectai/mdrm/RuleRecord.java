package com.projectai.projectai.mdrm;

/**
 * Projected rule row used by search and detail surfaces.
 */
public record RuleRecord(
        long ruleId,
        String ruleCategory,
        String reportSeries,
        String reportingForm,
        String scheduleName,
        String ruleType,
        String ruleNumber,
        String primaryMdrmCode,
        String targetItemLabel,
        String effectiveStartDate,
        String effectiveEndDate,
        String changeStatus,
        String ruleText,
        String ruleExpression,
        int dependencyCount,
        int discrepancyCount,
        String lineageStatus
) {
}
