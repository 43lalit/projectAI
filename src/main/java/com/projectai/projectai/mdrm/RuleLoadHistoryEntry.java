package com.projectai.projectai.mdrm;

/**
 * Historical load entry for imported rule datasets.
 */
public record RuleLoadHistoryEntry(
        long loadId,
        String sourceSystem,
        String reportSeries,
        String sourceFileName,
        long loadedAt,
        String loadStatus,
        int totalRuleRows,
        int totalDependencyRows,
        int warningCount,
        int errorCount
) {
}
