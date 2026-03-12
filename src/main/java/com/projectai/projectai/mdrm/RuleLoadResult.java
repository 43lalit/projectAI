package com.projectai.projectai.mdrm;

/**
 * Summary returned after loading one rules workbook.
 */
public record RuleLoadResult(
        long loadId,
        String sourceFileName,
        String reportSeries,
        String loadStatus,
        int loadedRules,
        int loadedDependencies,
        int warningCount,
        boolean skippedDuplicate,
        String message
) {
}
