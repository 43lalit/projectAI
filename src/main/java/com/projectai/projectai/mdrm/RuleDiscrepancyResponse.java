package com.projectai.projectai.mdrm;

import java.util.List;

/**
 * Discrepancy list for a selected run/report context.
 */
public record RuleDiscrepancyResponse(
        Long runId,
        String asOfDate,
        int totalResults,
        List<RuleDiscrepancyRecord> discrepancies
) {
}
