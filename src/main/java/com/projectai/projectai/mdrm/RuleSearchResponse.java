package com.projectai.projectai.mdrm;

import java.util.List;

/**
 * Search response for rule discovery.
 */
public record RuleSearchResponse(
        String query,
        Long runId,
        String asOfDate,
        int totalResults,
        List<RuleRecord> rules
) {
}
