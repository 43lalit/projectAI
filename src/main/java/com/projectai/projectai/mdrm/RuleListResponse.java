package com.projectai.projectai.mdrm;

import java.util.List;

/**
 * Shared list payload for report- and MDRM-scoped rule views.
 */
public record RuleListResponse(
        String scopeType,
        String scopeValue,
        Long runId,
        String asOfDate,
        int totalRules,
        int totalDependencies,
        int discrepancyCount,
        List<RuleRecord> rules
) {
}
