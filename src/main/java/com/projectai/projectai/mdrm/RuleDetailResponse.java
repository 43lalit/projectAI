package com.projectai.projectai.mdrm;

import java.util.List;

/**
 * Full detail payload for one rule.
 */
public record RuleDetailResponse(
        Long runId,
        String asOfDate,
        RuleRecord rule,
        List<RuleDependencyRecord> dependencies
) {
}
