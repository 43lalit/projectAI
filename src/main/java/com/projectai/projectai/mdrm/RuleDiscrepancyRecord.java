package com.projectai.projectai.mdrm;

/**
 * One actionable discrepancy row derived from rule lineage against a run context.
 */
public record RuleDiscrepancyRecord(
        long ruleId,
        Long dependencyId,
        String reportingForm,
        String scheduleName,
        String ruleType,
        String ruleNumber,
        String primaryMdrmCode,
        String secondaryMdrmCode,
        String secondaryTokenRaw,
        String lineageStatus,
        String message
) {
}
