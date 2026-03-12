package com.projectai.projectai.mdrm;

/**
 * Dependency row extracted from a rule expression.
 */
public record RuleDependencyRecord(
        long dependencyId,
        String primaryMdrmCode,
        String secondaryTokenRaw,
        String secondaryMdrmCode,
        String dependencyKind,
        String qualifierDetail,
        String parseConfidence,
        boolean selfReference,
        String dependencyStatus,
        String secondaryMdrmStatus
) {
}
