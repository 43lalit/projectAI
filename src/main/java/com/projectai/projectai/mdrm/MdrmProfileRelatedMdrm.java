package com.projectai.projectai.mdrm;

/**
 * Related MDRM that shares the same item_code family.
 */
public record MdrmProfileRelatedMdrm(
        String mdrmCode,
        String reportingForm,
        String status,
        String itemType,
        String label,
        Long runId,
        int reportCount
) {
}
