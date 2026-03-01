package com.projectai.projectai.mdrm;

/**
 * Related reporting form that shares the same item_code family.
 */
public record MdrmProfileRelatedReport(
        String reportingForm,
        int mdrmCount,
        boolean includesSelectedMdrm
) {
}
