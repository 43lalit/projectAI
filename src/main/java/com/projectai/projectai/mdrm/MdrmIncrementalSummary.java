package com.projectai.projectai.mdrm;

/**
 * Report-level incremental changes for one run.
 */
public record MdrmIncrementalSummary(
        String reportingForm,
        long runId,
        int addedMdrms,
        int modifiedMdrms,
        int deletedMdrms
) {
}
