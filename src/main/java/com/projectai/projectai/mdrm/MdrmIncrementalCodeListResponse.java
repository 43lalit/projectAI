package com.projectai.projectai.mdrm;

import java.util.List;

/**
 * MDRM code list for incremental buckets (ADDED/MODIFIED/DELETED).
 */
public record MdrmIncrementalCodeListResponse(
        String reportingForm,
        long runId,
        String changeType,
        List<String> mdrmCodes
) {
}
