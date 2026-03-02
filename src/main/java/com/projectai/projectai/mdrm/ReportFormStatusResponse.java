package com.projectai.projectai.mdrm;

public record ReportFormStatusResponse(
        String reportingForm,
        String status,
        long runId,
        int totalUniqueMdrms,
        int activeMdrms,
        long updatedAt
) {
}
