package com.projectai.projectai.mdrm;

public record ReportMetadataResponse(
        String reportSeries,
        String fullName,
        String description,
        String sourceUrl,
        Long lastFetchedAt
) {
}
