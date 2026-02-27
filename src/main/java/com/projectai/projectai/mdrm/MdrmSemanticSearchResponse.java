package com.projectai.projectai.mdrm;

import java.util.List;

/**
 * Response payload for MDRM semantic search.
 */
public record MdrmSemanticSearchResponse(
        String query,
        String interpretedReportingForm,
        List<String> interpretedKeywords,
        int totalResults,
        MdrmTableResponse results
) {
}
