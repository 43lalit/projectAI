package com.projectai.projectai.mdrm;

import java.util.List;

/**
 * Reporting-form run history with MDRM category counts.
 */
public record MdrmRunHistoryResponse(String reportingForm, List<MdrmRunSummary> runs) {
}
