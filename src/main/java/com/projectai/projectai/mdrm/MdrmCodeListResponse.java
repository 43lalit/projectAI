package com.projectai.projectai.mdrm;

import java.util.List;

/**
 * MDRM code list response for one run and one category bucket.
 */
public record MdrmCodeListResponse(String reportingForm, long runId, String bucket, List<String> mdrmCodes) {
}
