package com.projectai.projectai.mdrm;

import java.util.List;
import java.util.Map;

/**
 * Tabular response payload for MDRM UI queries.
 */
public record MdrmTableResponse(List<String> columns, List<Map<String, String>> rows) {
}
