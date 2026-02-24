package com.projectai.projectai.mdrm;

/**
 * API/service response for a completed load operation.
 */
public record MdrmLoadResult(String sourceFileName, int loadedRows) {
}
