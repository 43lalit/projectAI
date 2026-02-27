package com.projectai.projectai.mdrm;

import java.util.List;

/**
 * API/service response for a completed load operation.
 */
public record MdrmLoadResult(String sourceFileName, int loadedRows, int runsProcessed, List<String> processedFiles) {

    public MdrmLoadResult {
        processedFiles = processedFiles == null ? List.of() : List.copyOf(processedFiles);
    }

    public MdrmLoadResult(String sourceFileName, int loadedRows) {
        this(
                sourceFileName,
                loadedRows,
                sourceFileName == null ? 0 : 1,
                sourceFileName == null ? List.of() : List.of(sourceFileName)
        );
    }
}
