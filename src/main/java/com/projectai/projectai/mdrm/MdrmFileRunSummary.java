package com.projectai.projectai.mdrm;

/**
 * File-level run metrics used on the load screen.
 */
public record MdrmFileRunSummary(
        long runId,
        long runDatetime,
        String fileName,
        int numFileRecords,
        int numRecordsIngested,
        int numRecordsError,
        int reportsCount,
        int totalUniqueMdrms,
        int activeMdrms,
        int inactiveMdrms,
        int updatedMdrms
) {
}
