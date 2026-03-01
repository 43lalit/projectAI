package com.projectai.projectai.mdrm;

import java.util.List;

/**
 * One run-level timeline event for an MDRM.
 */
public record MdrmProfileTimelineEntry(
        long runId,
        Long runDatetime,
        String fileName,
        String reportingForms,
        int rowCount,
        int reportCount,
        int changedFieldCount,
        String changedFields,
        List<MdrmProfileTimelineFieldChange> fieldChanges,
        String status,
        String minStartDateUtc,
        String maxEndDateUtc
) {
}
