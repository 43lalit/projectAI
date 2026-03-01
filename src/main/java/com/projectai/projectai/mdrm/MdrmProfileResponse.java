package com.projectai.projectai.mdrm;

import java.util.List;

/**
 * Aggregated MDRM profile payload used by the MDRM detail page.
 */
public record MdrmProfileResponse(
        String mdrmCode,
        String itemCode,
        String mnemonic,
        String itemType,
        String description,
        String definition,
        String latestStatus,
        Long latestRunId,
        Long latestRunDatetime,
        String latestFileName,
        int timelineCount,
        int associationCount,
        int relatedMdrmCount,
        int relatedReportCount,
        List<MdrmProfileFieldChange> fieldChanges,
        List<MdrmProfileTimelineEntry> timeline,
        List<MdrmProfileAssociation> associations,
        List<MdrmProfileRelatedMdrm> relatedMdrms,
        List<MdrmProfileRelatedReport> relatedReports
) {
}
