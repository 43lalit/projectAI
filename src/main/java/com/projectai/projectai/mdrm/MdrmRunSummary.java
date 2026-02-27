package com.projectai.projectai.mdrm;

/**
 * Run-level summary counts for a reporting form.
 */
public record MdrmRunSummary(
        long runId,
        long runDatetime,
        String fileName,
        int totalUniqueMdrms,
        int activeMdrms,
        int inactiveMdrms,
        int updatedMdrms
) {
}
