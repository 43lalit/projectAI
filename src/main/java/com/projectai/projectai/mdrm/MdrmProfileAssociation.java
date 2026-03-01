package com.projectai.projectai.mdrm;

/**
 * Latest-run report/schedule/line association for an MDRM.
 */
public record MdrmProfileAssociation(
        String reportingForm,
        String schedule,
        String line,
        String label
) {
}
