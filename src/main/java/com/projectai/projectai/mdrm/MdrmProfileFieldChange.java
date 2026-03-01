package com.projectai.projectai.mdrm;

/**
 * Field-level delta between first and latest appearance of an MDRM.
 */
public record MdrmProfileFieldChange(
        String fieldName,
        String firstValue,
        String latestValue
) {
}
