package com.projectai.projectai.mdrm;

/**
 * Field-level change between adjacent timeline runs.
 */
public record MdrmProfileTimelineFieldChange(
        String fieldName,
        String previousValue,
        String currentValue
) {
}
