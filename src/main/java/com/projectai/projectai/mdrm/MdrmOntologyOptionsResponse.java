package com.projectai.projectai.mdrm;

import java.util.List;

/**
 * Selector options for ontology filtering workflow.
 */
public record MdrmOntologyOptionsResponse(
        Long runId,
        List<String> reports,
        List<String> mdrms
) {
}
