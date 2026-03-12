package com.projectai.projectai.mdrm;

import java.util.List;

/**
 * Graph payload used by ontology rules mode and MDRM lineage rendering.
 */
public record RuleGraphResponse(
        String mode,
        Long runId,
        String asOfDate,
        int reportCount,
        int mdrmCount,
        int ruleCount,
        int discrepancyCount,
        List<Node> nodes,
        List<Edge> edges
) {
    public record Node(
            String id,
            String label,
            String category,
            String status,
            String metadata
    ) {
    }

    public record Edge(
            String source,
            String target,
            String relation,
            String status
    ) {
    }
}
