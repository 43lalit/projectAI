package com.projectai.projectai.mdrm;

import java.util.List;

/**
 * Ontology graph payload for Report -> MDRM -> MDRM Type relationships.
 */
public record MdrmOntologyGraphResponse(
        Long runId,
        int reportCount,
        int mdrmCount,
        int mdrmTypeCount,
        int activeMdrmCount,
        int inactiveMdrmCount,
        List<TypeCount> typeBreakdown,
        List<StatusTypeCount> statusTypeBreakdown,
        List<Node> nodes,
        List<Edge> edges
) {
    public record TypeCount(
            String type,
            int count
    ) {
    }

    public record StatusTypeCount(
            String status,
            String type,
            int count
    ) {
    }

    public record Node(
            String id,
            String label,
            String category,
            String status
    ) {
    }

    public record Edge(
            String source,
            String target,
            String relation
    ) {
    }
}
