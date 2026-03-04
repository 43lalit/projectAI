package com.projectai.projectai.mdrm;

public final class MdrmManageModels {

    private MdrmManageModels() {
    }

    public record AddRequest(
            Long runId,
            String reportingForm,
            String mdrmCode,
            String description,
            String itemType,
            String mnemonic,
            String itemCode,
            Boolean active
    ) {
    }

    public record EditRequest(
            Long runId,
            String reportingForm,
            String mdrmCode,
            String newMdrmCode,
            String description,
            String itemType,
            Boolean active
    ) {
    }

    public record DeleteRequest(
            Long runId,
            String reportingForm,
            String mdrmCode
    ) {
    }

    public record MutationResponse(
            String operation,
            long runId,
            String reportingForm,
            String mdrmCode,
            int affectedRows,
            String message
    ) {
    }
}
