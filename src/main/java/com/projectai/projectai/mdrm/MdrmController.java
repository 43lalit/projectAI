package com.projectai.projectai.mdrm;

import org.springframework.http.ResponseEntity;
import org.springframework.http.MediaType;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.http.HttpStatus;

import java.util.List;

/**
 * Exposes manual MDRM load endpoint for on-demand ingestion.
 */
@RestController
@RequestMapping("/api/mdrm")
public class MdrmController {

    private final MdrmLoadService mdrmLoadService;

    public MdrmController(MdrmLoadService mdrmLoadService) {
        this.mdrmLoadService = mdrmLoadService;
    }

    /**
     * Triggers a fresh MDRM migration run: truncate all MDRM tables then process MDRM_mmyy files in sequence.
     */
    @PostMapping("/load")
    public ResponseEntity<MdrmLoadResult> loadMdrm() {
        return ResponseEntity.ok(mdrmLoadService.loadFreshMigration());
    }

    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<MdrmLoadResult> uploadMdrm(@RequestParam("file") MultipartFile file) {
        try {
            return ResponseEntity.ok(mdrmLoadService.loadUploadedMdrm(file.getOriginalFilename(), file.getBytes()));
        } catch (IllegalArgumentException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, ex.getMessage(), ex);
        } catch (Exception ex) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to upload MDRM file", ex);
        }
    }

    /**
     * Returns all available reporting forms from the staging table.
     */
    @GetMapping("/reporting-forms")
    public ResponseEntity<List<String>> getReportingForms() {
        return ResponseEntity.ok(mdrmLoadService.getReportingForms());
    }

    /**
     * Returns all rows for the selected reporting form.
     */
    @GetMapping("/data")
    public ResponseEntity<MdrmTableResponse> getDataByReportingForm(@RequestParam String reportingForm) {
        return ResponseEntity.ok(mdrmLoadService.getRowsByReportingForm(reportingForm));
    }

    /**
     * Returns run history summary for a reporting form.
     */
    @GetMapping("/run-history")
    public ResponseEntity<MdrmRunHistoryResponse> getRunHistoryByReportingForm(@RequestParam String reportingForm) {
        return ResponseEntity.ok(mdrmLoadService.getRunHistoryByReportingForm(reportingForm));
    }

    @GetMapping("/file-runs")
    public ResponseEntity<List<MdrmFileRunSummary>> getFileRuns() {
        return ResponseEntity.ok(mdrmLoadService.getFileRunHistory());
    }

    @GetMapping("/incremental-summary")
    public ResponseEntity<List<MdrmIncrementalSummary>> getIncrementalSummaryByRun(@RequestParam long runId) {
        return ResponseEntity.ok(mdrmLoadService.getIncrementalSummaryForRun(runId));
    }

    /**
     * Returns MDRM code list for one run and one status bucket (TOTAL/ACTIVE/INACTIVE/UPDATED).
     */
    @GetMapping("/run-mdrms")
    public ResponseEntity<MdrmCodeListResponse> getMdrmCodesForRunBucket(
            @RequestParam String reportingForm,
            @RequestParam long runId,
            @RequestParam String bucket) {
        return ResponseEntity.ok(mdrmLoadService.getMdrmCodesForRunBucket(reportingForm, runId, bucket));
    }

    @GetMapping("/semantic-search")
    public ResponseEntity<MdrmSemanticSearchResponse> semanticSearch(@RequestParam("q") String query) {
        return ResponseEntity.ok(mdrmLoadService.semanticSearch(query));
    }

    @GetMapping("/run-incremental-mdrms")
    public ResponseEntity<MdrmIncrementalCodeListResponse> getIncrementalCodesForRun(
            @RequestParam String reportingForm,
            @RequestParam long runId,
            @RequestParam String changeType) {
        return ResponseEntity.ok(mdrmLoadService.getIncrementalCodesForRun(reportingForm, runId, changeType));
    }
}
