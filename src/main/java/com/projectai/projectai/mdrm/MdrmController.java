package com.projectai.projectai.mdrm;

import com.projectai.projectai.auth.CsrfService;
import com.projectai.projectai.auth.AuthService;
import jakarta.servlet.http.HttpServletRequest;
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
    private final CsrfService csrfService;
    private final AuthService authService;
    private final ReportMetadataService reportMetadataService;

    public MdrmController(
            MdrmLoadService mdrmLoadService,
            CsrfService csrfService,
            AuthService authService,
            ReportMetadataService reportMetadataService
    ) {
        this.mdrmLoadService = mdrmLoadService;
        this.csrfService = csrfService;
        this.authService = authService;
        this.reportMetadataService = reportMetadataService;
    }

    /**
     * Triggers a fresh MDRM migration run: truncate all MDRM tables then process MDRM_mmyy files in sequence.
     */
    @PostMapping("/load")
    public ResponseEntity<MdrmLoadResult> loadMdrm(HttpServletRequest request) {
        csrfService.requireValidToken(request, request.getSession());
        authService.requireAdmin(request.getSession());
        return ResponseEntity.ok(mdrmLoadService.loadFreshMigration());
    }

    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<MdrmLoadResult> uploadMdrm(
            @RequestParam("file") MultipartFile file,
            HttpServletRequest request
    ) {
        csrfService.requireValidToken(request, request.getSession());
        authService.requireAdmin(request.getSession());
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
    public ResponseEntity<List<String>> getReportingForms(
            @RequestParam(required = false) Long runId
    ) {
        return ResponseEntity.ok(mdrmLoadService.getReportingForms(runId));
    }

    @GetMapping("/reporting-form-statuses")
    public ResponseEntity<List<ReportFormStatusResponse>> getReportingFormStatuses(
            @RequestParam(required = false) Long runId
    ) {
        return ResponseEntity.ok(mdrmLoadService.getReportingFormStatuses(runId));
    }

    /**
     * Returns all rows for the selected reporting form.
     */
    @GetMapping("/data")
    public ResponseEntity<MdrmTableResponse> getDataByReportingForm(
            @RequestParam String reportingForm,
            @RequestParam(required = false) Long runId
    ) {
        return ResponseEntity.ok(mdrmLoadService.getRowsByReportingForm(reportingForm, runId));
    }

    /**
     * Returns run history summary for a reporting form.
     */
    @GetMapping("/run-history")
    public ResponseEntity<MdrmRunHistoryResponse> getRunHistoryByReportingForm(
            @RequestParam String reportingForm,
            @RequestParam(required = false) Long runId
    ) {
        return ResponseEntity.ok(mdrmLoadService.getRunHistoryByReportingForm(reportingForm, runId));
    }

    @GetMapping("/report-metadata")
    public ResponseEntity<ReportMetadataResponse> getReportMetadata(@RequestParam String reportingForm) {
        return ResponseEntity.ok(reportMetadataService.getOrFetch(reportingForm));
    }

    @PostMapping("/report-metadata/refresh")
    public ResponseEntity<ReportMetadataResponse> refreshReportMetadata(
            @RequestParam String reportingForm,
            HttpServletRequest request
    ) {
        csrfService.requireValidToken(request, request.getSession());
        authService.requireAdmin(request.getSession());
        return ResponseEntity.ok(reportMetadataService.refresh(reportingForm));
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
    public ResponseEntity<MdrmSemanticSearchResponse> semanticSearch(
            @RequestParam("q") String query,
            @RequestParam(required = false) Long runId
    ) {
        return ResponseEntity.ok(mdrmLoadService.semanticSearch(query, runId));
    }

    @GetMapping("/profile")
    public ResponseEntity<MdrmProfileResponse> getMdrmProfile(
            @RequestParam("mdrm") String mdrmCode,
            @RequestParam(required = false) Long runId
    ) {
        return ResponseEntity.ok(mdrmLoadService.getMdrmProfile(mdrmCode, runId));
    }

    @GetMapping("/run-incremental-mdrms")
    public ResponseEntity<MdrmIncrementalCodeListResponse> getIncrementalCodesForRun(
            @RequestParam String reportingForm,
            @RequestParam long runId,
            @RequestParam String changeType) {
        return ResponseEntity.ok(mdrmLoadService.getIncrementalCodesForRun(reportingForm, runId, changeType));
    }
}
