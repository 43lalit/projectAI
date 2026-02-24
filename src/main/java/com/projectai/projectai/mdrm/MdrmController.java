package com.projectai.projectai.mdrm;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
     * Triggers full MDRM ingest and returns source file name with inserted row count.
     */
    @PostMapping("/load")
    public ResponseEntity<MdrmLoadResult> loadMdrm() {
        return ResponseEntity.ok(mdrmLoadService.loadLatestMdrm());
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
}
