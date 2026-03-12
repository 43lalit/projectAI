package com.projectai.projectai.mdrm;

import com.projectai.projectai.auth.AuthService;
import com.projectai.projectai.auth.CsrfService;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * API surface for rules ingestion and rule lineage queries.
 */
@RestController
@RequestMapping("/api/mdrm/rules")
public class RulesController {

    private final RulesService rulesService;
    private final CsrfService csrfService;
    private final AuthService authService;

    public RulesController(RulesService rulesService, CsrfService csrfService, AuthService authService) {
        this.rulesService = rulesService;
        this.csrfService = csrfService;
        this.authService = authService;
    }

    @PostMapping("/load")
    public ResponseEntity<RuleLoadResult> loadRulesWorkbook(HttpServletRequest request) {
        csrfService.requireValidToken(request, request.getSession());
        authService.requireAdmin(request.getSession());
        return ResponseEntity.ok(rulesService.loadConfiguredRulesWorkbook());
    }

    @GetMapping("/load-history")
    public ResponseEntity<List<RuleLoadHistoryEntry>> getLoadHistory() {
        return ResponseEntity.ok(rulesService.getLoadHistory());
    }

    @GetMapping("/search")
    public ResponseEntity<RuleSearchResponse> searchRules(
            @RequestParam("q") String query,
            @RequestParam(required = false) Long runId,
            @RequestParam(required = false) String reportingForm,
            @RequestParam(required = false) String ruleCategory,
            @RequestParam(required = false) String schedule,
            @RequestParam(required = false) String ruleType
    ) {
        return ResponseEntity.ok(rulesService.searchRules(query, runId, reportingForm, ruleCategory, schedule, ruleType));
    }

    @GetMapping("/by-mdrm")
    public ResponseEntity<RuleListResponse> getRulesByMdrm(
            @RequestParam("mdrm") String mdrmCode,
            @RequestParam(required = false) Long runId
    ) {
        return ResponseEntity.ok(rulesService.getRulesByMdrm(mdrmCode, runId));
    }

    @GetMapping("/by-report")
    public ResponseEntity<RuleListResponse> getRulesByReport(
            @RequestParam("reportingForm") String reportingForm,
            @RequestParam(required = false) String schedule,
            @RequestParam(required = false) Long runId
    ) {
        return ResponseEntity.ok(rulesService.getRulesByReport(reportingForm, schedule, runId));
    }

    @GetMapping("/by-scope")
    public ResponseEntity<RuleListResponse> getRulesByScope(
            @RequestParam(required = false, name = "reportingForm") List<String> reportingForms,
            @RequestParam(required = false, name = "mdrm") List<String> mdrmCodes,
            @RequestParam(required = false) Long runId
    ) {
        return ResponseEntity.ok(rulesService.getRulesByScope(reportingForms, mdrmCodes, runId));
    }

    @GetMapping("/{ruleId}")
    public ResponseEntity<RuleDetailResponse> getRuleDetail(
            @PathVariable long ruleId,
            @RequestParam(required = false) Long runId
    ) {
        try {
            return ResponseEntity.ok(rulesService.getRuleDetail(ruleId, runId));
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.notFound().build();
        }
    }

    @GetMapping("/graph")
    public ResponseEntity<RuleGraphResponse> getRuleGraph(
            @RequestParam(required = false, name = "mdrm") String mdrmCode,
            @RequestParam(required = false) String reportingForm,
            @RequestParam(required = false) Long runId,
            @RequestParam(defaultValue = "1") int depth
    ) {
        return ResponseEntity.ok(rulesService.getRuleGraph(mdrmCode, reportingForm, runId, depth));
    }

    @GetMapping("/discrepancies")
    public ResponseEntity<RuleDiscrepancyResponse> getDiscrepancies(
            @RequestParam(required = false) Long runId,
            @RequestParam(required = false) String reportingForm,
            @RequestParam(required = false) String status
    ) {
        return ResponseEntity.ok(rulesService.getRuleDiscrepancies(runId, reportingForm, status));
    }
}
