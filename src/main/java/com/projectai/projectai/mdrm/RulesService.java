package com.projectai.projectai.mdrm;

import jakarta.annotation.PostConstruct;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Handles generic rules ingestion, dependency extraction, and rule lineage queries.
 */
@Service
@DependsOn("mdrmLoadService")
public class RulesService {

    private static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final Set<String> RULE_SHEET_NAMES = Set.of("all_edits");
    private static final Set<String> DEPENDENCY_SHEET_NAMES = Set.of("edit_dependencies", "mdrm_graph_edges");
    private static final Pattern MDRM_CODE_PATTERN = Pattern.compile("^([A-Z]{4}[A-Z0-9]{4})(?:-Q([1-8]))?$");

    private final JdbcTemplate jdbcTemplate;
    private final MdrmProperties properties;

    public RulesService(JdbcTemplate jdbcTemplate, MdrmProperties properties) {
        this.jdbcTemplate = jdbcTemplate;
        this.properties = properties;
    }

    @PostConstruct
    public void initializeSchema() {
        ensureTables();
        ensureIndexes();
        ensureSqlFunctions();
    }

    @Transactional
    public RuleLoadResult loadConfiguredRulesWorkbook() {
        String path = properties.getRulesFilePath();
        ClassPathResource resource = new ClassPathResource(path);
        if (!resource.exists()) {
            throw new IllegalStateException("Rules workbook not found in resources: " + path);
        }

        byte[] content;
        try (InputStream inputStream = resource.getInputStream()) {
            content = inputStream.readAllBytes();
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to read rules workbook from resources: " + path, ex);
        }
        if (content.length == 0) {
            throw new IllegalStateException("Rules workbook is empty: " + path);
        }

        String checksum = sha256Base64(content);
        RuleLoadHistoryEntry existing = findCompletedLoadByChecksum(checksum);
        if (existing != null) {
            return new RuleLoadResult(
                    existing.loadId(),
                    existing.sourceFileName(),
                    existing.reportSeries(),
                    existing.loadStatus(),
                    existing.totalRuleRows(),
                    existing.totalDependencyRows(),
                    existing.warningCount(),
                    true,
                    "Rules workbook already loaded"
            );
        }

        long loadId = createLoadRow(resource.getFilename(), checksum);
        try {
            ParsedWorkbook parsedWorkbook = parseWorkbook(content);
            batchInsertRules(loadId, parsedWorkbook.rules());
            Map<String, Long> ruleIdsByRowHash = loadRuleIdsByHash(loadId);
            batchInsertDependencies(ruleIdsByRowHash, parsedWorkbook.dependencies());
            batchInsertWarnings(loadId, ruleIdsByRowHash, parsedWorkbook.warnings());
            markLoadCompleted(loadId, parsedWorkbook.rules().size(), parsedWorkbook.dependencies().size(), parsedWorkbook.warnings().size());

            return new RuleLoadResult(
                    loadId,
                    resource.getFilename(),
                    parsedWorkbook.reportSeries(),
                    "COMPLETED",
                    parsedWorkbook.rules().size(),
                    parsedWorkbook.dependencies().size(),
                    parsedWorkbook.warnings().size(),
                    false,
                    "Rules workbook loaded successfully"
            );
        } catch (RuntimeException ex) {
            markLoadFailed(loadId, ex.getMessage());
            throw ex;
        }
    }

    @Transactional
    public RuleLoadResult reprocessConfiguredRulesWorkbook() {
        clearRulesData();
        ensureTables();
        ensureIndexes();
        ensureSqlFunctions();
        return loadConfiguredRulesWorkbook();
    }

    public void reloadSqlFunctions() {
        ensureSqlFunctions();
    }

    public List<RuleLoadHistoryEntry> getLoadHistory() {
        String sql = """
                SELECT
                    load_id,
                    source_system,
                    report_series,
                    source_file_name,
                    loaded_at,
                    load_status,
                    total_rule_rows,
                    total_dependency_rows,
                    warning_count,
                    error_count
                FROM __RULE_LOADS__
                ORDER BY load_id DESC
                """
                .replace("__RULE_LOADS__", MdrmConstants.DEFAULT_RULE_LOADS_TABLE);
        return jdbcTemplate.query(
                sql,
                (rs, rowNum) -> new RuleLoadHistoryEntry(
                        rs.getLong("load_id"),
                        rs.getString("source_system"),
                        rs.getString("report_series"),
                        rs.getString("source_file_name"),
                        rs.getLong("loaded_at"),
                        rs.getString("load_status"),
                        rs.getInt("total_rule_rows"),
                        rs.getInt("total_dependency_rows"),
                        rs.getInt("warning_count"),
                        rs.getInt("error_count")
                )
        );
    }

    public RuleSearchResponse searchRules(
            String query,
            Long runId,
            String reportingForm,
            String ruleCategory,
            String scheduleName,
            String ruleType
    ) {
        String rawQuery = query == null ? "" : query.trim();
        if (rawQuery.isEmpty()) {
            return new RuleSearchResponse(rawQuery, normalizeRunId(runId), resolveAsOfDate(runId), 0, List.of());
        }

        String sql = "SELECT * FROM " + MdrmConstants.DEFAULT_RULE_SEARCH_FUNCTION + "(?, ?, ?, ?, ?, ?)";
        List<RuleRecord> rules = jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ps.setString(1, rawQuery);
                    setNullableLong(ps, 2, normalizeRunId(runId));
                    ps.setString(3, blankToNull(reportingForm));
                    ps.setString(4, blankToNull(ruleCategory));
                    ps.setString(5, blankToNull(scheduleName));
                    ps.setString(6, blankToNull(ruleType));
                    return ps;
                },
                (rs, rowNum) -> mapRuleRecord(rs)
        );

        return new RuleSearchResponse(rawQuery, normalizeRunId(runId), resolveAsOfDate(runId), rules.size(), rules);
    }

    public RuleListResponse getRulesByMdrm(String mdrmCode, Long runId) {
        String normalizedMdrm = normalizeMdrmCode(mdrmCode);
        if (normalizedMdrm == null) {
            Long normalizedRunId = normalizeRunId(runId);
            return new RuleListResponse("MDRM", blankToNull(mdrmCode), normalizedRunId, resolveAsOfDate(normalizedRunId), 0, 0, 0, List.of());
        }
        Long normalizedRunId = normalizeRunId(runId);
        List<RuleRecord> rules = queryScopedRules(
                normalizedRunId,
                null,
                null,
                """
                normalize_mdrm_text(r.primary_mdrm_code) = normalize_mdrm_text(?)
                OR EXISTS (
                    SELECT 1
                    FROM __RULE_DEPS__ rd
                    WHERE rd.rule_id = r.rule_id
                      AND normalize_mdrm_text(rd.secondary_mdrm_code) = normalize_mdrm_text(?)
                )
                """.replace("__RULE_DEPS__", MdrmConstants.DEFAULT_RULE_DEPENDENCIES_TABLE),
                new SqlBinder() {
                    @Override
                    public void bind(PreparedStatement ps, int startIndex) throws SQLException {
                        ps.setString(startIndex, normalizedMdrm);
                        ps.setString(startIndex + 1, normalizedMdrm);
                    }

                    @Override
                    public int parameterCount() {
                        return 2;
                    }
                }
        );
        int totalDependencies = countDependenciesForRules(rules);
        int discrepancyCount = (int) rules.stream().filter(rule -> !"VALID".equalsIgnoreCase(rule.lineageStatus())).count();
        return new RuleListResponse("MDRM", normalizedMdrm, normalizedRunId, resolveAsOfDate(normalizedRunId), rules.size(), totalDependencies, discrepancyCount, rules);
    }

    public RuleListResponse getRulesByReport(String reportingForm, String scheduleName, Long runId) {
        String normalizedReport = blankToNull(reportingForm);
        if (normalizedReport == null) {
            Long normalizedRunId = normalizeRunId(runId);
            return new RuleListResponse("REPORT", null, normalizedRunId, resolveAsOfDate(normalizedRunId), 0, 0, 0, List.of());
        }
        Long normalizedRunId = normalizeRunId(runId);
        List<RuleRecord> rules = queryScopedRules(
                normalizedRunId,
                normalizedReport,
                blankToNull(scheduleName),
                null,
                null
        );

        int totalDependencies = countDependenciesForRules(rules);
        int discrepancyCount = (int) rules.stream().filter(rule -> !"VALID".equalsIgnoreCase(rule.lineageStatus())).count();
        return new RuleListResponse("REPORT", normalizedReport, normalizedRunId, resolveAsOfDate(normalizedRunId), rules.size(), totalDependencies, discrepancyCount, rules);
    }

    public RuleListResponse getRulesByScope(List<String> reportingForms, List<String> mdrmCodes, Long runId) {
        Long normalizedRunId = normalizeRunId(runId);
        List<String> normalizedReportKeys = reportingForms == null ? List.of() : reportingForms.stream()
                .map(this::normalizeReportKey)
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        List<String> normalizedMdrms = mdrmCodes == null ? List.of() : mdrmCodes.stream()
                .map(this::normalizeMdrmCode)
                .filter(Objects::nonNull)
                .distinct()
                .toList();

        if (normalizedReportKeys.isEmpty() && normalizedMdrms.isEmpty()) {
            return new RuleListResponse("SCOPE", null, normalizedRunId, resolveAsOfDate(normalizedRunId), 0, 0, 0, List.of());
        }

        List<RuleRecord> rules = queryRulesByScope(normalizedRunId, normalizedReportKeys, normalizedMdrms);
        int totalDependencies = countDependenciesForRules(rules);
        int discrepancyCount = (int) rules.stream().filter(rule -> !"VALID".equalsIgnoreCase(rule.lineageStatus())).count();
        String scopeValue = !normalizedMdrms.isEmpty()
                ? String.join(", ", normalizedMdrms)
                : String.join(", ", reportingForms == null ? List.of() : reportingForms);
        return new RuleListResponse("SCOPE", blankToNull(scopeValue), normalizedRunId, resolveAsOfDate(normalizedRunId), rules.size(), totalDependencies, discrepancyCount, rules);
    }

    public RuleDetailResponse getRuleDetail(long ruleId, Long runId) {
        RuleRecord rule = loadRuleRecord(ruleId, runId);
        if (rule == null) {
            throw new IllegalArgumentException("Rule not found: " + ruleId);
        }
        List<RuleDependencyRecord> dependencies = loadRuleDependencies(ruleId, runId);
        return new RuleDetailResponse(normalizeRunId(runId), resolveAsOfDate(runId), rule, dependencies);
    }

    public RuleGraphResponse getRuleGraph(String mdrmCode, String reportingForm, Long runId, int depth) {
        String normalizedMdrm = normalizeMdrmCode(mdrmCode);
        String normalizedReport = blankToNull(reportingForm);
        if (normalizedMdrm == null && normalizedReport == null) {
            return new RuleGraphResponse("RULES", normalizeRunId(runId), resolveAsOfDate(runId), 0, 0, 0, 0, List.of(), List.of());
        }

        String sql = "SELECT * FROM " + MdrmConstants.DEFAULT_RULE_GRAPH_FUNCTION + "(?, ?, ?, ?)";
        List<RuleGraphResponse.Node> nodes = new ArrayList<>();
        List<RuleGraphResponse.Edge> edges = new ArrayList<>();
        Set<String> nodeKeys = new LinkedHashSet<>();
        Set<String> edgeKeys = new LinkedHashSet<>();
        int safeDepth = Math.max(1, Math.min(depth <= 0 ? 1 : depth, 5));

        jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ps.setString(1, normalizedMdrm);
                    ps.setString(2, normalizedReport);
                    setNullableLong(ps, 3, normalizeRunId(runId));
                    ps.setInt(4, safeDepth);
                    return ps;
                },
                rs -> {
                    while (rs.next()) {
                        String sourceId = rs.getString("source_id");
                        String sourceLabel = rs.getString("source_label");
                        String sourceCategory = rs.getString("source_category");
                        String sourceStatus = rs.getString("source_status");
                        String targetId = rs.getString("target_id");
                        String targetLabel = rs.getString("target_label");
                        String targetCategory = rs.getString("target_category");
                        String targetStatus = rs.getString("target_status");
                        String relation = rs.getString("relation");
                        String edgeStatus = rs.getString("edge_status");

                        if (nodeKeys.add(sourceId)) {
                            nodes.add(new RuleGraphResponse.Node(sourceId, sourceLabel, sourceCategory, sourceStatus, null));
                        }
                        if (nodeKeys.add(targetId)) {
                            nodes.add(new RuleGraphResponse.Node(targetId, targetLabel, targetCategory, targetStatus, null));
                        }
                        String edgeKey = sourceId + "->" + targetId + "|" + relation;
                        if (edgeKeys.add(edgeKey)) {
                            edges.add(new RuleGraphResponse.Edge(sourceId, targetId, relation, edgeStatus));
                        }
                    }
                    return null;
                }
        );

        int ruleCount = (int) nodes.stream().filter(node -> "RULE".equalsIgnoreCase(node.category())).count();
        int reportCount = (int) nodes.stream().filter(node -> "REPORT".equalsIgnoreCase(node.category())).count();
        int mdrmCount = (int) nodes.stream().filter(node -> "MDRM".equalsIgnoreCase(node.category())).count();
        int discrepancyCount = (int) edges.stream().filter(edge -> edge.status() != null && !"VALID".equalsIgnoreCase(edge.status())).count();
        return new RuleGraphResponse("RULES", normalizeRunId(runId), resolveAsOfDate(runId), reportCount, mdrmCount, ruleCount, discrepancyCount, nodes, edges);
    }

    public RuleDiscrepancyResponse getRuleDiscrepancies(Long runId, String reportingForm, String status) {
        Long normalizedRunId = normalizeRunId(runId);
        String sql = "SELECT * FROM " + MdrmConstants.DEFAULT_RULE_DISCREPANCY_FUNCTION + "(?, ?, ?)";
        List<RuleDiscrepancyRecord> rows = jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    setNullableLong(ps, 1, normalizedRunId);
                    ps.setString(2, blankToNull(reportingForm));
                    ps.setString(3, blankToNull(status));
                    return ps;
                },
                (rs, rowNum) -> new RuleDiscrepancyRecord(
                        rs.getLong("rule_id"),
                        rs.getObject("dependency_id") == null ? null : rs.getLong("dependency_id"),
                        rs.getString("reporting_form"),
                        rs.getString("schedule_name"),
                        rs.getString("rule_type"),
                        rs.getString("rule_number"),
                        rs.getString("primary_mdrm_code"),
                        rs.getString("secondary_mdrm_code"),
                        rs.getString("secondary_token_raw"),
                        rs.getString("lineage_status"),
                        rs.getString("message")
                )
        );
        return new RuleDiscrepancyResponse(normalizedRunId, resolveAsOfDate(normalizedRunId), rows.size(), rows);
    }

    private void ensureTables() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS __RULE_LOADS__ (
                    load_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    source_system TEXT NOT NULL,
                    report_series TEXT,
                    source_file_name TEXT NOT NULL,
                    file_checksum TEXT NOT NULL,
                    loaded_at BIGINT NOT NULL,
                    load_status TEXT NOT NULL,
                    total_rule_rows INT NOT NULL DEFAULT 0,
                    total_dependency_rows INT NOT NULL DEFAULT 0,
                    warning_count INT NOT NULL DEFAULT 0,
                    error_count INT NOT NULL DEFAULT 0,
                    error_message TEXT
                )
                """.replace("__RULE_LOADS__", MdrmConstants.DEFAULT_RULE_LOADS_TABLE));
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS __RULES__ (
                    rule_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    load_id BIGINT NOT NULL REFERENCES __RULE_LOADS__(load_id),
                    rule_category TEXT NOT NULL,
                    source_sheet TEXT NOT NULL,
                    source_row_number INT NOT NULL,
                    source_page INT,
                    report_series TEXT,
                    reporting_form TEXT,
                    schedule_name TEXT,
                    rule_type TEXT,
                    rule_number TEXT,
                    target_item_label TEXT,
                    primary_mdrm_code TEXT,
                    effective_start_date DATE,
                    effective_end_date DATE,
                    change_status TEXT,
                    rule_text TEXT,
                    rule_expression TEXT,
                    row_hash TEXT NOT NULL,
                    UNIQUE (load_id, source_sheet, source_row_number),
                    UNIQUE (load_id, row_hash)
                )
                """
                .replace("__RULE_LOADS__", MdrmConstants.DEFAULT_RULE_LOADS_TABLE)
                .replace("__RULES__", MdrmConstants.DEFAULT_RULES_TABLE));
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS __RULE_DEPS__ (
                    dependency_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    rule_id BIGINT NOT NULL REFERENCES __RULES__(rule_id) ON DELETE CASCADE,
                    primary_mdrm_code TEXT NOT NULL,
                    secondary_token_raw TEXT NOT NULL,
                    secondary_mdrm_code TEXT,
                    dependency_kind TEXT NOT NULL,
                    qualifier_detail TEXT,
                    parse_confidence TEXT NOT NULL,
                    is_self_reference BOOLEAN NOT NULL DEFAULT FALSE,
                    dependency_hash TEXT NOT NULL,
                    UNIQUE (rule_id, dependency_hash)
                )
                """
                .replace("__RULES__", MdrmConstants.DEFAULT_RULES_TABLE)
                .replace("__RULE_DEPS__", MdrmConstants.DEFAULT_RULE_DEPENDENCIES_TABLE));
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS __RULE_WARNINGS__ (
                    warning_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    load_id BIGINT NOT NULL REFERENCES __RULE_LOADS__(load_id),
                    rule_id BIGINT REFERENCES __RULES__(rule_id) ON DELETE SET NULL,
                    source_sheet TEXT,
                    source_row_number INT,
                    warning_type TEXT NOT NULL,
                    warning_message TEXT NOT NULL,
                    warning_context TEXT
                )
                """
                .replace("__RULE_LOADS__", MdrmConstants.DEFAULT_RULE_LOADS_TABLE)
                .replace("__RULES__", MdrmConstants.DEFAULT_RULES_TABLE)
                .replace("__RULE_WARNINGS__", MdrmConstants.DEFAULT_RULE_WARNINGS_TABLE));
    }

    private void ensureIndexes() {
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_rule_loads_checksum ON " + MdrmConstants.DEFAULT_RULE_LOADS_TABLE + " (file_checksum)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_rules_load_id ON " + MdrmConstants.DEFAULT_RULES_TABLE + " (load_id)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_rules_primary_mdrm ON " + MdrmConstants.DEFAULT_RULES_TABLE + " (primary_mdrm_code)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_rules_reporting_form ON " + MdrmConstants.DEFAULT_RULES_TABLE + " (reporting_form, schedule_name)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_rules_category_type ON " + MdrmConstants.DEFAULT_RULES_TABLE + " (rule_category, rule_type)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_rules_effective_dates ON " + MdrmConstants.DEFAULT_RULES_TABLE + " (effective_start_date, effective_end_date)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_rule_deps_rule_id ON " + MdrmConstants.DEFAULT_RULE_DEPENDENCIES_TABLE + " (rule_id)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_rule_deps_primary ON " + MdrmConstants.DEFAULT_RULE_DEPENDENCIES_TABLE + " (primary_mdrm_code)");
        jdbcTemplate.execute("CREATE INDEX IF NOT EXISTS idx_rule_deps_secondary ON " + MdrmConstants.DEFAULT_RULE_DEPENDENCIES_TABLE + " (secondary_mdrm_code)");
        if (isPostgres()) {
            jdbcTemplate.execute("""
                    CREATE INDEX IF NOT EXISTS idx_rules_search_fts
                    ON __RULES__
                    USING GIN (
                        to_tsvector(
                            'simple',
                            COALESCE(rule_number, '') || ' ' ||
                            COALESCE(primary_mdrm_code, '') || ' ' ||
                            COALESCE(report_series, '') || ' ' ||
                            COALESCE(reporting_form, '') || ' ' ||
                            COALESCE(schedule_name, '') || ' ' ||
                            COALESCE(rule_type, '') || ' ' ||
                            COALESCE(target_item_label, '') || ' ' ||
                            COALESCE(rule_text, '') || ' ' ||
                            COALESCE(rule_expression, '')
                        )
                    )
                    """.replace("__RULES__", MdrmConstants.DEFAULT_RULES_TABLE));
        }
    }

    private void ensureSqlFunctions() {
        if (!isPostgres()) {
            return;
        }
        jdbcTemplate.execute("""
                CREATE OR REPLACE FUNCTION normalize_mdrm_text(value TEXT)
                RETURNS TEXT
                LANGUAGE SQL
                IMMUTABLE
                AS $$
                SELECT NULLIF(
                    CASE
                        WHEN UPPER(BTRIM(value)) ~ '^[A-Z]{4}[A-Z0-9]{4}-Q[1-8]$'
                            THEN REGEXP_REPLACE(UPPER(BTRIM(value)), '^([A-Z]{4}[A-Z0-9]{4})-Q[1-8]$', '\\1')
                        ELSE UPPER(BTRIM(value))
                    END,
                    ''
                )
                $$;
                """);
        jdbcTemplate.execute("""
                CREATE OR REPLACE FUNCTION normalize_report_key(value TEXT)
                RETURNS TEXT
                LANGUAGE SQL
                IMMUTABLE
                AS $$
                SELECT NULLIF(REGEXP_REPLACE(UPPER(COALESCE(value, '')), '[^A-Z0-9]', '', 'g'), '')
                $$;
                """);
        jdbcTemplate.execute("""
                CREATE OR REPLACE FUNCTION rules_as_of_date(p_run_id BIGINT)
                RETURNS DATE
                LANGUAGE SQL
                STABLE
                AS $$
                SELECT COALESCE(
                    (
                        SELECT TO_TIMESTAMP(rm.run_datetime / 1000.0)::DATE
                        FROM __RUN_MASTER__ rm
                        WHERE rm.run_id = p_run_id
                    ),
                    CURRENT_DATE
                )
                $$;
                """.replace("__RUN_MASTER__", MdrmConstants.DEFAULT_RUN_MASTER_TABLE));
        jdbcTemplate.execute(buildRuleSearchFunctionSql());
        jdbcTemplate.execute(buildRuleGraphFunctionSql());
        jdbcTemplate.execute(buildRuleDiscrepancyFunctionSql());
    }

    private String buildRuleSearchFunctionSql() {
        return """
                CREATE OR REPLACE FUNCTION __SEARCH_FN__(
                    p_query TEXT,
                    p_run_id BIGINT DEFAULT NULL,
                    p_reporting_form TEXT DEFAULT NULL,
                    p_rule_category TEXT DEFAULT NULL,
                    p_schedule_name TEXT DEFAULT NULL,
                    p_rule_type TEXT DEFAULT NULL
                )
                RETURNS TABLE (
                    rule_id BIGINT,
                    rule_category TEXT,
                    report_series TEXT,
                    reporting_form TEXT,
                    schedule_name TEXT,
                    rule_type TEXT,
                    rule_number TEXT,
                    primary_mdrm_code TEXT,
                    target_item_label TEXT,
                    effective_start_date DATE,
                    effective_end_date DATE,
                    change_status TEXT,
                    rule_text TEXT,
                    rule_expression TEXT,
                    dependency_count INT,
                    discrepancy_count INT,
                    lineage_status TEXT
                )
                LANGUAGE SQL
                STABLE
                AS $$
                WITH as_of_ctx AS (
                    SELECT CASE WHEN p_run_id IS NULL THEN CURRENT_DATE ELSE rules_as_of_date(p_run_id) END AS as_of_date
                ),
                mdrm_status AS (
                    SELECT
                        normalize_mdrm_text(m.mdrm_code) AS mdrm_code,
                        MAX(CASE WHEN UPPER(BTRIM(COALESCE(m.is_active, ''))) = 'Y' THEN 1 ELSE 0 END) AS has_active,
                        MAX(CASE WHEN UPPER(BTRIM(COALESCE(m.is_active, ''))) = 'N' THEN 1 ELSE 0 END) AS has_inactive
                    FROM __MASTER__ m
                    WHERE p_run_id IS NOT NULL
                      AND m.run_id = p_run_id
                    GROUP BY 1
                ),
                candidate_rules AS (
                    SELECT r.*
                    FROM __RULES__ r
                    CROSS JOIN as_of_ctx a
                    WHERE (p_reporting_form IS NULL OR normalize_report_key(COALESCE(r.reporting_form, r.report_series)) = normalize_report_key(p_reporting_form))
                      AND (p_rule_category IS NULL OR UPPER(COALESCE(r.rule_category, '')) = UPPER(p_rule_category))
                      AND (p_schedule_name IS NULL OR UPPER(COALESCE(r.schedule_name, '')) = UPPER(p_schedule_name))
                      AND (p_rule_type IS NULL OR UPPER(COALESCE(r.rule_type, '')) = UPPER(p_rule_type))
                      AND CASE
                          WHEN p_query IS NULL OR BTRIM(p_query) = '' THEN TRUE
                          ELSE to_tsvector(
                              'simple',
                              COALESCE(r.rule_number, '') || ' ' ||
                              COALESCE(r.primary_mdrm_code, '') || ' ' ||
                              COALESCE(r.report_series, '') || ' ' ||
                              COALESCE(r.reporting_form, '') || ' ' ||
                              COALESCE(r.schedule_name, '') || ' ' ||
                              COALESCE(r.rule_type, '') || ' ' ||
                              COALESCE(r.target_item_label, '') || ' ' ||
                              COALESCE(r.rule_text, '') || ' ' ||
                              COALESCE(r.rule_expression, '')
                          ) @@ websearch_to_tsquery('simple', p_query)
                      END
                      AND (
                          p_run_id IS NULL OR (
                              COALESCE(r.effective_start_date, DATE '1900-01-01') <= a.as_of_date
                              AND COALESCE(r.effective_end_date, DATE '9999-12-31') >= a.as_of_date
                          )
                      )
                ),
                rule_stats AS (
                    SELECT
                        r.rule_id,
                        COUNT(d.dependency_id)::INT AS dependency_count,
                        COUNT(*) FILTER (
                            WHERE p_run_id IS NOT NULL AND (
                                (d.secondary_mdrm_code IS NOT NULL AND ms_secondary.mdrm_code IS NULL)
                                OR (d.secondary_mdrm_code IS NOT NULL AND ms_secondary.has_active = 0 AND ms_secondary.has_inactive = 1)
                                OR (ms_primary.mdrm_code IS NULL)
                            )
                        )::INT AS discrepancy_count,
                        CASE
                            WHEN p_run_id IS NOT NULL AND ms_primary.mdrm_code IS NULL THEN 'MISSING_PRIMARY'
                            WHEN COUNT(*) FILTER (WHERE d.secondary_mdrm_code IS NOT NULL AND ms_secondary.mdrm_code IS NULL) > 0 THEN 'MISSING_DEPENDENCY'
                            WHEN COUNT(*) FILTER (WHERE d.secondary_mdrm_code IS NOT NULL AND ms_secondary.has_active = 0 AND ms_secondary.has_inactive = 1) > 0 THEN 'INACTIVE_DEPENDENCY'
                            WHEN COUNT(*) FILTER (WHERE d.dependency_id IS NOT NULL AND d.secondary_mdrm_code IS NULL AND UPPER(COALESCE(d.parse_confidence, '')) <> 'HIGH') > 0 THEN 'PARSE_WARNING'
                            ELSE 'VALID'
                        END AS lineage_status
                    FROM candidate_rules r
                    LEFT JOIN __RULE_DEPS__ d ON d.rule_id = r.rule_id
                    LEFT JOIN mdrm_status ms_primary ON ms_primary.mdrm_code = normalize_mdrm_text(r.primary_mdrm_code)
                    LEFT JOIN mdrm_status ms_secondary ON ms_secondary.mdrm_code = normalize_mdrm_text(d.secondary_mdrm_code)
                    GROUP BY r.rule_id, ms_primary.mdrm_code
                )
                SELECT
                    r.rule_id,
                    r.rule_category,
                    r.report_series,
                    r.reporting_form,
                    r.schedule_name,
                    r.rule_type,
                    r.rule_number,
                    r.primary_mdrm_code,
                    r.target_item_label,
                    r.effective_start_date,
                    r.effective_end_date,
                    r.change_status,
                    r.rule_text,
                    r.rule_expression,
                    COALESCE(s.dependency_count, 0) AS dependency_count,
                    COALESCE(s.discrepancy_count, 0) AS discrepancy_count,
                    COALESCE(s.lineage_status, 'VALID') AS lineage_status
                FROM candidate_rules r
                LEFT JOIN rule_stats s ON s.rule_id = r.rule_id
                ORDER BY r.reporting_form, r.schedule_name, r.rule_number, r.rule_id
                LIMIT 500
                $$;
                """
                .replace("__SEARCH_FN__", MdrmConstants.DEFAULT_RULE_SEARCH_FUNCTION)
                .replace("__MASTER__", MdrmConstants.DEFAULT_MASTER_TABLE)
                .replace("__RULES__", MdrmConstants.DEFAULT_RULES_TABLE)
                .replace("__RULE_DEPS__", MdrmConstants.DEFAULT_RULE_DEPENDENCIES_TABLE);
    }

    private String buildRuleGraphFunctionSql() {
        return """
                CREATE OR REPLACE FUNCTION __GRAPH_FN__(
                    p_root_mdrm TEXT DEFAULT NULL,
                    p_reporting_form TEXT DEFAULT NULL,
                    p_run_id BIGINT DEFAULT NULL,
                    p_depth INT DEFAULT 1
                )
                RETURNS TABLE (
                    source_id TEXT,
                    source_label TEXT,
                    source_category TEXT,
                    source_status TEXT,
                    target_id TEXT,
                    target_label TEXT,
                    target_category TEXT,
                    target_status TEXT,
                    relation TEXT,
                    edge_status TEXT
                )
                LANGUAGE SQL
                STABLE
                AS $$
                WITH RECURSIVE
                as_of_ctx AS (
                    SELECT CASE WHEN p_run_id IS NULL THEN CURRENT_DATE ELSE rules_as_of_date(p_run_id) END AS as_of_date
                ),
                mdrm_status AS (
                    SELECT
                        normalize_mdrm_text(m.mdrm_code) AS mdrm_code,
                        CASE
                            WHEN MAX(CASE WHEN UPPER(BTRIM(COALESCE(m.is_active, ''))) = 'Y' THEN 1 ELSE 0 END) = 1 THEN 'Active'
                            WHEN MAX(CASE WHEN UPPER(BTRIM(COALESCE(m.is_active, ''))) = 'N' THEN 1 ELSE 0 END) = 1 THEN 'Inactive'
                            ELSE NULL
                        END AS mdrm_status
                    FROM __MASTER__ m
                    WHERE p_run_id IS NOT NULL
                      AND m.run_id = p_run_id
                    GROUP BY 1
                ),
                seed_rules AS (
                    SELECT r.rule_id, 1 AS depth
                    FROM __RULES__ r
                    CROSS JOIN as_of_ctx a
                    WHERE (
                        (
                            p_root_mdrm IS NOT NULL
                            AND normalize_mdrm_text(r.primary_mdrm_code) = normalize_mdrm_text(p_root_mdrm)
                        ) OR (
                            p_reporting_form IS NOT NULL
                            AND normalize_report_key(COALESCE(r.reporting_form, r.report_series)) = normalize_report_key(p_reporting_form)
                        )
                    )
                    AND COALESCE(r.effective_start_date, DATE '1900-01-01') <= a.as_of_date
                    AND COALESCE(r.effective_end_date, DATE '9999-12-31') >= a.as_of_date
                ),
                walked_rules AS (
                    SELECT * FROM seed_rules
                    UNION ALL
                    SELECT r2.rule_id, wr.depth + 1
                    FROM walked_rules wr
                    JOIN __RULE_DEPS__ d ON d.rule_id = wr.rule_id
                    JOIN __RULES__ r2 ON normalize_mdrm_text(r2.primary_mdrm_code) = normalize_mdrm_text(d.secondary_mdrm_code)
                    CROSS JOIN as_of_ctx a
                    WHERE wr.depth < GREATEST(1, LEAST(COALESCE(p_depth, 1), 5))
                      AND COALESCE(r2.effective_start_date, DATE '1900-01-01') <= a.as_of_date
                      AND COALESCE(r2.effective_end_date, DATE '9999-12-31') >= a.as_of_date
                ),
                relevant_rules AS (
                    SELECT DISTINCT r.*
                    FROM walked_rules wr
                    JOIN __RULES__ r ON r.rule_id = wr.rule_id
                ),
                rule_edges AS (
                    SELECT
                        'report:' || COALESCE(rr.reporting_form, rr.report_series, 'Unknown') AS source_id,
                        COALESCE(rr.reporting_form, rr.report_series, 'Unknown') AS source_label,
                        'REPORT' AS source_category,
                        NULL::TEXT AS source_status,
                        'mdrm:' || COALESCE(rr.primary_mdrm_code, 'UNKNOWN') AS target_id,
                        COALESCE(rr.primary_mdrm_code, 'UNKNOWN') AS target_label,
                        'MDRM' AS target_category,
                        ms_primary.mdrm_status AS target_status,
                        'REPORT_HAS_MDRM' AS relation,
                        'VALID' AS edge_status
                    FROM relevant_rules rr
                    LEFT JOIN mdrm_status ms_primary ON ms_primary.mdrm_code = normalize_mdrm_text(rr.primary_mdrm_code)
                    UNION ALL
                    SELECT
                        'mdrm:' || COALESCE(rr.primary_mdrm_code, 'UNKNOWN') AS source_id,
                        COALESCE(rr.primary_mdrm_code, 'UNKNOWN') AS source_label,
                        'MDRM' AS source_category,
                        ms_primary.mdrm_status AS source_status,
                        'rule:' || rr.rule_id AS target_id,
                        COALESCE(rr.rule_type, 'Rule') || ' ' || COALESCE(rr.rule_number, '#' || rr.rule_id::TEXT) AS target_label,
                        'RULE' AS target_category,
                        CASE
                            WHEN COUNT(*) FILTER (WHERE d.secondary_mdrm_code IS NOT NULL AND ms_secondary.mdrm_status = 'Inactive') > 0 THEN 'INACTIVE_DEPENDENCY'
                            WHEN COUNT(*) FILTER (WHERE d.secondary_mdrm_code IS NOT NULL AND ms_secondary.mdrm_code IS NULL) > 0 THEN 'MISSING_DEPENDENCY'
                            WHEN COUNT(*) FILTER (WHERE d.dependency_id IS NOT NULL AND d.secondary_mdrm_code IS NULL AND UPPER(COALESCE(d.parse_confidence, '')) <> 'HIGH') > 0 THEN 'PARSE_WARNING'
                            ELSE 'VALID'
                        END AS target_status,
                        'MDRM_HAS_RULE' AS relation,
                        CASE
                            WHEN COUNT(*) FILTER (WHERE d.secondary_mdrm_code IS NOT NULL AND ms_secondary.mdrm_status = 'Inactive') > 0 THEN 'INACTIVE_DEPENDENCY'
                            WHEN COUNT(*) FILTER (WHERE d.secondary_mdrm_code IS NOT NULL AND ms_secondary.mdrm_code IS NULL) > 0 THEN 'MISSING_DEPENDENCY'
                            WHEN COUNT(*) FILTER (WHERE d.dependency_id IS NOT NULL AND d.secondary_mdrm_code IS NULL AND UPPER(COALESCE(d.parse_confidence, '')) <> 'HIGH') > 0 THEN 'PARSE_WARNING'
                            ELSE 'VALID'
                        END AS edge_status
                    FROM relevant_rules rr
                    LEFT JOIN __RULE_DEPS__ d ON d.rule_id = rr.rule_id
                    LEFT JOIN mdrm_status ms_primary ON ms_primary.mdrm_code = normalize_mdrm_text(rr.primary_mdrm_code)
                    LEFT JOIN mdrm_status ms_secondary ON ms_secondary.mdrm_code = normalize_mdrm_text(d.secondary_mdrm_code)
                    GROUP BY rr.rule_id, rr.rule_type, rr.rule_number, rr.primary_mdrm_code, ms_primary.mdrm_status
                    UNION ALL
                    SELECT
                        'rule:' || rr.rule_id AS source_id,
                        COALESCE(rr.rule_type, 'Rule') || ' ' || COALESCE(rr.rule_number, '#' || rr.rule_id::TEXT) AS source_label,
                        'RULE' AS source_category,
                        CASE
                            WHEN d.secondary_mdrm_code IS NOT NULL AND ms_secondary.mdrm_status = 'Inactive' THEN 'INACTIVE_DEPENDENCY'
                            WHEN d.secondary_mdrm_code IS NOT NULL AND ms_secondary.mdrm_code IS NULL THEN 'MISSING_DEPENDENCY'
                            WHEN d.dependency_id IS NOT NULL AND d.secondary_mdrm_code IS NULL AND UPPER(COALESCE(d.parse_confidence, '')) <> 'HIGH' THEN 'PARSE_WARNING'
                            ELSE 'VALID'
                        END AS source_status,
                        'mdrm:' || COALESCE(d.secondary_mdrm_code, d.secondary_token_raw, 'UNKNOWN') AS target_id,
                        COALESCE(d.secondary_mdrm_code, d.secondary_token_raw, 'UNKNOWN') AS target_label,
                        'MDRM' AS target_category,
                        ms_secondary.mdrm_status AS target_status,
                        'RULE_REQUIRES_MDRM' AS relation,
                        CASE
                            WHEN d.secondary_mdrm_code IS NOT NULL AND ms_secondary.mdrm_status = 'Inactive' THEN 'INACTIVE_DEPENDENCY'
                            WHEN d.secondary_mdrm_code IS NOT NULL AND ms_secondary.mdrm_code IS NULL THEN 'MISSING_DEPENDENCY'
                            WHEN d.dependency_id IS NOT NULL AND d.secondary_mdrm_code IS NULL AND UPPER(COALESCE(d.parse_confidence, '')) <> 'HIGH' THEN 'PARSE_WARNING'
                            ELSE 'VALID'
                        END AS edge_status
                    FROM relevant_rules rr
                    JOIN __RULE_DEPS__ d ON d.rule_id = rr.rule_id
                    LEFT JOIN mdrm_status ms_secondary ON ms_secondary.mdrm_code = normalize_mdrm_text(d.secondary_mdrm_code)
                    WHERE COALESCE(d.is_self_reference, FALSE) = FALSE
                )
                SELECT DISTINCT *
                FROM rule_edges
                WHERE source_id IS NOT NULL AND target_id IS NOT NULL
                $$;
                """
                .replace("__GRAPH_FN__", MdrmConstants.DEFAULT_RULE_GRAPH_FUNCTION)
                .replace("__MASTER__", MdrmConstants.DEFAULT_MASTER_TABLE)
                .replace("__RULES__", MdrmConstants.DEFAULT_RULES_TABLE)
                .replace("__RULE_DEPS__", MdrmConstants.DEFAULT_RULE_DEPENDENCIES_TABLE);
    }

    private String buildRuleDiscrepancyFunctionSql() {
        return """
                CREATE OR REPLACE FUNCTION __DISC_FN__(
                    p_run_id BIGINT DEFAULT NULL,
                    p_reporting_form TEXT DEFAULT NULL,
                    p_status TEXT DEFAULT NULL
                )
                RETURNS TABLE (
                    rule_id BIGINT,
                    dependency_id BIGINT,
                    reporting_form TEXT,
                    schedule_name TEXT,
                    rule_type TEXT,
                    rule_number TEXT,
                    primary_mdrm_code TEXT,
                    secondary_mdrm_code TEXT,
                    secondary_token_raw TEXT,
                    lineage_status TEXT,
                    message TEXT
                )
                LANGUAGE SQL
                STABLE
                AS $$
                WITH as_of_ctx AS (
                    SELECT CASE WHEN p_run_id IS NULL THEN CURRENT_DATE ELSE rules_as_of_date(p_run_id) END AS as_of_date
                ),
                mdrm_status AS (
                    SELECT
                        normalize_mdrm_text(m.mdrm_code) AS mdrm_code,
                        MAX(CASE WHEN UPPER(BTRIM(COALESCE(m.is_active, ''))) = 'Y' THEN 1 ELSE 0 END) AS has_active,
                        MAX(CASE WHEN UPPER(BTRIM(COALESCE(m.is_active, ''))) = 'N' THEN 1 ELSE 0 END) AS has_inactive
                    FROM __MASTER__ m
                    WHERE p_run_id IS NOT NULL
                      AND m.run_id = p_run_id
                    GROUP BY 1
                ),
                applicable_rules AS (
                    SELECT r.*
                    FROM __RULES__ r
                    CROSS JOIN as_of_ctx a
                    WHERE COALESCE(r.effective_start_date, DATE '1900-01-01') <= a.as_of_date
                      AND COALESCE(r.effective_end_date, DATE '9999-12-31') >= a.as_of_date
                      AND (p_reporting_form IS NULL OR normalize_report_key(COALESCE(r.reporting_form, r.report_series)) = normalize_report_key(p_reporting_form))
                ),
                raw_rows AS (
                    SELECT
                        r.rule_id,
                        d.dependency_id,
                        COALESCE(r.reporting_form, r.report_series) AS reporting_form,
                        r.schedule_name,
                        r.rule_type,
                        r.rule_number,
                        r.primary_mdrm_code,
                        d.secondary_mdrm_code,
                        d.secondary_token_raw,
                        CASE
                            WHEN p_run_id IS NOT NULL AND ms_primary.mdrm_code IS NULL THEN 'MISSING_PRIMARY'
                            WHEN d.secondary_mdrm_code IS NOT NULL AND ms_secondary.mdrm_code IS NULL THEN 'MISSING_DEPENDENCY'
                            WHEN d.secondary_mdrm_code IS NOT NULL AND ms_secondary.has_active = 0 AND ms_secondary.has_inactive = 1 THEN 'INACTIVE_DEPENDENCY'
                            WHEN d.dependency_id IS NOT NULL AND d.secondary_mdrm_code IS NULL AND UPPER(COALESCE(d.parse_confidence, '')) <> 'HIGH' THEN 'PARSE_WARNING'
                            ELSE 'VALID'
                        END AS lineage_status
                    FROM applicable_rules r
                    LEFT JOIN __RULE_DEPS__ d ON d.rule_id = r.rule_id
                    LEFT JOIN mdrm_status ms_primary ON ms_primary.mdrm_code = normalize_mdrm_text(r.primary_mdrm_code)
                    LEFT JOIN mdrm_status ms_secondary ON ms_secondary.mdrm_code = normalize_mdrm_text(d.secondary_mdrm_code)
                )
                SELECT
                    rr.rule_id,
                    rr.dependency_id,
                    rr.reporting_form,
                    rr.schedule_name,
                    rr.rule_type,
                    rr.rule_number,
                    rr.primary_mdrm_code,
                    rr.secondary_mdrm_code,
                    rr.secondary_token_raw,
                    rr.lineage_status,
                    CASE rr.lineage_status
                        WHEN 'MISSING_PRIMARY' THEN 'Primary MDRM is missing in selected run'
                        WHEN 'MISSING_DEPENDENCY' THEN 'Secondary MDRM is missing in selected run'
                        WHEN 'INACTIVE_DEPENDENCY' THEN 'Active rule depends on an inactive MDRM'
                        WHEN 'PARSE_WARNING' THEN 'Dependency token could not be normalized confidently'
                        ELSE 'No discrepancy'
                    END AS message
                FROM raw_rows rr
                WHERE rr.lineage_status <> 'VALID'
                  AND (p_status IS NULL OR UPPER(rr.lineage_status) = UPPER(p_status))
                ORDER BY rr.reporting_form, rr.schedule_name, rr.rule_number, rr.dependency_id
                $$;
                """
                .replace("__DISC_FN__", MdrmConstants.DEFAULT_RULE_DISCREPANCY_FUNCTION)
                .replace("__MASTER__", MdrmConstants.DEFAULT_MASTER_TABLE)
                .replace("__RULES__", MdrmConstants.DEFAULT_RULES_TABLE)
                .replace("__RULE_DEPS__", MdrmConstants.DEFAULT_RULE_DEPENDENCIES_TABLE);
    }

    private long createLoadRow(String fileName, String checksum) {
        String sql = """
                INSERT INTO __RULE_LOADS__ (
                    source_system,
                    report_series,
                    source_file_name,
                    file_checksum,
                    loaded_at,
                    load_status,
                    total_rule_rows,
                    total_dependency_rows,
                    warning_count,
                    error_count
                ) VALUES (?, NULL, ?, ?, ?, 'LOADING', 0, 0, 0, 0)
                RETURNING load_id
                """
                .replace("__RULE_LOADS__", MdrmConstants.DEFAULT_RULE_LOADS_TABLE);
        return jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ps.setString(1, firstNonBlank(properties.getRulesSourceSystem(), MdrmConstants.DEFAULT_RULES_SOURCE_SYSTEM));
                    ps.setString(2, firstNonBlank(fileName, properties.getRulesFilePath()));
                    ps.setString(3, checksum);
                    ps.setLong(4, Instant.now().toEpochMilli());
                    return ps;
                },
                rs -> rs.next() ? rs.getLong(1) : 0L
        );
    }

    private void markLoadCompleted(long loadId, int ruleCount, int dependencyCount, int warningCount) {
        String reportSeries = jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(
                            "SELECT COALESCE(MAX(report_series), MAX(reporting_form), ?) FROM " + MdrmConstants.DEFAULT_RULES_TABLE + " WHERE load_id = ?"
                    );
                    ps.setString(1, "UNKNOWN");
                    ps.setLong(2, loadId);
                    return ps;
                },
                rs -> rs.next() ? rs.getString(1) : "UNKNOWN"
        );
        jdbcTemplate.update(
                "UPDATE " + MdrmConstants.DEFAULT_RULE_LOADS_TABLE
                        + " SET load_status = 'COMPLETED', report_series = ?, total_rule_rows = ?, total_dependency_rows = ?, warning_count = ?, error_count = 0, error_message = NULL WHERE load_id = ?",
                reportSeries,
                ruleCount,
                dependencyCount,
                warningCount,
                loadId
        );
    }

    private void markLoadFailed(long loadId, String errorMessage) {
        jdbcTemplate.update(
                "UPDATE " + MdrmConstants.DEFAULT_RULE_LOADS_TABLE
                        + " SET load_status = 'FAILED', error_count = COALESCE(error_count, 0) + 1, error_message = ? WHERE load_id = ?",
                firstNonBlank(errorMessage, "Unknown import failure"),
                loadId
        );
    }

    private RuleLoadHistoryEntry findCompletedLoadByChecksum(String checksum) {
        String sql = """
                SELECT
                    load_id,
                    source_system,
                    report_series,
                    source_file_name,
                    loaded_at,
                    load_status,
                    total_rule_rows,
                    total_dependency_rows,
                    warning_count,
                    error_count
                FROM __RULE_LOADS__
                WHERE file_checksum = ?
                  AND load_status = 'COMPLETED'
                ORDER BY load_id DESC
                LIMIT 1
                """
                .replace("__RULE_LOADS__", MdrmConstants.DEFAULT_RULE_LOADS_TABLE);
        return jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ps.setString(1, checksum);
                    return ps;
                },
                rs -> rs.next()
                        ? new RuleLoadHistoryEntry(
                        rs.getLong("load_id"),
                        rs.getString("source_system"),
                        rs.getString("report_series"),
                        rs.getString("source_file_name"),
                        rs.getLong("loaded_at"),
                        rs.getString("load_status"),
                        rs.getInt("total_rule_rows"),
                        rs.getInt("total_dependency_rows"),
                        rs.getInt("warning_count"),
                        rs.getInt("error_count")
                )
                        : null
        );
    }

    private void clearRulesData() {
        if (isPostgres()) {
            jdbcTemplate.execute("""
                    TRUNCATE TABLE __WARNINGS__, __DEPENDENCIES__, __RULES__, __LOADS__
                    RESTART IDENTITY CASCADE
                    """
                    .replace("__WARNINGS__", MdrmConstants.DEFAULT_RULE_WARNINGS_TABLE)
                    .replace("__DEPENDENCIES__", MdrmConstants.DEFAULT_RULE_DEPENDENCIES_TABLE)
                    .replace("__RULES__", MdrmConstants.DEFAULT_RULES_TABLE)
                    .replace("__LOADS__", MdrmConstants.DEFAULT_RULE_LOADS_TABLE));
            return;
        }
        jdbcTemplate.execute("DELETE FROM " + MdrmConstants.DEFAULT_RULE_WARNINGS_TABLE);
        jdbcTemplate.execute("DELETE FROM " + MdrmConstants.DEFAULT_RULE_DEPENDENCIES_TABLE);
        jdbcTemplate.execute("DELETE FROM " + MdrmConstants.DEFAULT_RULES_TABLE);
        jdbcTemplate.execute("DELETE FROM " + MdrmConstants.DEFAULT_RULE_LOADS_TABLE);
    }

    private ParsedWorkbook parseWorkbook(byte[] content) {
        Pattern expressionPattern = Pattern.compile(firstNonBlank(properties.getRulesExpressionRegex(), MdrmConstants.DEFAULT_RULES_EXPRESSION_REGEX));
        DataFormatter formatter = new DataFormatter(Locale.US);
        List<RuleImportRow> rules = new ArrayList<>();
        List<DependencyImportRow> dependencies = new ArrayList<>();
        List<WarningImportRow> warnings = new ArrayList<>();
        String detectedReportSeries = null;

        try (Workbook workbook = WorkbookFactory.create(new java.io.ByteArrayInputStream(content))) {
            Map<String, Sheet> sheetsByNormalizedName = workbookSheetMap(workbook);
            Sheet ruleSheet = resolveSheet(sheetsByNormalizedName, RULE_SHEET_NAMES);
            if (ruleSheet == null) {
                throw new IllegalStateException("Rules workbook is missing a supported rule sheet (All_Edits)");
            }

            ParsedRuleSheet parsedRuleSheet = parseRuleSheet(ruleSheet, formatter, warnings);
            rules.addAll(parsedRuleSheet.rules());
            detectedReportSeries = parsedRuleSheet.reportSeries();

            Map<RuleReferenceKey, String> ruleHashByReference = parsedRuleSheet.ruleHashByReference();
            Sheet dependencySheet = resolveSheet(sheetsByNormalizedName, DEPENDENCY_SHEET_NAMES);
            if (dependencySheet != null) {
                dependencies.addAll(parseDependencySheet(dependencySheet, formatter, ruleHashByReference, warnings));
            } else {
                for (RuleImportRow rule : parsedRuleSheet.rules()) {
                    dependencies.addAll(deriveDependencies(rule, expressionPattern, warnings));
                }
            }
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to parse rules workbook", ex);
        }

        return new ParsedWorkbook(
                firstNonBlank(detectedReportSeries, "UNKNOWN"),
                rules,
                dedupeDependencies(dependencies, warnings),
                warnings
        );
    }

    private ParsedRuleSheet parseRuleSheet(Sheet sheet, DataFormatter formatter, List<WarningImportRow> warnings) {
        Map<String, Integer> headerMap = headerMap(sheet, formatter);
        int lastRowNum = sheet.getLastRowNum();
        List<RuleImportRow> rules = new ArrayList<>();
        Map<RuleReferenceKey, String> ruleHashByReference = new LinkedHashMap<>();
        String detectedReportSeries = null;

        for (int rowNum = 1; rowNum <= lastRowNum; rowNum++) {
            Row row = sheet.getRow(rowNum);
            if (row == null || isBlankRow(row, formatter)) {
                continue;
            }
            String sourceSheet = sheet.getSheetName();
            String reportSeries = firstNonBlank(
                    cellValue(row, headerMap, formatter, "reporting_form"),
                    cellValue(row, headerMap, formatter, "series"),
                    cellValue(row, headerMap, formatter, "report_series")
            );
            String reportingForm = firstNonBlank(
                    cellValue(row, headerMap, formatter, "reporting_form"),
                    reportSeries
            );
            String primaryMdrm = normalizeMdrmCode(firstNonBlank(
                    cellValue(row, headerMap, formatter, "primary_mdrm"),
                    cellValue(row, headerMap, formatter, "mdrm"),
                    cellValue(row, headerMap, formatter, "target_mdrm")
            ));
            if (primaryMdrm == null) {
                warnings.add(new WarningImportRow(null, sourceSheet, rowNum + 1, "MISSING_PRIMARY", "Primary MDRM is blank; row skipped", null));
                continue;
            }

            String ruleNumber = firstNonBlank(
                    cellValue(row, headerMap, formatter, "rule_number"),
                    cellValue(row, headerMap, formatter, "edit_number"),
                    "ROW-" + (rowNum + 1)
            );
            LocalDate effectiveStart = parseDateCell(row, headerMap, "effective_start_date");
            LocalDate effectiveEnd = parseDateCell(row, headerMap, "effective_end_date");
            String ruleExpression = firstNonBlank(
                    cellValue(row, headerMap, formatter, "rule_expression"),
                    cellValue(row, headerMap, formatter, "alg_edit_test"),
                    cellValue(row, headerMap, formatter, "expression_text")
            );
            String ruleText = firstNonBlank(
                    cellValue(row, headerMap, formatter, "rule_text"),
                    cellValue(row, headerMap, formatter, "edit_test"),
                    ruleExpression
            );
            String rowHash = sha256Base64(String.join("||",
                    sourceSheet,
                    String.valueOf(rowNum + 1),
                    firstNonBlank(reportSeries, ""),
                    firstNonBlank(reportingForm, ""),
                    firstNonBlank(ruleNumber, ""),
                    firstNonBlank(primaryMdrm, ""),
                    firstNonBlank(ruleExpression, "")
            ).getBytes(StandardCharsets.UTF_8));

            String sanitizedSecondaryMdrms = sanitizeSecondaryMdrms(
                    primaryMdrm,
                    cellValue(row, headerMap, formatter, "secondary_mdrms"),
                    rowHash,
                    sourceSheet,
                    rowNum + 1,
                    warnings
            );

            RuleImportRow importRow = new RuleImportRow(
                    sourceSheet,
                    rowNum + 1,
                    parseInteger(firstNonBlank(cellValue(row, headerMap, formatter, "source_page"), cellValue(row, headerMap, formatter, "pdf_page"))),
                    firstNonBlank(reportSeries, reportingForm),
                    firstNonBlank(reportingForm, reportSeries),
                    firstNonBlank(cellValue(row, headerMap, formatter, "rule_category"), properties.getRulesDefaultCategory()),
                    cellValue(row, headerMap, formatter, "schedule_name", "schedule"),
                    cellValue(row, headerMap, formatter, "rule_type", "edit_type"),
                    ruleNumber,
                    cellValue(row, headerMap, formatter, "target_item_label", "target_item_number"),
                    primaryMdrm,
                    effectiveStart,
                    effectiveEnd,
                    cellValue(row, headerMap, formatter, "change_status", "edit_change"),
                    ruleText,
                    ruleExpression,
                    rowHash,
                    sanitizedSecondaryMdrms
            );
            rules.add(importRow);
            if (detectedReportSeries == null) {
                detectedReportSeries = firstNonBlank(importRow.reportSeries(), importRow.reportingForm());
            }

            RuleReferenceKey ruleReferenceKey = new RuleReferenceKey(
                    normalizeReportKey(firstNonBlank(importRow.reportingForm(), importRow.reportSeries())),
                    normalizeValue(importRow.scheduleName()),
                    normalizeValue(importRow.ruleNumber()),
                    normalizeMdrmCode(importRow.primaryMdrmCode())
            );
            ruleHashByReference.put(ruleReferenceKey, rowHash);
        }

        return new ParsedRuleSheet(firstNonBlank(detectedReportSeries, "UNKNOWN"), rules, ruleHashByReference);
    }

    private List<DependencyImportRow> parseDependencySheet(
            Sheet sheet,
            DataFormatter formatter,
            Map<RuleReferenceKey, String> ruleHashByReference,
            List<WarningImportRow> warnings
    ) {
        Map<String, Integer> headerMap = headerMap(sheet, formatter);
        List<DependencyImportRow> rows = new ArrayList<>();
        for (int rowNum = 1; rowNum <= sheet.getLastRowNum(); rowNum++) {
            Row row = sheet.getRow(rowNum);
            if (row == null || isBlankRow(row, formatter)) {
                continue;
            }
            String primaryMdrm = normalizeMdrmCode(firstNonBlank(
                    cellValue(row, headerMap, formatter, "primary_mdrm"),
                    cellValue(row, headerMap, formatter, "mdrm"),
                    cellValue(row, headerMap, formatter, "target_mdrm")
            ));
            String secondaryRaw = firstNonBlank(
                    cellValue(row, headerMap, formatter, "secondary_mdrm"),
                    cellValue(row, headerMap, formatter, "secondary_mdrms"),
                    cellValue(row, headerMap, formatter, "dependency_mdrm")
            );
            if (secondaryRaw == null) {
                continue;
            }
            String secondaryMdrm = normalizeMdrmCode(secondaryRaw);
            String ruleNumber = firstNonBlank(
                    cellValue(row, headerMap, formatter, "rule_number"),
                    cellValue(row, headerMap, formatter, "edit_number")
            );
            RuleReferenceKey key = new RuleReferenceKey(
                    normalizeReportKey(firstNonBlank(
                            cellValue(row, headerMap, formatter, "reporting_form"),
                            cellValue(row, headerMap, formatter, "series"),
                            cellValue(row, headerMap, formatter, "report_series")
                    )),
                    normalizeValue(cellValue(row, headerMap, formatter, "schedule_name", "schedule")),
                    normalizeValue(ruleNumber),
                    primaryMdrm
            );
            String ruleHash = ruleHashByReference.get(key);
            if (ruleHash == null) {
                warnings.add(new WarningImportRow(null, sheet.getSheetName(), rowNum + 1, "UNMATCHED_DEPENDENCY", "Dependency row could not be matched to a rule", secondaryRaw));
                continue;
            }
            if (Objects.equals(primaryMdrm, secondaryMdrm)) {
                warnings.add(new WarningImportRow(ruleHash, sheet.getSheetName(), rowNum + 1, "SELF_REFERENCE_SKIPPED", "Primary MDRM will not be duplicated as a dependency", secondaryRaw));
                continue;
            }
            rows.add(new DependencyImportRow(
                    ruleHash,
                    primaryMdrm,
                    secondaryRaw.trim(),
                    secondaryMdrm,
                    firstNonBlank(cellValue(row, headerMap, formatter, "dependency_kind"), "MDRM"),
                    cellValue(row, headerMap, formatter, "qualifier_detail", "qualifier"),
                    firstNonBlank(cellValue(row, headerMap, formatter, "parse_confidence"), "HIGH"),
                    false
            ));
        }
        return rows;
    }

    private List<DependencyImportRow> deriveDependencies(RuleImportRow rule, Pattern expressionPattern, List<WarningImportRow> warnings) {
        List<DependencyImportRow> rows = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        String expressionText = rule.ruleExpression() == null ? "" : rule.ruleExpression();

        for (String candidate : splitCandidates(rule.secondaryMdrmsRaw())) {
            appendDerivedDependency(rule, candidate, null, "COLUMN", "HIGH", seen, rows, warnings);
        }

        Matcher matcher = expressionPattern.matcher(expressionText);
        while (matcher.find()) {
            String rawToken = matcher.group(0);
            String qualifier = matcher.groupCount() >= 2 ? blankToNull(matcher.group(2)) : null;
            appendDerivedDependency(rule, rawToken, qualifier, "MDRM", "HIGH", seen, rows, warnings);
        }

        if (rows.isEmpty() && !expressionText.trim().isEmpty()) {
            warnings.add(new WarningImportRow(rule.rowHash(), rule.sourceSheet(), rule.sourceRowNumber(), "NO_DEPENDENCIES_DERIVED", "No secondary MDRMs were derived from expression text", rule.ruleExpression()));
        }
        return rows;
    }

    private String sanitizeSecondaryMdrms(
            String primaryMdrm,
            String rawSecondaryMdrms,
            String ruleRowHash,
            String sourceSheet,
            int sourceRowNumber,
            List<WarningImportRow> warnings
    ) {
        List<String> candidates = splitCandidates(rawSecondaryMdrms);
        if (candidates.isEmpty()) {
            return null;
        }

        String normalizedPrimary = normalizeMdrmCode(primaryMdrm);
        List<String> sanitized = new ArrayList<>();
        Set<String> seen = new LinkedHashSet<>();
        for (String candidate : candidates) {
            String trimmed = blankToNull(candidate);
            if (trimmed == null) {
                continue;
            }
            String normalizedCandidate = normalizeMdrmCode(trimmed);
            if (normalizedPrimary != null && Objects.equals(normalizedPrimary, normalizedCandidate)) {
                warnings.add(new WarningImportRow(
                        ruleRowHash,
                        sourceSheet,
                        sourceRowNumber,
                        "SELF_REFERENCE_SKIPPED",
                        "Primary MDRM was removed from the secondary MDRM column during rule parsing",
                        trimmed
                ));
                continue;
            }

            String dedupeKey = firstNonBlank(normalizedCandidate, trimmed.toUpperCase(Locale.ROOT));
            if (seen.add(dedupeKey)) {
                sanitized.add(trimmed);
            }
        }
        return sanitized.isEmpty() ? null : String.join(", ", sanitized);
    }

    private void appendDerivedDependency(
            RuleImportRow rule,
            String rawToken,
            String qualifier,
            String dependencyKind,
            String parseConfidence,
            Set<String> seen,
            List<DependencyImportRow> rows,
            List<WarningImportRow> warnings
    ) {
        String trimmed = blankToNull(rawToken);
        if (trimmed == null) {
            return;
        }
        String normalizedToken = normalizeMdrmCode(trimmed);
        if (Objects.equals(normalizedToken, normalizeMdrmCode(rule.primaryMdrmCode()))) {
            warnings.add(new WarningImportRow(rule.rowHash(), rule.sourceSheet(), rule.sourceRowNumber(), "SELF_REFERENCE_SKIPPED", "Primary MDRM was referenced in expression and was not added as a dependency", trimmed));
            return;
        }
        String dedupeKey = String.join("|",
                firstNonBlank(rule.rowHash(), ""),
                firstNonBlank(normalizedToken, trimmed.toUpperCase(Locale.ROOT)),
                firstNonBlank(qualifier, "")
        );
        if (!seen.add(dedupeKey)) {
            warnings.add(new WarningImportRow(rule.rowHash(), rule.sourceSheet(), rule.sourceRowNumber(), "DUPLICATE_DEPENDENCY_REMOVED", "Duplicate dependency edge removed within the same rule", trimmed));
            return;
        }
        rows.add(new DependencyImportRow(
                rule.rowHash(),
                normalizeMdrmCode(rule.primaryMdrmCode()),
                trimmed,
                normalizedToken,
                firstNonBlank(dependencyKind, normalizedToken == null ? "UNKNOWN" : "MDRM"),
                qualifier,
                firstNonBlank(parseConfidence, normalizedToken == null ? "LOW" : "HIGH"),
                false
        ));
    }

    private List<DependencyImportRow> dedupeDependencies(List<DependencyImportRow> rows, List<WarningImportRow> warnings) {
        Map<String, DependencyImportRow> deduped = new LinkedHashMap<>();
        for (DependencyImportRow row : rows) {
            String key = String.join("|",
                    firstNonBlank(row.ruleRowHash(), ""),
                    firstNonBlank(row.primaryMdrmCode(), ""),
                    firstNonBlank(row.secondaryMdrmCode(), row.secondaryTokenRaw()),
                    firstNonBlank(row.qualifierDetail(), "")
            );
            if (!deduped.containsKey(key)) {
                deduped.put(key, row);
            } else {
                warnings.add(new WarningImportRow(row.ruleRowHash(), null, null, "DUPLICATE_DEPENDENCY_REMOVED", "Dependency row was duplicated and kept once", row.secondaryTokenRaw()));
            }
        }
        return new ArrayList<>(deduped.values());
    }

    private void batchInsertRules(long loadId, List<RuleImportRow> rows) {
        if (rows.isEmpty()) {
            return;
        }
        String sql = """
                INSERT INTO __RULES__ (
                    load_id,
                    rule_category,
                    source_sheet,
                    source_row_number,
                    source_page,
                    report_series,
                    reporting_form,
                    schedule_name,
                    rule_type,
                    rule_number,
                    target_item_label,
                    primary_mdrm_code,
                    effective_start_date,
                    effective_end_date,
                    change_status,
                    rule_text,
                    rule_expression,
                    row_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                .replace("__RULES__", MdrmConstants.DEFAULT_RULES_TABLE);
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                RuleImportRow row = rows.get(i);
                ps.setLong(1, loadId);
                ps.setString(2, firstNonBlank(row.ruleCategory(), properties.getRulesDefaultCategory()));
                ps.setString(3, row.sourceSheet());
                ps.setInt(4, row.sourceRowNumber());
                setNullableInteger(ps, 5, row.sourcePage());
                ps.setString(6, row.reportSeries());
                ps.setString(7, row.reportingForm());
                ps.setString(8, row.scheduleName());
                ps.setString(9, row.ruleType());
                ps.setString(10, row.ruleNumber());
                ps.setString(11, row.targetItemLabel());
                ps.setString(12, row.primaryMdrmCode());
                setNullableDate(ps, 13, row.effectiveStartDate());
                setNullableDate(ps, 14, row.effectiveEndDate());
                ps.setString(15, row.changeStatus());
                ps.setString(16, row.ruleText());
                ps.setString(17, row.ruleExpression());
                ps.setString(18, row.rowHash());
            }

            @Override
            public int getBatchSize() {
                return rows.size();
            }
        });
    }

    private Map<String, Long> loadRuleIdsByHash(long loadId) {
        String sql = "SELECT rule_id, row_hash FROM " + MdrmConstants.DEFAULT_RULES_TABLE + " WHERE load_id = ?";
        Map<String, Long> result = new HashMap<>();
        jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    ps.setLong(1, loadId);
                    return ps;
                },
                rs -> {
                    while (rs.next()) {
                        result.put(rs.getString("row_hash"), rs.getLong("rule_id"));
                    }
                    return null;
                }
        );
        return result;
    }

    private void batchInsertDependencies(Map<String, Long> ruleIdsByHash, List<DependencyImportRow> dependencies) {
        List<ResolvedDependencyRow> resolved = dependencies.stream()
                .map(row -> {
                    Long ruleId = ruleIdsByHash.get(row.ruleRowHash());
                    if (ruleId == null) {
                        return null;
                    }
                    String dependencyHash = sha256Base64(String.join("||",
                            String.valueOf(ruleId),
                            firstNonBlank(row.primaryMdrmCode(), ""),
                            firstNonBlank(row.secondaryMdrmCode(), row.secondaryTokenRaw()),
                            firstNonBlank(row.qualifierDetail(), "")
                    ).getBytes(StandardCharsets.UTF_8));
                    return new ResolvedDependencyRow(ruleId, row, dependencyHash);
                })
                .filter(Objects::nonNull)
                .toList();
        if (resolved.isEmpty()) {
            return;
        }
        String sql = """
                INSERT INTO __RULE_DEPS__ (
                    rule_id,
                    primary_mdrm_code,
                    secondary_token_raw,
                    secondary_mdrm_code,
                    dependency_kind,
                    qualifier_detail,
                    parse_confidence,
                    is_self_reference,
                    dependency_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (rule_id, dependency_hash) DO NOTHING
                """
                .replace("__RULE_DEPS__", MdrmConstants.DEFAULT_RULE_DEPENDENCIES_TABLE);
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ResolvedDependencyRow row = resolved.get(i);
                ps.setLong(1, row.ruleId());
                ps.setString(2, row.dependency().primaryMdrmCode());
                ps.setString(3, row.dependency().secondaryTokenRaw());
                ps.setString(4, row.dependency().secondaryMdrmCode());
                ps.setString(5, row.dependency().dependencyKind());
                ps.setString(6, row.dependency().qualifierDetail());
                ps.setString(7, row.dependency().parseConfidence());
                ps.setBoolean(8, row.dependency().selfReference());
                ps.setString(9, row.dependencyHash());
            }

            @Override
            public int getBatchSize() {
                return resolved.size();
            }
        });
    }

    private void batchInsertWarnings(long loadId, Map<String, Long> ruleIdsByHash, List<WarningImportRow> warnings) {
        if (warnings.isEmpty()) {
            return;
        }
        String sql = """
                INSERT INTO __WARNINGS__ (
                    load_id,
                    rule_id,
                    source_sheet,
                    source_row_number,
                    warning_type,
                    warning_message,
                    warning_context
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                .replace("__WARNINGS__", MdrmConstants.DEFAULT_RULE_WARNINGS_TABLE);
        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                WarningImportRow row = warnings.get(i);
                ps.setLong(1, loadId);
                Long ruleId = row.ruleRowHash() == null ? null : ruleIdsByHash.get(row.ruleRowHash());
                setNullableLong(ps, 2, ruleId);
                ps.setString(3, row.sourceSheet());
                setNullableInteger(ps, 4, row.sourceRowNumber());
                ps.setString(5, row.warningType());
                ps.setString(6, row.warningMessage());
                ps.setString(7, row.warningContext());
            }

            @Override
            public int getBatchSize() {
                return warnings.size();
            }
        });
    }

    private RuleRecord loadRuleRecord(long ruleId, Long runId) {
        List<RuleRecord> rules = queryScopedRules(
                normalizeRunId(runId),
                null,
                null,
                "r.rule_id = ?",
                new SqlBinder() {
                    @Override
                    public void bind(PreparedStatement ps, int startIndex) throws SQLException {
                        ps.setLong(startIndex, ruleId);
                    }

                    @Override
                    public int parameterCount() {
                        return 1;
                    }
                }
        );
        return rules.isEmpty() ? null : rules.get(0);
    }

    private List<RuleDependencyRecord> loadRuleDependencies(long ruleId, Long runId) {
        Long normalizedRunId = normalizeRunId(runId);
        String sql = """
                WITH mdrm_status AS (
                    SELECT
                        normalize_mdrm_text(m.mdrm_code) AS mdrm_code,
                        CASE
                            WHEN MAX(CASE WHEN UPPER(BTRIM(COALESCE(m.is_active, ''))) = 'Y' THEN 1 ELSE 0 END) = 1 THEN 'Active'
                            WHEN MAX(CASE WHEN UPPER(BTRIM(COALESCE(m.is_active, ''))) = 'N' THEN 1 ELSE 0 END) = 1 THEN 'Inactive'
                            ELSE NULL
                        END AS status
                    FROM __MASTER__ m
                    WHERE ? IS NOT NULL
                      AND m.run_id = ?
                    GROUP BY 1
                )
                SELECT
                    d.dependency_id,
                    d.primary_mdrm_code,
                    d.secondary_token_raw,
                    d.secondary_mdrm_code,
                    d.dependency_kind,
                    d.qualifier_detail,
                    d.parse_confidence,
                    d.is_self_reference,
                    CASE
                        WHEN d.dependency_id IS NOT NULL AND d.secondary_mdrm_code IS NULL AND UPPER(COALESCE(d.parse_confidence, '')) <> 'HIGH' THEN 'PARSE_WARNING'
                        WHEN ? IS NULL THEN 'VALID'
                        WHEN d.secondary_mdrm_code IS NOT NULL AND ms.status IS NULL THEN 'MISSING_DEPENDENCY'
                        WHEN d.secondary_mdrm_code IS NOT NULL AND ms.status = 'Inactive' THEN 'INACTIVE_DEPENDENCY'
                        ELSE 'VALID'
                    END AS dependency_status,
                    ms.status AS secondary_mdrm_status
                FROM __RULE_DEPS__ d
                LEFT JOIN mdrm_status ms ON ms.mdrm_code = normalize_mdrm_text(d.secondary_mdrm_code)
                WHERE d.rule_id = ?
                  AND NOT (
                      d.secondary_mdrm_code IS NOT NULL
                      AND normalize_mdrm_text(d.secondary_mdrm_code) = normalize_mdrm_text(d.primary_mdrm_code)
                  )
                ORDER BY d.dependency_id
                """
                .replace("__MASTER__", MdrmConstants.DEFAULT_MASTER_TABLE)
                .replace("__RULE_DEPS__", MdrmConstants.DEFAULT_RULE_DEPENDENCIES_TABLE);
        return jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    setNullableLong(ps, 1, normalizedRunId);
                    setNullableLong(ps, 2, normalizedRunId);
                    setNullableLong(ps, 3, normalizedRunId);
                    ps.setLong(4, ruleId);
                    return ps;
                },
                (rs, rowNum) -> new RuleDependencyRecord(
                        rs.getLong("dependency_id"),
                        rs.getString("primary_mdrm_code"),
                        rs.getString("secondary_token_raw"),
                        rs.getString("secondary_mdrm_code"),
                        rs.getString("dependency_kind"),
                        rs.getString("qualifier_detail"),
                        rs.getString("parse_confidence"),
                        rs.getBoolean("is_self_reference"),
                        rs.getString("dependency_status"),
                        rs.getString("secondary_mdrm_status")
                )
        );
    }

    private RuleRecord mapRuleRecord(java.sql.ResultSet rs) throws SQLException {
        return new RuleRecord(
                rs.getLong("rule_id"),
                rs.getString("rule_category"),
                rs.getString("report_series"),
                rs.getString("reporting_form"),
                rs.getString("schedule_name"),
                rs.getString("rule_type"),
                rs.getString("rule_number"),
                rs.getString("primary_mdrm_code"),
                rs.getString("target_item_label"),
                valueAsIsoDate(rs.getDate("effective_start_date")),
                valueAsIsoDate(rs.getDate("effective_end_date")),
                rs.getString("change_status"),
                rs.getString("rule_text"),
                rs.getString("rule_expression"),
                rs.getInt("dependency_count"),
                rs.getInt("discrepancy_count"),
                rs.getString("lineage_status")
        );
    }

    private int countDependenciesForRules(List<RuleRecord> rules) {
        return rules.stream().mapToInt(RuleRecord::dependencyCount).sum();
    }

    private List<RuleRecord> queryScopedRules(
            Long runId,
            String reportingForm,
            String scheduleName,
            String extraWhereSql,
            SqlBinder extraBinder
    ) {
        String sql = """
                WITH as_of_ctx AS (
                    SELECT CASE WHEN ? IS NULL THEN CURRENT_DATE ELSE rules_as_of_date(?) END AS as_of_date
                ),
                mdrm_status AS (
                    SELECT
                        normalize_mdrm_text(m.mdrm_code) AS mdrm_code,
                        MAX(CASE WHEN UPPER(BTRIM(COALESCE(m.is_active, ''))) = 'Y' THEN 1 ELSE 0 END) AS has_active,
                        MAX(CASE WHEN UPPER(BTRIM(COALESCE(m.is_active, ''))) = 'N' THEN 1 ELSE 0 END) AS has_inactive
                    FROM __MASTER__ m
                    WHERE ? IS NOT NULL
                      AND m.run_id = ?
                    GROUP BY 1
                ),
                candidate_rules AS (
                    SELECT r.*
                    FROM __RULES__ r
                    CROSS JOIN as_of_ctx a
                    WHERE (? IS NULL OR normalize_report_key(COALESCE(r.reporting_form, r.report_series)) = normalize_report_key(?))
                      AND (? IS NULL OR UPPER(COALESCE(r.schedule_name, '')) = UPPER(?))
                      AND (
                          ? IS NULL OR (
                              COALESCE(r.effective_start_date, DATE '1900-01-01') <= a.as_of_date
                              AND COALESCE(r.effective_end_date, DATE '9999-12-31') >= a.as_of_date
                          )
                      )
                      __EXTRA_WHERE__
                ),
                rule_stats AS (
                    SELECT
                        r.rule_id,
                        COUNT(d.dependency_id)::INT AS dependency_count,
                        COUNT(*) FILTER (
                            WHERE ? IS NOT NULL AND (
                                (d.secondary_mdrm_code IS NOT NULL AND ms_secondary.mdrm_code IS NULL)
                                OR (d.secondary_mdrm_code IS NOT NULL AND ms_secondary.has_active = 0 AND ms_secondary.has_inactive = 1)
                                OR (ms_primary.mdrm_code IS NULL)
                            )
                        )::INT AS discrepancy_count,
                        CASE
                            WHEN ? IS NOT NULL AND ms_primary.mdrm_code IS NULL THEN 'MISSING_PRIMARY'
                            WHEN COUNT(*) FILTER (WHERE d.secondary_mdrm_code IS NOT NULL AND ms_secondary.mdrm_code IS NULL) > 0 THEN 'MISSING_DEPENDENCY'
                            WHEN COUNT(*) FILTER (WHERE d.secondary_mdrm_code IS NOT NULL AND ms_secondary.has_active = 0 AND ms_secondary.has_inactive = 1) > 0 THEN 'INACTIVE_DEPENDENCY'
                            WHEN COUNT(*) FILTER (WHERE d.dependency_id IS NOT NULL AND d.secondary_mdrm_code IS NULL AND UPPER(COALESCE(d.parse_confidence, '')) <> 'HIGH') > 0 THEN 'PARSE_WARNING'
                            ELSE 'VALID'
                        END AS lineage_status
                    FROM candidate_rules r
                    LEFT JOIN __RULE_DEPS__ d ON d.rule_id = r.rule_id
                    LEFT JOIN mdrm_status ms_primary ON ms_primary.mdrm_code = normalize_mdrm_text(r.primary_mdrm_code)
                    LEFT JOIN mdrm_status ms_secondary ON ms_secondary.mdrm_code = normalize_mdrm_text(d.secondary_mdrm_code)
                    GROUP BY r.rule_id, ms_primary.mdrm_code
                )
                SELECT
                    r.rule_id,
                    r.rule_category,
                    r.report_series,
                    r.reporting_form,
                    r.schedule_name,
                    r.rule_type,
                    r.rule_number,
                    r.primary_mdrm_code,
                    r.target_item_label,
                    r.effective_start_date,
                    r.effective_end_date,
                    r.change_status,
                    r.rule_text,
                    r.rule_expression,
                    COALESCE(s.dependency_count, 0) AS dependency_count,
                    COALESCE(s.discrepancy_count, 0) AS discrepancy_count,
                    COALESCE(s.lineage_status, 'VALID') AS lineage_status
                FROM candidate_rules r
                LEFT JOIN rule_stats s ON s.rule_id = r.rule_id
                ORDER BY r.reporting_form, r.schedule_name, r.rule_number, r.rule_id
                """
                .replace("__MASTER__", MdrmConstants.DEFAULT_MASTER_TABLE)
                .replace("__RULES__", MdrmConstants.DEFAULT_RULES_TABLE)
                .replace("__RULE_DEPS__", MdrmConstants.DEFAULT_RULE_DEPENDENCIES_TABLE)
                .replace("__EXTRA_WHERE__", extraWhereSql == null || extraWhereSql.isBlank() ? "" : "\n  AND (" + extraWhereSql + ")");

        return jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    int index = 1;
                    setNullableLong(ps, index++, runId);
                    setNullableLong(ps, index++, runId);
                    setNullableLong(ps, index++, runId);
                    setNullableLong(ps, index++, runId);
                    ps.setString(index++, blankToNull(reportingForm));
                    ps.setString(index++, blankToNull(reportingForm));
                    ps.setString(index++, blankToNull(scheduleName));
                    ps.setString(index++, blankToNull(scheduleName));
                    setNullableLong(ps, index++, runId);
                    if (extraBinder != null) {
                        extraBinder.bind(ps, index);
                        index += extraBinder.parameterCount();
                    }
                    setNullableLong(ps, index++, runId);
                    setNullableLong(ps, index, runId);
                    return ps;
                },
                (rs, rowNum) -> mapRuleRecord(rs)
        );
    }

    private List<RuleRecord> queryRulesByScope(Long runId, List<String> normalizedReportKeys, List<String> normalizedMdrms) {
        StringBuilder reportClause = new StringBuilder();
        StringBuilder mdrmClause = new StringBuilder();
        if (!normalizedReportKeys.isEmpty()) {
            reportClause.append(" AND normalize_report_key(COALESCE(r.reporting_form, r.report_series)) IN (")
                    .append(String.join(", ", Collections.nCopies(normalizedReportKeys.size(), "?")))
                    .append(")");
        }
        if (!normalizedMdrms.isEmpty()) {
            mdrmClause.append(" AND normalize_mdrm_text(r.primary_mdrm_code) IN (")
                    .append(String.join(", ", Collections.nCopies(normalizedMdrms.size(), "?")))
                    .append(")");
        }

        String sql = """
                WITH as_of_ctx AS (
                    SELECT CASE WHEN ? IS NULL THEN CURRENT_DATE ELSE rules_as_of_date(?) END AS as_of_date
                ),
                mdrm_status AS (
                    SELECT
                        normalize_mdrm_text(m.mdrm_code) AS mdrm_code,
                        MAX(CASE WHEN UPPER(BTRIM(COALESCE(m.is_active, ''))) = 'Y' THEN 1 ELSE 0 END) AS has_active,
                        MAX(CASE WHEN UPPER(BTRIM(COALESCE(m.is_active, ''))) = 'N' THEN 1 ELSE 0 END) AS has_inactive
                    FROM __MASTER__ m
                    WHERE ? IS NOT NULL
                      AND m.run_id = ?
                    GROUP BY 1
                ),
                candidate_rules AS (
                    SELECT r.*
                    FROM __RULES__ r
                    CROSS JOIN as_of_ctx a
                    WHERE 1 = 1
                      AND (
                          ? IS NULL OR (
                              COALESCE(r.effective_start_date, DATE '1900-01-01') <= a.as_of_date
                              AND COALESCE(r.effective_end_date, DATE '9999-12-31') >= a.as_of_date
                          )
                      )
                      __REPORT_CLAUSE__
                      __MDRM_CLAUSE__
                ),
                rule_stats AS (
                    SELECT
                        r.rule_id,
                        COUNT(d.dependency_id)::INT AS dependency_count,
                        COUNT(*) FILTER (
                            WHERE ? IS NOT NULL AND (
                                (d.secondary_mdrm_code IS NOT NULL AND ms_secondary.mdrm_code IS NULL)
                                OR (d.secondary_mdrm_code IS NOT NULL AND ms_secondary.has_active = 0 AND ms_secondary.has_inactive = 1)
                                OR (ms_primary.mdrm_code IS NULL)
                            )
                        )::INT AS discrepancy_count,
                        CASE
                            WHEN ? IS NOT NULL AND ms_primary.mdrm_code IS NULL THEN 'MISSING_PRIMARY'
                            WHEN COUNT(*) FILTER (WHERE d.secondary_mdrm_code IS NOT NULL AND ms_secondary.mdrm_code IS NULL) > 0 THEN 'MISSING_DEPENDENCY'
                            WHEN COUNT(*) FILTER (WHERE d.secondary_mdrm_code IS NOT NULL AND ms_secondary.has_active = 0 AND ms_secondary.has_inactive = 1) > 0 THEN 'INACTIVE_DEPENDENCY'
                            WHEN COUNT(*) FILTER (WHERE d.dependency_id IS NOT NULL AND d.secondary_mdrm_code IS NULL AND UPPER(COALESCE(d.parse_confidence, '')) <> 'HIGH') > 0 THEN 'PARSE_WARNING'
                            ELSE 'VALID'
                        END AS lineage_status
                    FROM candidate_rules r
                    LEFT JOIN __RULE_DEPS__ d ON d.rule_id = r.rule_id
                    LEFT JOIN mdrm_status ms_primary ON ms_primary.mdrm_code = normalize_mdrm_text(r.primary_mdrm_code)
                    LEFT JOIN mdrm_status ms_secondary ON ms_secondary.mdrm_code = normalize_mdrm_text(d.secondary_mdrm_code)
                    GROUP BY r.rule_id, ms_primary.mdrm_code
                )
                SELECT
                    r.rule_id,
                    r.rule_category,
                    r.report_series,
                    r.reporting_form,
                    r.schedule_name,
                    r.rule_type,
                    r.rule_number,
                    r.primary_mdrm_code,
                    r.target_item_label,
                    r.effective_start_date,
                    r.effective_end_date,
                    r.change_status,
                    r.rule_text,
                    r.rule_expression,
                    COALESCE(s.dependency_count, 0) AS dependency_count,
                    COALESCE(s.discrepancy_count, 0) AS discrepancy_count,
                    COALESCE(s.lineage_status, 'VALID') AS lineage_status
                FROM candidate_rules r
                LEFT JOIN rule_stats s ON s.rule_id = r.rule_id
                ORDER BY r.reporting_form, r.schedule_name, r.rule_number, r.rule_id
                """
                .replace("__MASTER__", MdrmConstants.DEFAULT_MASTER_TABLE)
                .replace("__RULES__", MdrmConstants.DEFAULT_RULES_TABLE)
                .replace("__RULE_DEPS__", MdrmConstants.DEFAULT_RULE_DEPENDENCIES_TABLE)
                .replace("__REPORT_CLAUSE__", reportClause)
                .replace("__MDRM_CLAUSE__", mdrmClause);

        return jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    int index = 1;
                    setNullableLong(ps, index++, runId);
                    setNullableLong(ps, index++, runId);
                    setNullableLong(ps, index++, runId);
                    setNullableLong(ps, index++, runId);
                    setNullableLong(ps, index++, runId);
                    for (String reportKey : normalizedReportKeys) {
                        ps.setString(index++, reportKey);
                    }
                    for (String mdrmCode : normalizedMdrms) {
                        ps.setString(index++, mdrmCode);
                    }
                    setNullableLong(ps, index++, runId);
                    setNullableLong(ps, index, runId);
                    return ps;
                },
                (rs, rowNum) -> mapRuleRecord(rs)
        );
    }

    private String resolveAsOfDate(Long runId) {
        Long normalizedRunId = normalizeRunId(runId);
        String sql = "SELECT rules_as_of_date(?)";
        return jdbcTemplate.query(
                connection -> {
                    PreparedStatement ps = connection.prepareStatement(sql);
                    setNullableLong(ps, 1, normalizedRunId);
                    return ps;
                },
                rs -> rs.next() ? valueAsIsoDate(rs.getDate(1)) : valueAsIsoDate(Date.valueOf(LocalDate.now(ZoneOffset.UTC)))
        );
    }

    private Long normalizeRunId(Long runId) {
        return runId == null || runId <= 0 ? null : runId;
    }

    private boolean reportMatches(String left, String right) {
        String normalizedLeft = normalizeReportKey(left);
        String normalizedRight = normalizeReportKey(right);
        return normalizedLeft != null && normalizedLeft.equals(normalizedRight);
    }

    private Sheet resolveSheet(Map<String, Sheet> sheetsByNormalizedName, Collection<String> candidates) {
        for (String candidate : candidates) {
            Sheet match = sheetsByNormalizedName.get(candidate);
            if (match != null) {
                return match;
            }
        }
        return null;
    }

    private Map<String, Sheet> workbookSheetMap(Workbook workbook) {
        Map<String, Sheet> result = new LinkedHashMap<>();
        for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
            Sheet sheet = workbook.getSheetAt(i);
            result.put(normalizeHeader(sheet.getSheetName()), sheet);
        }
        return result;
    }

    private Map<String, Integer> headerMap(Sheet sheet, DataFormatter formatter) {
        Row headerRow = sheet.getRow(sheet.getFirstRowNum());
        if (headerRow == null) {
            throw new IllegalStateException("Sheet has no header row: " + sheet.getSheetName());
        }
        Map<String, Integer> result = new LinkedHashMap<>();
        for (Cell cell : headerRow) {
            String raw = formatter.formatCellValue(cell);
            String normalized = normalizeHeader(raw);
            if (!normalized.isEmpty()) {
                result.put(normalized, cell.getColumnIndex());
            }
        }
        return result;
    }

    private String cellValue(Row row, Map<String, Integer> headerMap, DataFormatter formatter, String... aliases) {
        for (String alias : aliases) {
            Integer index = headerMap.get(normalizeHeader(alias));
            if (index == null) {
                continue;
            }
            Cell cell = row.getCell(index);
            String value = cell == null ? null : formatter.formatCellValue(cell);
            value = blankToNull(value);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private LocalDate parseDateCell(Row row, Map<String, Integer> headerMap, String alias) {
        Integer index = headerMap.get(normalizeHeader(alias));
        if (index == null) {
            return null;
        }
        Cell cell = row.getCell(index);
        if (cell == null || cell.getCellType() == CellType.BLANK) {
            return null;
        }
        if (cell.getCellType() == CellType.NUMERIC) {
            if (DateUtil.isCellDateFormatted(cell)) {
                return cell.getLocalDateTimeCellValue().toLocalDate();
            }
            long numeric = Math.round(cell.getNumericCellValue());
            String digits = String.valueOf(numeric);
            if (digits.length() == 8) {
                return parseLocalDate(digits);
            }
        }
        if (cell.getCellType() == CellType.STRING || cell.getCellType() == CellType.FORMULA) {
            return parseLocalDate(blankToNull(cell.toString()));
        }
        return null;
    }

    private boolean isBlankRow(Row row, DataFormatter formatter) {
        if (row == null) {
            return true;
        }
        for (Cell cell : row) {
            if (blankToNull(formatter.formatCellValue(cell)) != null) {
                return false;
            }
        }
        return true;
    }

    private List<String> splitCandidates(String raw) {
        String value = blankToNull(raw);
        if (value == null) {
            return List.of();
        }
        return Arrays.stream(value.split("[,;\\n\\r\\t ]+"))
                .map(this::blankToNull)
                .filter(Objects::nonNull)
                .toList();
    }

    private LocalDate parseLocalDate(String value) {
        String trimmed = blankToNull(value);
        if (trimmed == null) {
            return null;
        }
        List<DateTimeFormatter> formatters = List.of(
                DateTimeFormatter.BASIC_ISO_DATE,
                DateTimeFormatter.ISO_LOCAL_DATE,
                DateTimeFormatter.ofPattern("M/d/yyyy"),
                DateTimeFormatter.ofPattern("MM/dd/yyyy")
        );
        for (DateTimeFormatter formatter : formatters) {
            try {
                return LocalDate.parse(trimmed, formatter);
            } catch (DateTimeParseException ignored) {
            }
        }
        return null;
    }

    private String normalizeHeader(String value) {
        if (value == null) {
            return "";
        }
        return value.trim()
                .toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9]+", "_")
                .replaceAll("^_+|_+$", "");
    }

    private String normalizeReportKey(String value) {
        String raw = blankToNull(value);
        return raw == null ? null : raw.replaceAll("[^A-Za-z0-9]", "").toUpperCase(Locale.ROOT);
    }

    private String normalizeValue(String value) {
        String raw = blankToNull(value);
        return raw == null ? null : raw.trim().toUpperCase(Locale.ROOT);
    }

    private String normalizeMdrmCode(String value) {
        String raw = blankToNull(value);
        if (raw == null) {
            return null;
        }
        String cleaned = raw.trim().toUpperCase(Locale.ROOT);
        Matcher matcher = MDRM_CODE_PATTERN.matcher(cleaned);
        return matcher.matches() ? matcher.group(1) : null;
    }

    private String blankToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            String normalized = blankToNull(value);
            if (normalized != null) {
                return normalized;
            }
        }
        return null;
    }

    private Integer parseInteger(String value) {
        String raw = blankToNull(value);
        if (raw == null) {
            return null;
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException ex) {
            return null;
        }
    }

    private void setNullableDate(PreparedStatement ps, int index, LocalDate value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.DATE);
        } else {
            ps.setDate(index, Date.valueOf(value));
        }
    }

    private void setNullableInteger(PreparedStatement ps, int index, Integer value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.INTEGER);
        } else {
            ps.setInt(index, value);
        }
    }

    private void setNullableLong(PreparedStatement ps, int index, Long value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.BIGINT);
        } else {
            ps.setLong(index, value);
        }
    }

    private String valueAsIsoDate(Date value) {
        if (value == null) {
            return null;
        }
        return value.toLocalDate().format(ISO_DATE);
    }

    private String sha256Base64(byte[] content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return Base64.getEncoder().encodeToString(digest.digest(content));
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is unavailable", ex);
        }
    }

    private boolean isPostgres() {
        Boolean postgres = jdbcTemplate.execute((ConnectionCallback<Boolean>) connection -> {
            String db = connection.getMetaData().getDatabaseProductName();
            return db != null && db.toLowerCase(Locale.ROOT).contains("postgresql");
        });
        return Boolean.TRUE.equals(postgres);
    }

    private record ParsedWorkbook(
            String reportSeries,
            List<RuleImportRow> rules,
            List<DependencyImportRow> dependencies,
            List<WarningImportRow> warnings
    ) {
    }

    private record ParsedRuleSheet(
            String reportSeries,
            List<RuleImportRow> rules,
            Map<RuleReferenceKey, String> ruleHashByReference
    ) {
    }

    private record RuleImportRow(
            String sourceSheet,
            int sourceRowNumber,
            Integer sourcePage,
            String reportSeries,
            String reportingForm,
            String ruleCategory,
            String scheduleName,
            String ruleType,
            String ruleNumber,
            String targetItemLabel,
            String primaryMdrmCode,
            LocalDate effectiveStartDate,
            LocalDate effectiveEndDate,
            String changeStatus,
            String ruleText,
            String ruleExpression,
            String rowHash,
            String secondaryMdrmsRaw
    ) {
    }

    private record DependencyImportRow(
            String ruleRowHash,
            String primaryMdrmCode,
            String secondaryTokenRaw,
            String secondaryMdrmCode,
            String dependencyKind,
            String qualifierDetail,
            String parseConfidence,
            boolean selfReference
    ) {
    }

    private record ResolvedDependencyRow(
            long ruleId,
            DependencyImportRow dependency,
            String dependencyHash
    ) {
    }

    private record WarningImportRow(
            String ruleRowHash,
            String sourceSheet,
            Integer sourceRowNumber,
            String warningType,
            String warningMessage,
            String warningContext
    ) {
    }

    private record RuleReferenceKey(
            String reportKey,
            String scheduleName,
            String ruleNumber,
            String primaryMdrmCode
    ) {
    }

    @FunctionalInterface
    private interface SqlBinder {
        void bind(PreparedStatement ps, int startIndex) throws SQLException;

        default int parameterCount() {
            return 0;
        }
    }
}
