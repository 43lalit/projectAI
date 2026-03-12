package com.projectai.projectai.mdrm;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class RulesServiceTest {

    @Autowired
    private RulesService rulesService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void resetRuleTables() {
        Boolean postgres = jdbcTemplate.execute((ConnectionCallback<Boolean>) connection -> {
            String db = connection.getMetaData().getDatabaseProductName();
            return db != null && db.toLowerCase().contains("postgresql");
        });
        Assumptions.assumeTrue(Boolean.TRUE.equals(postgres), "Skipping: tests require PostgreSQL runtime");

        jdbcTemplate.execute("DROP TABLE IF EXISTS " + MdrmConstants.DEFAULT_RULE_WARNINGS_TABLE);
        jdbcTemplate.execute("DROP TABLE IF EXISTS " + MdrmConstants.DEFAULT_RULE_DEPENDENCIES_TABLE);
        jdbcTemplate.execute("DROP TABLE IF EXISTS " + MdrmConstants.DEFAULT_RULES_TABLE);
        jdbcTemplate.execute("DROP TABLE IF EXISTS " + MdrmConstants.DEFAULT_RULE_LOADS_TABLE);
        rulesService.initializeSchema();
    }

    @Test
    void shouldLoadRulesWorkbookAndSupportSearch() {
        RuleLoadResult loadResult = rulesService.loadConfiguredRulesWorkbook();
        assertTrue(loadResult.loadedRules() > 0);
        assertTrue(loadResult.loadedDependencies() > 0);

        RuleSearchResponse response = rulesService.searchRules("BHCKC447", null, null, null, null, null);
        assertFalse(response.rules().isEmpty());

        RuleListResponse byMdrm = rulesService.getRulesByMdrm("BHCKC447", null);
        assertFalse(byMdrm.rules().isEmpty());
    }

    @Test
    void shouldReturnExpectedBhckb529Rules() {
        rulesService.loadConfiguredRulesWorkbook();

        RuleListResponse byMdrm = rulesService.getRulesByMdrm("BHCKB529", null);
        RuleListResponse byReport = rulesService.getRulesByReport("FR Y-9C", null, null);

        long directBhckb529Rules = byMdrm.rules().stream()
                .filter(rule -> "BHCKB529".equalsIgnoreCase(rule.primaryMdrmCode()))
                .count();
        long reportBhckb529Rules = byReport.rules().stream()
                .filter(rule -> "BHCKB529".equalsIgnoreCase(rule.primaryMdrmCode()))
                .count();

        assertEquals(2L, directBhckb529Rules);
        assertEquals(2L, reportBhckb529Rules);
    }
}
