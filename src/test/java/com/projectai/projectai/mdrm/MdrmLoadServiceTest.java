package com.projectai.projectai.mdrm;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Assumptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class MdrmLoadServiceTest {

    @Autowired
    private MdrmLoadService mdrmLoadService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TestMdrmDownloader mdrmDownloader;

    @BeforeEach
    void resetTables() {
        Boolean postgres = jdbcTemplate.execute((ConnectionCallback<Boolean>) connection -> {
            String db = connection.getMetaData().getDatabaseProductName();
            return db != null && db.toLowerCase().contains("postgresql");
        });
        Assumptions.assumeTrue(Boolean.TRUE.equals(postgres), "Skipping: tests require PostgreSQL runtime");

        jdbcTemplate.execute("DROP TABLE IF EXISTS mdrm_master");
        jdbcTemplate.execute("DROP TABLE IF EXISTS mdrm_staging");
        jdbcTemplate.execute("DROP TABLE IF EXISTS mdrm_run_error");
        jdbcTemplate.execute("DROP TABLE IF EXISTS mdrm_run_master");
    }

    @Test
    void shouldCreateRunAndLoadMasterWithRunId() throws IOException {
        mdrmDownloader.enqueue(new DownloadedMdrm("MDRM.zip", zip("MDRM.csv", List.of(
                "FILE_DATE,2026-02-24",
                "mnemonic,item code,reporting form,description,start date,end date",
                "AAA,111,FFIEC 101,Alpha,01/01/2020 12:00:00 AM,12/31/9999 12:00:00 AM",
                "BBB,222,FFIEC 101,Beta,01/01/2020 12:00:00 AM,01/01/2021 12:00:00 AM"
        ))));

        MdrmLoadResult result = mdrmLoadService.loadLatestMdrm();
        assertEquals("MDRM.csv", result.sourceFileName());
        assertEquals(2, result.loadedRows());

        Integer runCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM mdrm_run_master", Integer.class);
        assertEquals(1, runCount);

        Integer stagingCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM mdrm_staging", Integer.class);
        assertEquals(2, stagingCount);

        Integer masterCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM mdrm_master", Integer.class);
        assertEquals(2, masterCount);

        String runIdFromStaging = jdbcTemplate.queryForObject("SELECT CAST(run_id AS VARCHAR) FROM mdrm_staging LIMIT 1", String.class);
        String runIdFromMaster = jdbcTemplate.queryForObject("SELECT CAST(run_id AS VARCHAR) FROM mdrm_master LIMIT 1", String.class);
        assertEquals(runIdFromStaging, runIdFromMaster);

        String activeFlag = jdbcTemplate.queryForObject(
                "SELECT is_active FROM mdrm_master WHERE mdrm_code = 'AAA111'",
                String.class
        );
        assertEquals("Y", activeFlag);
    }

    @Test
    void shouldLogErrorsForInvalidOrNullDates() throws IOException {
        mdrmDownloader.enqueue(new DownloadedMdrm("MDRM.zip", zip("MDRM.csv", List.of(
                "FILE_DATE,2026-02-24",
                "mnemonic,item code,reporting form,description,start date,end date",
                "AAA,111,FFIEC 101,Alpha,invalid date,12/31/9999 12:00:00 AM",
                "BBB,222,FFIEC 101,Beta,01/01/2020 12:00:00 AM,",
                "CCC,333,FFIEC 101,Gamma,01/01/2020 12:00:00 AM,12/31/9999 12:00:00 AM"
        ))));

        MdrmLoadResult result = mdrmLoadService.loadLatestMdrm();
        assertEquals(1, result.loadedRows());

        Integer masterCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM mdrm_master", Integer.class);
        assertEquals(1, masterCount);

        Integer errorCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM mdrm_run_error", Integer.class);
        assertEquals(2, errorCount);

        String errorText = jdbcTemplate.queryForObject("SELECT error_description FROM mdrm_run_error LIMIT 1", String.class);
        assertTrue(errorText.contains("Invalid date format") || errorText.contains("Date value is null/blank"));
    }

    @Test
    void shouldReturnReportingFormsAndFilteredRowsFromMaster() throws IOException {
        mdrmDownloader.enqueue(new DownloadedMdrm("MDRM.zip", zip("MDRM.csv", List.of(
                "FILE_DATE,2026-02-24",
                "mnemonic,item code,reporting form,description,start date,end date",
                "AAA,111,FFIEC 101,Alpha,01/01/2020 12:00:00 AM,12/31/9999 12:00:00 AM",
                "BBB,222,FFIEC 101,Beta,01/01/2020 12:00:00 AM,12/31/9999 12:00:00 AM",
                "CCC,333,FR Y-9C,Gamma,01/01/2020 12:00:00 AM,12/31/9999 12:00:00 AM"
        ))));

        mdrmLoadService.loadLatestMdrm();

        List<String> forms = mdrmLoadService.getReportingForms();
        assertEquals(List.of("FFIEC 101", "FR Y-9C"), forms);

        MdrmTableResponse response = mdrmLoadService.getRowsByReportingForm("FFIEC 101");
        assertTrue(response.columns().contains("mdrm_code"));
        assertTrue(response.columns().contains("is_active"));
        assertEquals(2, response.rows().size());

        Map<String, String> firstRow = response.rows().get(0);
        assertEquals("FFIEC 101", firstRow.get("reporting_form"));
    }

    @Test
    void shouldPersistRunMasterEntryEvenWhenLoadFails() throws IOException {
        mdrmDownloader.enqueue(new DownloadedMdrm("MDRM.zip", zip("MDRM.csv", List.of(
                "FILE_DATE,2026-02-24"
        ))));

        assertThrows(IllegalStateException.class, () -> mdrmLoadService.loadLatestMdrm());

        Integer runCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM mdrm_run_master", Integer.class);
        assertEquals(1, runCount);

        Integer ingested = jdbcTemplate.queryForObject("SELECT num_records_ingested FROM mdrm_run_master LIMIT 1", Integer.class);
        Integer errors = jdbcTemplate.queryForObject("SELECT num_records_error FROM mdrm_run_master LIMIT 1", Integer.class);
        assertEquals(0, ingested);
        assertEquals(0, errors);
    }

    private byte[] zip(String fileName, List<String> lines) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ZipOutputStream zos = new ZipOutputStream(out, StandardCharsets.UTF_8)) {
            zos.putNextEntry(new ZipEntry(fileName));
            String payload = String.join("\n", lines) + "\n";
            zos.write(payload.getBytes(StandardCharsets.UTF_8));
            zos.closeEntry();
        }
        return out.toByteArray();
    }

    @TestConfiguration
    static class TestConfig {
        @Bean
        @Primary
        TestMdrmDownloader testMdrmDownloader() {
            return new TestMdrmDownloader();
        }
    }

    static class TestMdrmDownloader implements MdrmDownloader {
        private final Queue<DownloadedMdrm> queue = new ArrayDeque<>();

        void enqueue(DownloadedMdrm downloadedMdrm) {
            queue.add(downloadedMdrm);
        }

        @Override
        public DownloadedMdrm download() {
            DownloadedMdrm next = queue.poll();
            if (next == null) {
                throw new IllegalStateException("No test MDRM payload enqueued");
            }
            return next;
        }
    }
}
