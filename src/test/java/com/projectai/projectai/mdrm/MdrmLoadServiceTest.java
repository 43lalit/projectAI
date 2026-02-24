package com.projectai.projectai.mdrm;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
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

@SpringBootTest
class MdrmLoadServiceTest {

    @Autowired
    private MdrmLoadService mdrmLoadService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TestMdrmDownloader mdrmDownloader;

    @Test
    void shouldRecreateStagingTableAndLoadFreshRows() throws IOException {
        mdrmDownloader.enqueue(new DownloadedMdrm("MDRM.zip", zip("MDRM.csv", List.of(
                "FILE_DATE,2026-02-24",
                "code,reporting form,description",
                "A,FFIEC 101,Alpha",
                "B,FFIEC 101,Beta",
                "C,FR Y-9C,Gamma"
        ))));
        mdrmDownloader.enqueue(new DownloadedMdrm("MDRM.zip", zip("MDRM.csv", List.of(
                "FILE_DATE,2026-02-25",
                "code,reporting form,description",
                "X,FFIEC 101,Xray"
        ))));

        MdrmLoadResult first = mdrmLoadService.loadLatestMdrm();
        assertEquals("MDRM.csv", first.sourceFileName());
        assertEquals(3, first.loadedRows());
        assertEquals(3, rowCount());

        MdrmLoadResult second = mdrmLoadService.loadLatestMdrm();
        assertEquals("MDRM.csv", second.sourceFileName());
        assertEquals(1, second.loadedRows());
        assertEquals(1, rowCount());

        String row = jdbcTemplate.queryForObject(
                "SELECT description FROM mdrm_staging ORDER BY code LIMIT 1",
                String.class
        );
        assertEquals("Xray", row);
    }

    @Test
    void shouldReturnReportingFormsAndFilteredRows() throws IOException {
        mdrmDownloader.enqueue(new DownloadedMdrm("MDRM.zip", zip("MDRM.csv", List.of(
                "FILE_DATE,2026-02-24",
                "code,reporting form,description",
                "A,FFIEC 101,Alpha",
                "B,FFIEC 101,Beta",
                "C,FR Y-9C,Gamma"
        ))));

        mdrmLoadService.loadLatestMdrm();

        List<String> forms = mdrmLoadService.getReportingForms();
        assertEquals(List.of("FFIEC 101", "FR Y-9C"), forms);

        MdrmTableResponse response = mdrmLoadService.getRowsByReportingForm("FFIEC 101");
        assertEquals(List.of("code", "reporting_form", "description"), response.columns());
        assertEquals(2, response.rows().size());

        Map<String, String> firstRow = response.rows().get(0);
        assertEquals("A", firstRow.get("code"));
        assertEquals("FFIEC 101", firstRow.get("reporting_form"));
        assertEquals("Alpha", firstRow.get("description"));
    }

    private int rowCount() {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM mdrm_staging", Integer.class);
        return count == null ? 0 : count;
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
