package com.projectai.projectai.mdrm;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;
import org.springframework.web.util.HtmlUtils;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class ReportMetadataService {

    private static final String REPORT_BASE_URL = "https://www.federalreserve.gov/apps/reportingforms/Report/Index/";
    private static final Pattern DESCRIPTION_SECTION_PATTERN = Pattern.compile(
            "(?is)<h[1-6][^>]*>\\s*Description\\s+of\\s+the\\s+report\\s*</h[1-6]>(.*?)(<h[1-6][^>]*>|</section>|</article>|</main>)"
    );
    private static final Pattern GENERIC_DESCRIPTION_HEADING_PATTERN = Pattern.compile(
            "(?is)<h[1-6][^>]*>\\s*Description\\s*</h[1-6]>(.*?)(<h[1-6][^>]*>|</section>|</article>|</main>)"
    );
    private static final Pattern DESCRIPTION_BETWEEN_INSTRUCTIONS_AND_OMB_PATTERN = Pattern.compile(
            "(?is)Instructions\\s*:?\\s*.*?Description\\s*:?\\s*(.*?)\\s*OMB\\s+Control\\s+Number\\s*:"
    );
    private static final Pattern DESCRIPTION_TO_OMB_PATTERN = Pattern.compile(
            "(?is)Description\\s*:?\\s*(.*?)\\s*OMB\\s+Control\\s+Number\\s*:"
    );
    private static final Pattern META_DESCRIPTION_PATTERN = Pattern.compile(
            "(?is)<meta\\s+name=[\"']description[\"']\\s+content=[\"'](.*?)[\"']\\s*/?>"
    );
    private static final Pattern TITLE_TAG_PATTERN = Pattern.compile(
            "(?is)<title[^>]*>(.*?)</title>"
    );
    private static final Pattern HEADING_TAG_PATTERN = Pattern.compile(
            "(?is)<h[1-6][^>]*>(.*?)</h[1-6]>"
    );
    private static final Pattern REPORT_TITLE_BETWEEN_SERIES_AND_DESCRIPTION_PATTERN = Pattern.compile(
            "(?is)Report\\s+Series\\s*:?\\s*(.*?)\\s+Report\\s+Title\\s*:?\\s*(.*?)\\s+Description\\s*:"
    );
    private static final Pattern TAG_PATTERN = Pattern.compile("(?is)<[^>]+>");
    private static final Pattern SCRIPT_STYLE_PATTERN = Pattern.compile("(?is)<(script|style)[^>]*>.*?</\\1>");

    private final JdbcTemplate jdbcTemplate;
    private final HttpClient httpClient;

    public ReportMetadataService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
        ensureMetadataTable();
    }

    public ReportMetadataResponse getOrFetch(String reportSeries) {
        String normalizedReport = normalizeReportSeries(reportSeries);
        ReportMetadataResponse existing = findByReportSeries(normalizedReport);
        if (existing != null
                && existing.description() != null && !existing.description().isBlank()
                && isValidFullName(existing.fullName())) {
            return existing;
        }

        String sourceUrl = buildReportUrl(normalizedReport);
        ParsedMetadata parsedMetadata = fetchMetadata(sourceUrl, normalizedReport);
        long now = System.currentTimeMillis();

        upsert(normalizedReport, parsedMetadata.fullName(), parsedMetadata.description(), sourceUrl, now);
        return new ReportMetadataResponse(normalizedReport, parsedMetadata.fullName(), parsedMetadata.description(), sourceUrl, now);
    }

    public ReportMetadataResponse refresh(String reportSeries) {
        String normalizedReport = normalizeReportSeries(reportSeries);
        String sourceUrl = buildReportUrl(normalizedReport);
        ParsedMetadata parsedMetadata = fetchMetadata(sourceUrl, normalizedReport);
        long now = System.currentTimeMillis();
        upsert(normalizedReport, parsedMetadata.fullName(), parsedMetadata.description(), sourceUrl, now);
        return new ReportMetadataResponse(normalizedReport, parsedMetadata.fullName(), parsedMetadata.description(), sourceUrl, now);
    }

    private ReportMetadataResponse findByReportSeries(String reportSeries) {
        try {
            return jdbcTemplate.queryForObject(
                    "SELECT report_series, full_name, description, source_url, last_fetched_at FROM report_metadata WHERE report_series = ?",
                    (rs, rowNum) -> new ReportMetadataResponse(
                            rs.getString("report_series"),
                            rs.getString("full_name"),
                            rs.getString("description"),
                            rs.getString("source_url"),
                            rs.getObject("last_fetched_at") == null ? null : rs.getLong("last_fetched_at")
                    ),
                    reportSeries
            );
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }

    private void upsert(String reportSeries, String fullName, String description, String sourceUrl, long now) {
        jdbcTemplate.update("""
                INSERT INTO report_metadata (report_series, full_name, description, source_url, last_fetched_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (report_series)
                DO UPDATE SET
                    full_name = EXCLUDED.full_name,
                    description = EXCLUDED.description,
                    source_url = EXCLUDED.source_url,
                    last_fetched_at = EXCLUDED.last_fetched_at,
                    updated_at = EXCLUDED.updated_at
                """,
                reportSeries, fullName, description, sourceUrl, now, now, now
        );
    }

    private ParsedMetadata fetchMetadata(String sourceUrl, String reportSeries) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(sourceUrl))
                    .timeout(Duration.ofSeconds(20))
                    .header("User-Agent", "ProjectAI/1.0 (MDRM metadata fetch)")
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new ResponseStatusException(
                        HttpStatus.BAD_GATEWAY,
                        "Unable to fetch report page. HTTP " + response.statusCode()
                );
            }
            String body = response.body();
            String fullName = extractFullName(body, reportSeries);
            String description = extractDescription(body, fullName);
            return new ParsedMetadata(fullName, description);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "Unable to fetch report description", ex);
        } catch (IOException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "Unable to fetch report description", ex);
        }
    }

    private String extractDescription(String html, String fullName) {
        String withoutScripts = SCRIPT_STYLE_PATTERN.matcher(html == null ? "" : html).replaceAll(" ");

        String flattened = cleanText(withoutScripts);
        String markerBased = extractDescriptionUsingStopMarkers(flattened, fullName);
        if (!markerBased.isBlank()) {
            return markerBased;
        }

        Matcher betweenInstructionsAndOmb = DESCRIPTION_BETWEEN_INSTRUCTIONS_AND_OMB_PATTERN.matcher(flattened);
        if (betweenInstructionsAndOmb.find()) {
            String cleaned = cleanText(betweenInstructionsAndOmb.group(1));
            if (!cleaned.isBlank()) {
                return cleaned;
            }
        }

        Matcher descriptionToOmb = DESCRIPTION_TO_OMB_PATTERN.matcher(flattened);
        if (descriptionToOmb.find()) {
            String cleaned = cleanText(descriptionToOmb.group(1));
            if (!cleaned.isBlank()) {
                return cleaned;
            }
        }

        Matcher sectionMatcher = DESCRIPTION_SECTION_PATTERN.matcher(withoutScripts);
        if (sectionMatcher.find()) {
            String cleaned = cleanText(sectionMatcher.group(1));
            if (!cleaned.isBlank()) {
                return cleaned;
            }
        }

        Matcher genericSectionMatcher = GENERIC_DESCRIPTION_HEADING_PATTERN.matcher(withoutScripts);
        if (genericSectionMatcher.find()) {
            String cleaned = cleanText(genericSectionMatcher.group(1));
            if (!cleaned.isBlank()) {
                return cleaned;
            }
        }

        Matcher metaMatcher = META_DESCRIPTION_PATTERN.matcher(withoutScripts);
        if (metaMatcher.find()) {
            String cleaned = cleanText(metaMatcher.group(1));
            if (!cleaned.isBlank()) {
                return cleaned;
            }
        }

        throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "Description of the report was not found on source page");
    }

    private String extractDescriptionUsingStopMarkers(String flattened, String fullName) {
        String haystack = flattened == null ? "" : flattened;
        if (haystack.isBlank()) {
            return "";
        }
        String upperHaystack = haystack.toUpperCase();
        int fromIndex = 0;
        while (true) {
            int descriptionIndex = upperHaystack.indexOf("DESCRIPTION:", fromIndex);
            if (descriptionIndex < 0) {
                return "";
            }
            int start = descriptionIndex + "DESCRIPTION:".length();
            if (start >= haystack.length()) {
                return "";
            }
            String tail = haystack.substring(start);
            int stop = firstDescriptionStopMarkerOffset(tail, fullName);
            String candidate = stop > 0 ? tail.substring(0, stop) : tail;
            String cleaned = cleanText(candidate);
            if (!cleaned.isBlank()) {
                return cleaned;
            }
            fromIndex = start;
        }
    }

    private int firstDescriptionStopMarkerOffset(String tail, String fullName) {
        String upperTail = tail == null ? "" : tail.toUpperCase();
        int best = -1;
        int ombIndex = upperTail.indexOf("OMB CONTROL NUMBER:");
        if (ombIndex >= 0) {
            best = ombIndex;
        }
        int reportSeriesIndex = upperTail.indexOf("REPORT SERIES:");
        if (reportSeriesIndex >= 0 && (best < 0 || reportSeriesIndex < best)) {
            best = reportSeriesIndex;
        }
        int formIndex = upperTail.indexOf("FORM:");
        if (formIndex >= 0 && (best < 0 || formIndex < best)) {
            best = formIndex;
        }
        int instructionsIndex = upperTail.indexOf("INSTRUCTIONS:");
        if (instructionsIndex >= 0 && (best < 0 || instructionsIndex < best)) {
            best = instructionsIndex;
        }
        String normalizedFullName = cleanText(fullName);
        if (!normalizedFullName.isBlank()) {
            int fullNameIndex = upperTail.indexOf(normalizedFullName.toUpperCase());
            if (fullNameIndex >= 0 && (best < 0 || fullNameIndex < best)) {
                best = fullNameIndex;
            }
        }
        return best;
    }

    private String extractFullName(String html, String reportSeries) {
        String withoutScripts = SCRIPT_STYLE_PATTERN.matcher(html == null ? "" : html).replaceAll(" ");
        String normalizedSeries = normalizeLooseText(reportSeries);
        String flattened = cleanText(withoutScripts);

        String markerBased = extractNameUsingStopMarkers(flattened, reportSeries);
        if (!markerBased.isBlank()) {
            return markerBased;
        }

        Pattern nameInMetadataBlockPattern = Pattern.compile(
                "(?is)" + Pattern.quote(reportSeries) + "\\s+(.*?)\\s+Form\\s*:.*?\\s+Instructions\\s*:.*?\\s+Description\\s*:"
        );
        Matcher nameInMetadataBlockMatcher = nameInMetadataBlockPattern.matcher(flattened);
        String bestInMetadataBlock = "";
        while (nameInMetadataBlockMatcher.find()) {
            String cleaned = cleanFullNameCandidate(nameInMetadataBlockMatcher.group(1), reportSeries);
            if (!cleaned.isBlank()) {
                bestInMetadataBlock = cleaned;
            }
        }
        if (!bestInMetadataBlock.isBlank()) {
            return bestInMetadataBlock;
        }

        Pattern seriesThenFormPattern = Pattern.compile(
                "(?is)" + Pattern.quote(reportSeries) + "\\s+(.*?)\\s+Form\\s*:"
        );
        Matcher seriesThenFormMatcher = seriesThenFormPattern.matcher(flattened);
        String bestBetweenSeriesAndForm = "";
        while (seriesThenFormMatcher.find()) {
            String cleaned = cleanFullNameCandidate(seriesThenFormMatcher.group(1), reportSeries);
            if (!cleaned.isBlank()) {
                bestBetweenSeriesAndForm = cleaned;
            }
        }
        if (!bestBetweenSeriesAndForm.isBlank()) {
            return bestBetweenSeriesAndForm;
        }

        Matcher headingMatcher = HEADING_TAG_PATTERN.matcher(withoutScripts);
        String previousHeading = null;
        while (headingMatcher.find()) {
            String heading = cleanText(headingMatcher.group(1));
            if (heading.isBlank()) {
                continue;
            }
            if (previousHeading != null && normalizeLooseText(previousHeading).equals(normalizedSeries)) {
                String cleaned = cleanFullNameCandidate(heading, reportSeries);
                if (!cleaned.isBlank()) {
                    return cleaned;
                }
            }
            previousHeading = heading;
        }

        Matcher betweenSeriesAndDescription = REPORT_TITLE_BETWEEN_SERIES_AND_DESCRIPTION_PATTERN.matcher(flattened);
        while (betweenSeriesAndDescription.find()) {
            String series = cleanText(betweenSeriesAndDescription.group(1));
            String title = cleanText(betweenSeriesAndDescription.group(2));
            if (normalizeLooseText(series).equals(normalizedSeries)) {
                String cleaned = cleanFullNameCandidate(title, reportSeries);
                if (!cleaned.isBlank()) {
                    return cleaned;
                }
            }
        }

        Matcher titleMatcher = TITLE_TAG_PATTERN.matcher(withoutScripts);
        if (titleMatcher.find()) {
            String titleText = cleanText(titleMatcher.group(1));
            String cleaned = cleanFullNameCandidate(titleText, reportSeries);
            if (!cleaned.isBlank()) {
                return cleaned;
            }
        }

        return "";
    }

    private String extractNameUsingStopMarkers(String flattened, String reportSeries) {
        String haystack = flattened == null ? "" : flattened;
        if (haystack.isBlank()) {
            return "";
        }

        String series = reportSeries == null ? "" : reportSeries.trim();
        if (series.isBlank()) {
            return "";
        }

        String haystackUpper = haystack.toUpperCase();
        String seriesUpper = series.toUpperCase();
        int fromIndex = 0;
        while (true) {
            int seriesIndex = haystackUpper.indexOf(seriesUpper, fromIndex);
            if (seriesIndex < 0) {
                return "";
            }
            int nameStart = seriesIndex + seriesUpper.length();
            if (nameStart >= haystack.length()) {
                return "";
            }
            String tail = haystack.substring(nameStart);
            int stopOffset = firstStopMarkerOffset(tail);
            if (stopOffset > 0) {
                String cleaned = cleanFullNameCandidate(tail.substring(0, stopOffset), reportSeries);
                if (!cleaned.isBlank()) {
                    return cleaned;
                }
            }
            fromIndex = nameStart;
        }
    }

    private int firstStopMarkerOffset(String tail) {
        String upperTail = tail == null ? "" : tail.toUpperCase();
        int best = -1;
        int formIndex = upperTail.indexOf("FORM:");
        if (formIndex >= 0) {
            best = formIndex;
        }
        int instructionsIndex = upperTail.indexOf("INSTRUCTIONS:");
        if (instructionsIndex >= 0 && (best < 0 || instructionsIndex < best)) {
            best = instructionsIndex;
        }
        int descriptionIndex = upperTail.indexOf("DESCRIPTION:");
        if (descriptionIndex >= 0 && (best < 0 || descriptionIndex < best)) {
            best = descriptionIndex;
        }
        return best;
    }

    private String cleanFullNameCandidate(String candidate, String reportSeries) {
        String value = cleanText(candidate);
        if (value.isBlank()) {
            return "";
        }
        String normalizedSeries = normalizeLooseText(reportSeries);
        String normalizedValue = normalizeLooseText(value);
        if (normalizedValue.equals(normalizedSeries)) {
            return "";
        }

        String upperValue = value.toUpperCase();
        String upperSeries = reportSeries == null ? "" : reportSeries.toUpperCase();
        if ((upperValue.startsWith(upperSeries + " - ") || upperValue.startsWith(upperSeries + ": "))
                && value.length() > reportSeries.length() + 3) {
            value = value.substring(reportSeries.length() + 3).trim();
        }

        String upperCleaned = value.toUpperCase();
        if (upperCleaned.equals("FEDERAL RESERVE BOARD - REPORTING FORMS")
                || upperCleaned.equals("REPORTING FORMS")) {
            return "";
        }
        if (upperCleaned.startsWith("DESCRIPTION")
                || upperCleaned.startsWith("INSTRUCTIONS")
                || upperCleaned.startsWith("OMB CONTROL NUMBER")) {
            return "";
        }
        return value;
    }

    private boolean isValidFullName(String fullName) {
        String cleaned = cleanFullNameCandidate(fullName, "");
        return !cleaned.isBlank();
    }

    private String normalizeLooseText(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().replaceAll("\\s+", " ").toUpperCase();
    }

    private String cleanText(String raw) {
        String noTags = TAG_PATTERN.matcher(raw == null ? "" : raw).replaceAll(" ");
        String unescaped = HtmlUtils.htmlUnescape(noTags);
        return unescaped.replace('\u00A0', ' ').replaceAll("\\s+", " ").trim();
    }

    private String buildReportUrl(String reportSeries) {
        String slug = reportSeries.trim()
                .replace('/', '_')
                .replaceAll("\\s+", "_");
        return REPORT_BASE_URL + slug;
    }

    private String normalizeReportSeries(String reportSeries) {
        String value = reportSeries == null ? "" : reportSeries.trim().replaceAll("\\s+", " ");
        if (value.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "reportingForm is required");
        }
        return value.toUpperCase();
    }

    private void ensureMetadataTable() {
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS report_metadata (
                    report_series TEXT PRIMARY KEY,
                    full_name TEXT,
                    description TEXT,
                    source_url TEXT NOT NULL,
                    last_fetched_at BIGINT NULL,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL
                )
                """);
        jdbcTemplate.execute("ALTER TABLE report_metadata ADD COLUMN IF NOT EXISTS full_name TEXT");
    }

    private record ParsedMetadata(String fullName, String description) {
    }
}
