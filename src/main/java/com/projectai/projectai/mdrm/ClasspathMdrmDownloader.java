package com.projectai.projectai.mdrm;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Loads MDRM source content from a classpath resource configured via {@code mdrm.local-file-path}.
 */
@Component
public class ClasspathMdrmDownloader implements MdrmDownloader {

    private static final Pattern MDRM_FILE_PATTERN = Pattern.compile("^MDRM_(\\d{2})(\\d{2})\\.csv$");

    private final MdrmProperties mdrmProperties;

    public ClasspathMdrmDownloader(MdrmProperties mdrmProperties) {
        this.mdrmProperties = mdrmProperties;
    }

    /**
     * Reads the configured MDRM file bytes and returns them with file metadata.
     */
    @Override
    public DownloadedMdrm download() {
        String path = mdrmProperties.getLocalFilePath();
        ClassPathResource resource = new ClassPathResource(path);
        if (!resource.exists()) {
            throw new IllegalStateException(MdrmConstants.MSG_RESOURCE_NOT_FOUND.formatted(path));
        }

        try {
            byte[] content = resource.getInputStream().readAllBytes();
            if (content.length == 0) {
                throw new IllegalStateException(MdrmConstants.MSG_RESOURCE_EMPTY.formatted(path));
            }
            return new DownloadedMdrm(resource.getFilename(), content);
        } catch (IOException ex) {
            throw new IllegalStateException(MdrmConstants.MSG_RESOURCE_READ_FAILED.formatted(path), ex);
        }
    }

    @Override
    public List<DownloadedMdrm> downloadAllForMigration() {
        try {
            PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
            Resource[] resources = resolver.getResources("classpath*:" + mdrmProperties.getMigrationFilePattern());

            List<Resource> valid = new ArrayList<>();
            for (Resource resource : resources) {
                if (resource == null || !resource.exists()) {
                    continue;
                }
                String name = resource.getFilename();
                if (name != null && MDRM_FILE_PATTERN.matcher(name).matches()) {
                    valid.add(resource);
                }
            }

            valid.sort(Comparator.comparing(this::sortKeyFromResource));

            List<DownloadedMdrm> downloaded = new ArrayList<>(valid.size());
            for (Resource resource : valid) {
                String name = resource.getFilename();
                byte[] content = resource.getInputStream().readAllBytes();
                if (content.length == 0) {
                    throw new IllegalStateException(MdrmConstants.MSG_RESOURCE_EMPTY.formatted(name));
                }
                downloaded.add(new DownloadedMdrm(name, content));
            }

            if (downloaded.isEmpty()) {
                throw new IllegalStateException(
                        "No MDRM files found matching mmyy convention in: " + mdrmProperties.getMigrationFilePattern()
                );
            }

            return downloaded;
        } catch (IOException ex) {
            throw new IllegalStateException(
                    "Failed to load MDRM migration files from pattern: " + mdrmProperties.getMigrationFilePattern(),
                    ex
            );
        }
    }

    private String sortKeyFromResource(Resource resource) {
        String fileName = resource.getFilename();
        if (fileName == null) {
            return "";
        }
        Matcher matcher = MDRM_FILE_PATTERN.matcher(fileName);
        if (!matcher.matches()) {
            return "";
        }
        int month = Integer.parseInt(matcher.group(1));
        int year = 2000 + Integer.parseInt(matcher.group(2));
        return "%04d-%02d-%s".formatted(year, month, fileName);
    }
}
