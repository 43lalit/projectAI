package com.projectai.projectai.mdrm;

import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Loads MDRM source content from a classpath resource configured via {@code mdrm.local-file-path}.
 */
@Component
public class ClasspathMdrmDownloader implements MdrmDownloader {

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
}
