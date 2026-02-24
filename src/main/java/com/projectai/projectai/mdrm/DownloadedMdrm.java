package com.projectai.projectai.mdrm;

/**
 * MDRM source payload with original filename and byte content.
 */
public record DownloadedMdrm(String fileName, byte[] content) {
}
