package com.projectai.projectai.mdrm;

/**
 * Abstraction for obtaining MDRM source file bytes from any backing store.
 */
public interface MdrmDownloader {

    /**
     * Downloads/reads MDRM source and returns file metadata plus raw bytes.
     */
    DownloadedMdrm download();
}
