package com.projectai.projectai.mdrm;

import java.util.List;

/**
 * Abstraction for obtaining MDRM source file bytes from any backing store.
 */
public interface MdrmDownloader {

    /**
     * Downloads/reads MDRM source and returns file metadata plus raw bytes.
     */
    DownloadedMdrm download();

    /**
     * Downloads all MDRM files that should be processed in one migration run.
     * Default behavior falls back to single-file download for existing implementations.
     */
    default List<DownloadedMdrm> downloadAllForMigration() {
        return List.of(download());
    }
}
