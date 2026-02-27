package com.projectai.projectai.mdrm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Runs periodic MDRM ingestion based on cron expression in configuration.
 */
@Component
public class MdrmScheduler {

    private static final Logger log = LoggerFactory.getLogger(MdrmScheduler.class);

    private final MdrmLoadService mdrmLoadService;

    public MdrmScheduler(MdrmLoadService mdrmLoadService) {
        this.mdrmLoadService = mdrmLoadService;
    }

    /**
     * Scheduled entry point that performs one full MDRM load cycle.
     */
    @Scheduled(cron = "${mdrm.cron}")
    public void scheduledLoad() {
        MdrmLoadResult result = mdrmLoadService.loadFreshMigration();
        log.info("MDRM load complete. runs={}, rows={}, latestFile={}",
                result.runsProcessed(), result.loadedRows(), result.sourceFileName());
    }
}
