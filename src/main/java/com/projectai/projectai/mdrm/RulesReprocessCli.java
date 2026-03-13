package com.projectai.projectai.mdrm;

import com.projectai.projectai.ProjectAiApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * One-shot CLI entrypoint to clear and reload rules data using the configured datasource.
 */
public final class RulesReprocessCli {

    private RulesReprocessCli() {
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext context = new SpringApplicationBuilder(ProjectAiApplication.class)
                .web(WebApplicationType.NONE)
                .run(args);
        int exitCode = 0;
        try {
            RuleLoadResult result = context.getBean(RulesService.class).reprocessConfiguredRulesWorkbook();
            System.out.printf(
                    "Rules reprocessed successfully | rules=%d | dependencies=%d | warnings=%d | loadId=%d%n",
                    result.loadedRules(),
                    result.loadedDependencies(),
                    result.warningCount(),
                    result.loadId()
            );
        } catch (RuntimeException ex) {
            exitCode = 1;
            ex.printStackTrace(System.err);
        } finally {
            context.close();
        }
        System.exit(exitCode);
    }
}
