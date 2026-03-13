package com.projectai.projectai.mdrm;

import com.projectai.projectai.ProjectAiApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * One-shot CLI entrypoint to recreate the rule SQL helper functions against the configured datasource.
 */
public final class RulesReloadFunctionsCli {

    private RulesReloadFunctionsCli() {
    }

    public static void main(String[] args) {
        ConfigurableApplicationContext context = new SpringApplicationBuilder(ProjectAiApplication.class)
                .web(WebApplicationType.NONE)
                .run(args);
        int exitCode = 0;
        try {
            context.getBean(RulesService.class).reloadSqlFunctions();
            System.out.println("Rule SQL functions reloaded successfully.");
        } catch (RuntimeException ex) {
            exitCode = 1;
            ex.printStackTrace(System.err);
        } finally {
            context.close();
        }
        System.exit(exitCode);
    }
}
