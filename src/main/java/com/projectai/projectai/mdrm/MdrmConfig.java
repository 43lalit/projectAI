package com.projectai.projectai.mdrm;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Enables binding of MDRM-specific configuration properties.
 */
@Configuration
@EnableConfigurationProperties(MdrmProperties.class)
public class MdrmConfig {
}
