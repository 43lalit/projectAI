package com.projectai.projectai;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ProjectAiApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProjectAiApplication.class, args);
    }
}
