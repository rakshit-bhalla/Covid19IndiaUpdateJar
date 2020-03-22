package com.rakshit.covid19indiaupdate;

import com.google.common.base.Stopwatch;
import com.rakshit.covid19indiaupdate.service.JarExecutor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@SpringBootApplication
public class Covid19IndiaUpdateApplication implements CommandLineRunner {

    @Autowired
    private JarExecutor jarExecutor;

    public static void main(String[] args) {
        new SpringApplicationBuilder(Covid19IndiaUpdateApplication.class)
                .contextClass(AnnotationConfigApplicationContext.class)
                .run(args);
        Runtime.getRuntime().exit(0);
    }

    @Override
    public void run(String... args) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        jarExecutor.execute();
        logStats(stopwatch.stop());
    }

    private void logStats(Stopwatch stopwatch) {
        Map<String, Object> statsMap = new HashMap<>();
        statsMap.put("Duration", stopwatch.toString());
        statsMap.forEach((stat, v) -> log.info("{} : {}", stat, v));
    }
}