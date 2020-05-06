/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package com.nodeunify.jupiter.datastream.writer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
public class App {

    public static void main(String[] args) {
        log.info("Start running Datastream Writer");
        SpringApplication.run(App.class, args);
    }
}
