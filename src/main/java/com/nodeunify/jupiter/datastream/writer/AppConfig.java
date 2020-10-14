package com.nodeunify.jupiter.datastream.writer;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;

import org.apache.parquet.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class AppConfig {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int MIDNIGHT = 0;
    private static final int BEFORE_DAY_MARKET = 8 * 60;    // 08:00:00
    private static final int AFTER_DAY_MARKET = 16 * 60;    // 16:00:00
    private static final int BEFORE_NIGHT_MARKET = 20 * 60; // 20:00:00
    private static final int AFTER_NIGHT_MARKET = 3 * 60;   // 03:00:00

    private LocalDateTime now = LocalDateTime.now();
    private int minutesOfDayNow = now.get(ChronoField.MINUTE_OF_DAY);

    @Value("${app.time-period:day}")
    private String timePeriod;
    @Value("${app.start-time:#{null}}")
    private String startTime;
    @Value("${app.end-time:#{null}}")
    private String endTime;

    private LocalDateTime getStartTimeFromTimePeriod() {
        switch (timePeriod) {
            case "day":
                if (minutesOfDayNow < AFTER_DAY_MARKET) {
                    return now.minusDays(1).with(ChronoField.MINUTE_OF_DAY, BEFORE_DAY_MARKET);
                } else {
                    return now.with(ChronoField.MINUTE_OF_DAY, BEFORE_DAY_MARKET);
                }
            case "night":
                if (minutesOfDayNow < AFTER_NIGHT_MARKET) {
                    return now.minusDays(2).with(ChronoField.MINUTE_OF_DAY, BEFORE_NIGHT_MARKET);
                } else {
                    return now.minusDays(1).with(ChronoField.MINUTE_OF_DAY, BEFORE_NIGHT_MARKET);
                }
            case "naturalDay":
                return now.minusDays(1).with(ChronoField.MINUTE_OF_DAY, MIDNIGHT);
            case "tradingDay":
                if (minutesOfDayNow > AFTER_DAY_MARKET ) {
                    return now.minusDays(1).with(ChronoField.MINUTE_OF_DAY, BEFORE_NIGHT_MARKET);
                } else {
                    return now.minusDays(2).with(ChronoField.MINUTE_OF_DAY, BEFORE_NIGHT_MARKET);
                }
            default:
                return now;
        }
    }

    private LocalDateTime getEndTimeFromTimePeriod() {
        switch (timePeriod) {
            case "day":
            if (minutesOfDayNow < AFTER_DAY_MARKET) {
                return now.minusDays(1).with(ChronoField.MINUTE_OF_DAY, AFTER_DAY_MARKET);
            } else {
                return now.with(ChronoField.MINUTE_OF_DAY, AFTER_DAY_MARKET);
            }
            case "night":
                if (minutesOfDayNow < AFTER_NIGHT_MARKET) {
                    return now.minusDays(1).with(ChronoField.MINUTE_OF_DAY, AFTER_NIGHT_MARKET);
                } else {
                    return now.with(ChronoField.MINUTE_OF_DAY, AFTER_NIGHT_MARKET);
                }
            case "naturalDay":
                return now.with(ChronoField.MINUTE_OF_DAY, MIDNIGHT);
            case "tradingDay":
                if (minutesOfDayNow > AFTER_DAY_MARKET ) {
                    return now.with(ChronoField.MINUTE_OF_DAY, AFTER_DAY_MARKET);
                } else {
                    return now.minusDays(1).with(ChronoField.MINUTE_OF_DAY, AFTER_DAY_MARKET);
                }
            default:
                return now;
        }
    }
    
    @Bean
    public LocalDateTime getStartTime() {
        LocalDateTime dateTime;
        if (Strings.isNullOrEmpty(startTime)) {
            dateTime = getStartTimeFromTimePeriod();
        } else {
            dateTime = LocalDateTime.parse(startTime, DATE_TIME_FORMATTER);
        }
        log.debug("getStartTime: {}, {}", startTime, dateTime);
        return dateTime;
    }

    @Bean
    public LocalDateTime getEndTime() {
        LocalDateTime dateTime;
        if (Strings.isNullOrEmpty(endTime)) {
            dateTime = getEndTimeFromTimePeriod();
        } else {
            dateTime = LocalDateTime.parse(endTime, DATE_TIME_FORMATTER);
        }
        log.debug("getEndTime: {}, {}", endTime, dateTime);
        return dateTime;
    }
}
