package com.nodeunify.jupiter.datastream.writer;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class Util {

    private static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static final TimeZone timeZone = TimeZone.getTimeZone("Asia/Shanghai");

    public static String getDateOfToday() {
        Date todayDate = new Date();
        dateFormat.setTimeZone(timeZone);
        return dateFormat.format(todayDate);
    }

}