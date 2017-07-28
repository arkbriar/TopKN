package com.alibaba.middleware.topkn.utils;

import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Shunjie Ding on 27/07/2017.
 */
public class Logger {
    private static final SimpleDateFormat timeFormat =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    PrintStream out;
    private String name;

    public Logger(String name, PrintStream out) {
        this.name = name;
        this.out = out;
    }

    public synchronized void info(String format, Object... args) {
        out.println("[" + timeFormat.format(new Date()) + "] INFO " + name + " "
            + String.format(format, args));
    }

    public synchronized void warn(String format, Object... args) {
        out.println("[" + timeFormat.format(new Date()) + "] WARN " + name + " "
            + String.format(format, args));
    }
}
