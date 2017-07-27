package com.alibaba.middleware.topkn.utils;

/**
 * Created by Shunjie Ding on 27/07/2017.
 */
public class LoggerFactory {

    public static Logger getLogger(Class clazz) {
        return new Logger(clazz.getName(), System.out);
    }
}
