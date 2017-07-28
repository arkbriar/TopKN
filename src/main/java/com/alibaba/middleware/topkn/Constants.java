package com.alibaba.middleware.topkn;

/**
 * Created by Shunjie Ding on 27/07/2017.
 */
public class Constants {
    public static final int BUCKET_SIZE = 128 * 36 * 36;

    public static final int NUM_CORES = 24;

    public static final int SEGMENT_SIZE = 48 * 1024 * 1024;

    public static final int RESULT_BUFFER_SIZE = 5 * 1024 * 1024;

    public static final int OP_INDEX = 0;
    public static final int OP_QUERY = 1;
}
