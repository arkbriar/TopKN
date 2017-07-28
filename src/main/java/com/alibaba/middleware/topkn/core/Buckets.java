package com.alibaba.middleware.topkn.core;

import com.alibaba.middleware.topkn.Constants;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Created by Shunjie Ding on 27/07/2017.
 */
public class Buckets {
    private static Buckets instance = new Buckets();
    private AtomicIntegerArray bucketCounts = new AtomicIntegerArray(Constants.BUCKET_SIZE);

    private Buckets() {}

    public static Buckets getInstance() {
        return instance;
    }

    private static int getByteIndex(byte b) {
        return b <= '9' ? b - '0' : b - 'a' + 10;
    }

    public static int getBucketIndex(int len, byte first, byte second) {
        if (len == 1) {
            return getByteIndex(first);
        } else {
            return (len - 1) * 36 * 36 + getByteIndex(first) * 36 + getByteIndex(second);
        }
    }

    public void increaseBucketCount(int len, byte first, byte second) {
        int index = getBucketIndex(len, first, second);
        bucketCounts.incrementAndGet(index);
    }

    public void writeToBuffer(ByteBuffer buffer) {
        for (int i = 0; i < bucketCounts.length(); ++i) {
            buffer.putInt(bucketCounts.get(i));
        }
    }

    public void readFromBuffer(ByteBuffer buffer) {
        for (int i = 0; i < bucketCounts.length(); ++i) {
            int c = buffer.getInt();
            bucketCounts.set(i, c);
        }
    }

    public void clear() {
        bucketCounts = new AtomicIntegerArray(Constants.BUCKET_SIZE);
    }
}
