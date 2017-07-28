package com.alibaba.middleware.topkn.core;

import com.alibaba.middleware.topkn.Constants;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Created by Shunjie Ding on 27/07/2017.
 */
public class Bucket {
    private static Bucket global = new Bucket();
    private static HashMap<Object, Bucket> bucketPool = new HashMap<>();
    private AtomicIntegerArray bucketCounts = new AtomicIntegerArray(Constants.BUCKET_SIZE);

    public Bucket() {}

    public static Bucket getGlobal() {
        return global;
    }

    public static Bucket allocate(Object o) {
        if (bucketPool.containsKey(o)) {
            return bucketPool.get(o);
        } else {
            Bucket bucket = new Bucket();
            bucketPool.put(o, bucket);
            return bucket;
        }
    }

    public static Bucket getFromPool(Object o) {
        return bucketPool.get(o);
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

    public int[] getRangeSums() {
        int[] rangeSums = new int[Constants.BUCKET_SIZE];
        getRangeSums(rangeSums);
        return rangeSums;
    }

    public void getRangeSums(int[] rangeSums) {
        rangeSums[0] = bucketCounts.get(0);
        for (int i = 1; i < Constants.BUCKET_SIZE; ++i) {
            rangeSums[i] = rangeSums[i - 1] + bucketCounts.get(i);
        }
    }

    public void clear() {
        bucketCounts = new AtomicIntegerArray(Constants.BUCKET_SIZE);
    }
}
