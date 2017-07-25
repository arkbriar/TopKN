package com.alibaba.middleware.topkn.race.sort.buckets;

import com.alibaba.middleware.topkn.race.sort.StringComparator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by Shunjie Ding on 24/07/2017.
 */
public class BufferedBucket {
    public static final int UNLIMITED = -1;

    private final int persistenceLimit;
    private BucketMeta meta;
    private List<String> data;

    private Comparator<String> comparator = StringComparator.getSingle();

    public BufferedBucket(BucketMeta meta, int persistenceLimit) {
        this.meta = meta;

        this.persistenceLimit = persistenceLimit;
        this.data = new ArrayList<>(persistenceLimit);
    }

    public BufferedBucket(
        int strLen, char leadingCharacter, String blockFile, int persistenceLimit) {
        this(new BucketMeta(strLen, leadingCharacter, blockFile), persistenceLimit);
    }

    public BucketMeta getMeta() {
        return meta;
    }

    public int getPersistenceLimit() {
        return persistenceLimit;
    }

    synchronized public void add(String s) {
        meta.increaseSizeByOne();

        // Bucket with length 1 and leading character has no need to
        // actually add it.
        if (getStrLen() == 1) {
            return;
        }

        if (persistenceLimit != UNLIMITED && data.size() == persistenceLimit) {
            flushToDiskAndClear();
        }

        data.add(s);
    }

    synchronized public void increaseSize() {
        meta.increaseSizeByOne();
    }

    public List<String> sort(Comparator<String> comparator) {
        Collections.sort(data, comparator);
        return data;
    }

    public int getStrLen() {
        return meta.getStrLen();
    }

    public char getLeadingCharacter() {
        return meta.getLeadingCharacter();
    }

    public int getSize() {
        return meta.getSize();
    }

    public List<String> getData() {
        return data;
    }

    public void clearData() {
        data.clear();
    }

    public String getBlockFile() {
        return meta.getBlockFile();
    }

    public void flushToDisk() {
        flushToDisk(meta.getBlockFile());
    }

    public void flushToDisk(String filePath) {
        try {
            BucketUtils.FlushToDisk(data, filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void flushToDiskAndClear() {
        flushToDisk();
        clearData();
    }
}
