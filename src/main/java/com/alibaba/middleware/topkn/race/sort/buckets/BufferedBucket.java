package com.alibaba.middleware.topkn.race.sort.buckets;

import com.alibaba.middleware.topkn.race.sort.StringComparator;

import java.io.IOException;
import java.util.ArrayList;
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

    public BufferedBucket(int strLen, char leadingCharacter, String blockFile, int persistenceLimit) {
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

        if (persistenceLimit != UNLIMITED && data.size() == persistenceLimit) {
            flushToDiskAndClear();
        }

        data.add(s);

        // if (getStart().isEmpty() || comparator.compare(s, getStart()) < 0) {
        //     setStart(s);
        // }
        // if (getEnd().isEmpty() || comparator.compare(s, getEnd()) > 0) {
        //     setEnd(s);
        // }
    }

    // private String getStart() {
    //     return meta.getStart();
    // }
    //
    // private void setStart(String start) {
    //     meta.setStart(start);
    // }
    //
    // private String getEnd() {
    //     return meta.getEnd();
    // }
    //
    // private void setEnd(String end) {
    //     meta.setEnd(end);
    // }

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