package com.alibaba.middleware.topkn.race.sort.buckets;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Created by Shunjie Ding on 24/07/2017.
 */
public class BucketMeta {
    private int size = 0;
    private int strLen;
    private char leadingCharacter;

    // @JsonIgnore
    // private String start, end;

    @JsonIgnore
    private String blockFile;

    protected BucketMeta() {}

    public BucketMeta(int strLen, char leadingCharacter, String blockFile) {
        this.strLen = strLen;
        this.leadingCharacter = leadingCharacter;
        this.blockFile = blockFile;
    }

    public String getBlockFile() {
        return blockFile;
    }

    // public String getStart() {
    //     return start;
    // }
    //
    // public void setStart(String start) {
    //     this.start = start;
    // }
    //
    // public String getEnd() {
    //     return end;
    // }
    //
    // public void setEnd(String end) {
    //     this.end = end;
    // }

    public char getLeadingCharacter() {
        return leadingCharacter;
    }

    public int getStrLen() {
        return strLen;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void increaseSizeByOne() {
        size++;
    }
}
