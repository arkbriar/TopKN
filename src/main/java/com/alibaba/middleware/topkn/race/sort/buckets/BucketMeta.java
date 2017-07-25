package com.alibaba.middleware.topkn.race.sort.buckets;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "Bucket(" + strLen + ", " + leadingCharacter + ")";
    }
}
