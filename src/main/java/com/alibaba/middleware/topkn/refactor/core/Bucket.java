package com.alibaba.middleware.topkn.refactor.core;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Shunjie Ding on 26/07/2017.
 */
public class Bucket {
    private AtomicInteger size = new AtomicInteger(0);
    private int strLen;
    private char leadingChar;

    public Bucket(int strLen, char leadingChar) {
        this.strLen = strLen;
        this.leadingChar = leadingChar;
    }

    public void increaseSizeByOne() {
        size.getAndIncrement();
    }

    public int getSize() {
        return size.get();
    }

    public int getStrLen() {
        return strLen;
    }

    public char getLeadingChar() {
        return leadingChar;
    }
}
