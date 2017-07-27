package com.alibaba.middleware.topkn.core;

import java.io.File;

/**
 * Created by Shunjie Ding on 27/07/2017.
 */
public class FileSegment {
    private File file;

    private long offset;

    private long size;

    public FileSegment(File file, long offset, long size) {
        this.file = file;
        this.offset = offset;
        this.size = size;
    }

    public long getOffset() {
        return offset;
    }

    public long getSize() {
        return size;
    }

    public File getFile() {
        return file;
    }
}
