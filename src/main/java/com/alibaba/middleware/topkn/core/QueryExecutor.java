package com.alibaba.middleware.topkn.core;

import java.nio.ByteBuffer;

/**
 * Created by Shunjie Ding on 28/07/2017.
 */
public class QueryExecutor extends BufferedFileSegmentReadProcessor {
    private final ByteBuffer resultBuffer;

    private int lower, upper;

    public QueryExecutor(
        FileSegmentLoader fileSegmentLoader, int bufferSize, int lower, int upper,
        ByteBuffer resultBuffer) {
        super(fileSegmentLoader, bufferSize);
        this.lower = lower;
        this.upper = upper;
        this.resultBuffer = resultBuffer;
    }

    @Override
    protected void processSegment(FileSegment segment, byte[] readBuffer, int limit) {
        int pos = 0;
        // Here we skip the first/first-half string if it is not the first segment
        if (segment.getOffset() != 0) {
            while (pos < limit && readBuffer[pos++] != '\n') {
                ;
            }
        }

        // This will handle these conditions:
        // 1. segment end with '\n', then process the next after this segment
        // 2. segment does not end with '\n', then process until the last-half is fully processed
        while (pos <= segment.getSize() && pos < limit) {
            int endPos = pos;
            while (readBuffer[endPos] != '\n') {
                endPos++;
            }

            int len = endPos - pos;
            int index;
            if (len == 1) {
                index = Bucket.getBucketIndex(len, readBuffer[pos], (byte) 0);
            } else {
                index = Bucket.getBucketIndex(len, readBuffer[pos], readBuffer[pos + 1]);
            }

            if (index >= lower && index <= upper) {
                synchronized (resultBuffer) {
                    resultBuffer.put(readBuffer, pos, len + 1);
                }
            }

            pos = endPos + 1;
        }
    }
}
