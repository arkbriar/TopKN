package com.alibaba.middleware.topkn.core;

import com.alibaba.middleware.topkn.utils.Logger;
import com.alibaba.middleware.topkn.utils.LoggerFactory;

/**
 * Created by Shunjie Ding on 27/07/2017.
 */
public class IndexBuilder extends BufferedFileSegmentReadProcessor {
    private static final Logger logger = LoggerFactory.getLogger(IndexBuilder.class);

    private Bucket buckets = Bucket.getGlobal();

    public IndexBuilder(FileSegmentLoader fileSegmentLoader, int bufferSize) {
        super(fileSegmentLoader, bufferSize);
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
            if (len == 1) {
                buckets.increaseBucketCount(len, readBuffer[pos], (byte) 0);
            } else {
                buckets.increaseBucketCount(len, readBuffer[pos], readBuffer[pos + 1]);
            }

            pos = endPos + 1;
        }
    }
}
