package com.alibaba.middleware.topkn.core;

import java.io.File;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Shunjie Ding on 27/07/2017.
 */
public class FileSegmentLoader {
    private List<String> filePaths;

    private Iterator<String> currentFilePathIter;

    private File currentFile;

    private long currentOffset;

    private final long segmentSize;

    public FileSegmentLoader(List<String> filePaths, long segmentSize) {
        this.filePaths = filePaths;
        currentFilePathIter = filePaths.iterator();
        this.segmentSize = segmentSize;

        currentFile = nextFile();
    }

    private File nextFile() {
        if (currentFilePathIter.hasNext()) {
            currentOffset = 0;
            return new File(currentFilePathIter.next());
        }
        return null;
    }

    public synchronized FileSegment nextFileSegment() {
        if (currentOffset >= currentFile.length()) {
            currentFile = nextFile();
            if (currentFile == null)
                return null;
        }
        long prevOffset = currentOffset;
        currentOffset += segmentSize;
        return new FileSegment(currentFile, prevOffset, segmentSize);
    }
}
