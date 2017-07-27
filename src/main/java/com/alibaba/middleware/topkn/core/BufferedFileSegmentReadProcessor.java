package com.alibaba.middleware.topkn.core;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Created by Shunjie Ding on 27/07/2017.
 */
public abstract class BufferedFileSegmentReadProcessor implements Runnable {
    private final int bufferMargin = 1024;
    private FileSegmentLoader fileSegmentLoader;
    private int bufferSize;
    private byte[] readBuffer;

    public BufferedFileSegmentReadProcessor(FileSegmentLoader fileSegmentLoader, int bufferSize) {
        this.bufferSize = bufferSize;
        this.fileSegmentLoader = fileSegmentLoader;
    }

    private int readSegment(FileSegment segment, byte[] readBuffer) throws IOException {
        int limit = 0;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(segment.getFile(), "r")) {
            randomAccessFile.seek(segment.getOffset());
            limit = randomAccessFile.read(readBuffer, 0, readBuffer.length);
        }
        return limit;
    }

    protected abstract void processSegment(FileSegment segment, byte[] readBuffer, int limit);

    @Override
    public void run() {
        readBuffer = new byte[bufferSize + bufferMargin];
        try {
            FileSegment segment;
            while ((segment = fileSegmentLoader.nextFileSegment()) != null) {
                int limit = readSegment(segment, readBuffer);
                processSegment(segment, readBuffer, limit);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
