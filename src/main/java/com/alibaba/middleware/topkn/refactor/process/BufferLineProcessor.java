package com.alibaba.middleware.topkn.refactor.process;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Shunjie Ding on 26/07/2017.
 */
public abstract class BufferLineProcessor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BufferLineProcessor.class);

    private ConcurrentLinkedQueue<ByteBuffer> freeBufferConcurrentLinkedQueue,
        bufferConcurrentLinkedQueue;

    public BufferLineProcessor(
        ConcurrentLinkedQueue<ByteBuffer> freeBufferConcurrentLinkedQueue,
        ConcurrentLinkedQueue<ByteBuffer> bufferConcurrentLinkedQueue) {
        this.freeBufferConcurrentLinkedQueue = freeBufferConcurrentLinkedQueue;
        this.bufferConcurrentLinkedQueue = bufferConcurrentLinkedQueue;
    }

    @Override
    public void run() {
        while (true) {
            ByteBuffer buffer = bufferConcurrentLinkedQueue.poll();
            while (buffer == null) { buffer = bufferConcurrentLinkedQueue.poll(); }

            // exit on empty buffer
            if (buffer.capacity() == 0) {
                return;
            }

            process(buffer);
            buffer.clear();
            freeBufferConcurrentLinkedQueue.offer(buffer);
        }
    }

    protected abstract void processLine(byte[] a, int i, int j);

    private int nextLine(byte[] a, int i, int limit) {
        if (i == limit || i == -1) {
            return -1;
        }

        int j = i;
        while (j < limit && a[j] != '\n') {
            ++j;
        }
        processLine(a, i, j);
        if (j == limit) {
            return -1;
        }
        return j + 1;
    }

    private void process(ByteBuffer buffer) {
        byte[] inner = buffer.array();
        int limit = buffer.limit();
        int i = nextLine(inner, 0, limit);
        while (i != -1) {
            i = nextLine(inner, i, limit);
        }
    }
}