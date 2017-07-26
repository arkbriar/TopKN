package com.alibaba.middleware.topkn.refactor.process;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Shunjie Ding on 26/07/2017.
 */
public abstract class BufferLineProcessor implements Runnable {
    private BlockingQueue<ByteBuffer> freeBufferBlockingQueue, bufferBlockingQueue;

    public BufferLineProcessor(BlockingQueue<ByteBuffer> freeBufferBlockingQueue,
        BlockingQueue<ByteBuffer> bufferBlockingQueue) {
        this.freeBufferBlockingQueue = freeBufferBlockingQueue;
        this.bufferBlockingQueue = bufferBlockingQueue;
    }

    @Override
    public void run() {
        try {
            while (true) {
                ByteBuffer buffer = bufferBlockingQueue.take();
                // exit on empty buffer
                if (buffer.capacity() == 0) {
                    return;
                }

                process(buffer);
                buffer.clear();
                freeBufferBlockingQueue.put(buffer);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
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