package com.alibaba.middleware.topkn.refactor.process;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by Shunjie Ding on 26/07/2017.
 */
public abstract class LineProcessor implements Runnable {
    private static final int BUFFER_SIZE = 16 * 1024 * 1024;
    private static final int BUFFER_SIZE_WITH_MARGIN = BUFFER_SIZE + 256;

    private int concurrentNum = 4;

    private File file;

    private BlockingQueue<ByteBuffer> freeBufferBlockingQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<ByteBuffer> bufferBlockingQueue = new LinkedBlockingQueue<>();

    public LineProcessor(int concurrentNum, String filePath) {
        this.concurrentNum = concurrentNum;
        this.file = new File(filePath);

        prepareByteBuffers();
    }

    private void prepareByteBuffers() {
        for (int i = 0; i < concurrentNum; ++i) {
            freeBufferBlockingQueue.add(ByteBuffer.allocate(BUFFER_SIZE_WITH_MARGIN));
        }
    }

    @Override
    public void run() {
        try {
            scan();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void scan() throws FileNotFoundException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(concurrentNum + 1);
        executorService.submit(new Reader(new FileInputStream(file)));
        for (int i = 0; i < concurrentNum; ++i) {
            executorService.submit(new Processor());
        }
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    protected abstract void processLine(byte[] a, int i, int j, int limit);

    private int processLine(byte[] a, int i, int limit) {
        int j = i;
        while (a[j] != '\n' && j < limit) {
            ++j;
        }
        processLine(a, i, j - 1, limit);
        if (j == limit) { return -1; }
        return j + 1;
    }

    private class Reader implements Runnable {

        private FileInputStream fileInputStream;

        Reader(FileInputStream fileInputStream) {
            this.fileInputStream = fileInputStream;
        }

        @Override
        public void run() {
            try {
                while (fileInputStream.available() != 0) {
                    readNext();
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private int readToNext(FileInputStream fileInputStream, byte[] bytes, int i, char c)
            throws IOException {
            int b = -2, count = 0;

            while (b != c && b != -1) {
                b = fileInputStream.read();
                bytes[i++] = (byte) b;
                count++;
            }

            return count == 0 ? -1 : count;
        }

        private void readNext() throws InterruptedException, IOException {
            ByteBuffer buffer = freeBufferBlockingQueue.take();
            buffer.clear();
            byte[] inner = buffer.array();
            int readSize = fileInputStream.read(inner, 0, BUFFER_SIZE);

            // read to next '\n'
            if (readSize == BUFFER_SIZE) {
                if (inner[BUFFER_SIZE - 1] != '\n') {
                    int r = readToNext(fileInputStream, inner, BUFFER_SIZE, '\n');
                    if (r != -1) { readSize += r; }
                }
            }
            buffer.limit(readSize);

            bufferBlockingQueue.put(buffer);
        }
    }

    private class Processor implements Runnable {

        @Override
        public void run() {
            try {
                process(bufferBlockingQueue.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        private void process(ByteBuffer buffer) {
            byte[] inner = buffer.array();
            int limit = buffer.limit();
            int i = 0;
            while ((i = processLine(inner, i, limit)) != -1) { ; }
        }
    }
}
