package com.alibaba.middleware.topkn.refactor.process;

import com.alibaba.middleware.topkn.refactor.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created by Shunjie Ding on 26/07/2017.
 */
public class LineProcessor {
    private static final Logger logger = LoggerFactory.getLogger(LineProcessor.class);

    private int concurrentNum;

    private File file;

    private ConcurrentLinkedQueue<ByteBuffer> freeBufferConcurrentLinkedQueue =
        new ConcurrentLinkedQueue<>();
    private ConcurrentLinkedQueue<ByteBuffer> bufferConcurrentLinkedQueue =
        new ConcurrentLinkedQueue<>();

    public LineProcessor(int concurrentNum, String filePath) {
        this.concurrentNum = concurrentNum;
        this.file = new File(filePath);

        prepareByteBuffers();
    }

    private void prepareByteBuffers() {
        for (int i = 0; i < concurrentNum * 3 / 2; ++i) {
            freeBufferConcurrentLinkedQueue
                .add(ByteBuffer.allocate(Constants.BUFFER_SIZE_WITH_MARGIN));
        }
    }

    public <T extends BufferLineProcessorFactory> void scan(T factory)
        throws FileNotFoundException, InterruptedException, ExecutionException,
               IllegalAccessException, InstantiationException, NoSuchMethodException,
               InvocationTargetException {
        ExecutorService executorService = Executors.newFixedThreadPool(concurrentNum + 1);
        Future future = executorService.submit(new Reader(new FileInputStream(file)));
        for (int i = 0; i < concurrentNum; ++i) {
            BufferLineProcessor processor =
                factory.newInstance(freeBufferConcurrentLinkedQueue, bufferConcurrentLinkedQueue);
            executorService.submit(processor);
        }
        executorService.shutdown();
        // wait for reader to exit
        future.get();

        // send empty ByteBuffer to notify processors to exit
        for (int i = 0; i < concurrentNum; ++i) {
            bufferConcurrentLinkedQueue.offer(ByteBuffer.allocate(0));
        }

        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
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
                    readNextSegment();
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

        private void readNextSegment() throws IOException, InterruptedException {
            ByteBuffer buffer = freeBufferConcurrentLinkedQueue.poll();
            while (buffer == null) { buffer = freeBufferConcurrentLinkedQueue.poll(); }

            buffer.clear();
            byte[] inner = buffer.array();
            int readSize = fileInputStream.read(inner, 0, Constants.BUFFER_SIZE);

            // read to next '\n'
            if (readSize == Constants.BUFFER_SIZE) {
                if (inner[Constants.BUFFER_SIZE - 1] != '\n') {
                    int r = readToNext(fileInputStream, inner, Constants.BUFFER_SIZE, '\n');
                    if (r != -1) {
                        readSize += r;
                    }
                }
            }
            buffer.limit(readSize);

            bufferConcurrentLinkedQueue.offer(buffer);
        }
    }
}
