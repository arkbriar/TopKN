package com.alibaba.middleware.topkn.refactor.process;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by Shunjie Ding on 26/07/2017.
 */
public class LineProcessor {
    private static final Logger logger = LoggerFactory.getLogger(LineProcessor.class);

    private static final int BUFFER_SIZE = 1 * 1024 * 1024;
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
        for (int i = 0; i < concurrentNum * 2; ++i) {
            freeBufferBlockingQueue.add(ByteBuffer.allocate(BUFFER_SIZE_WITH_MARGIN));
        }
    }

    public <T extends BufferLineProcessor> void scan(Class<T> clazz)
        throws FileNotFoundException, InterruptedException, ExecutionException, IllegalAccessException,
               InstantiationException, NoSuchMethodException, InvocationTargetException {
        ExecutorService executorService = Executors.newFixedThreadPool(concurrentNum + 1);
        Future future = executorService.submit(new Reader(new FileInputStream(file)));
        for (int i = 0; i < concurrentNum; ++i) {
            BufferLineProcessor processor =
                clazz.getDeclaredConstructor(BlockingQueue.class, BlockingQueue.class)
                    .newInstance(freeBufferBlockingQueue, bufferBlockingQueue);
            executorService.submit(processor);
        }
        executorService.shutdown();
        // wait for reader to exit
        future.get();

        // send empty ByteBuffer to notify processors to exit
        for (int i = 0; i < concurrentNum; ++i) {
            bufferBlockingQueue.put(ByteBuffer.allocate(0));
        }

        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    public <T extends BufferLineProcessor, R> List<R> scan(Class<T> clazz, R result)
        throws FileNotFoundException, InterruptedException, ExecutionException, IllegalAccessException,
               InstantiationException, NoSuchMethodException, InvocationTargetException {
        ExecutorService executorService = Executors.newFixedThreadPool(concurrentNum + 1);
        Future future = executorService.submit(new Reader(new FileInputStream(file)));

        List<Future<R>> processorResultFutures = new ArrayList<>(concurrentNum);
        for (int i = 0; i < concurrentNum; ++i) {
            BufferLineProcessor processor =
                clazz.getDeclaredConstructor(BlockingQueue.class, BlockingQueue.class)
                    .newInstance(freeBufferBlockingQueue, bufferBlockingQueue);

            processorResultFutures.add(executorService.submit(processor, result));
        }
        executorService.shutdown();
        // wait for reader to exit
        future.get();

        // send empty ByteBuffer to notify processors to exit
        for (int i = 0; i < concurrentNum; ++i) {
            bufferBlockingQueue.put(ByteBuffer.allocate(0));
        }

        List<R> processorResults = new ArrayList<>(concurrentNum);
        for (int i = 0; i < concurrentNum; ++i) {
            processorResults.add(processorResultFutures.get(i).get());
        }

        return processorResults;
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
}
