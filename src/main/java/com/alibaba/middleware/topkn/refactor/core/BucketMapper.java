package com.alibaba.middleware.topkn.refactor.core;

import com.alibaba.middleware.topkn.refactor.Constants;
import com.alibaba.middleware.topkn.refactor.process.BufferLineProcessor;
import com.alibaba.middleware.topkn.refactor.process.BufferLineProcessorFactory;
import com.alibaba.middleware.topkn.refactor.process.LineProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Created by Shunjie Ding on 26/07/2017.
 */
public class BucketMapper {
    private static final Logger logger = LoggerFactory.getLogger(BucketMapper.class);

    private List<String> fileSplits;

    private AtomicIntegerArray atomicIntegerArray = new AtomicIntegerArray(127 * 36 * 36 + 36);

    public BucketMapper(List<String> fileSplits) {
        this.fileSplits = fileSplits;
    }

    private static int getCharIndex(char c) {
        if (c <= '9') {
            return c - '0';
        } else {
            return c - 'a';
        }
    }

    public static int getBucketIndex(int strLen, byte[] a, int i) {
        if (strLen == 1) {
            return getCharIndex((char) a[i]);
        }
        return (strLen - 2) * 36 * 36 + getCharIndex((char) a[i]) * 36
            + getCharIndex((char) a[i + 1]);
    }

    // for test purpose
    public static void main(String[] args) throws InterruptedException {
        List<String> fileSplits =
            Arrays.asList("split1.txt", "split2.txt", "split3.txt", "split4.txt", "split5.txt");

        BucketMapper bucketMapper = new BucketMapper(fileSplits);

        bucketMapper.mapToBuckets();

        AtomicIntegerArray atomicIntegerArray = bucketMapper.getAtomicIntegerArray();

        logger.info(atomicIntegerArray.toString());

        int sum = 0;
        for (int i = 0; i < 128 * 36; ++i) {
            sum += atomicIntegerArray.get(i);
        }

        assert atomicIntegerArray.get(0) + atomicIntegerArray.get(1) == 343739;
        assert sum == 80000000;
    }

    public AtomicIntegerArray getAtomicIntegerArray() {
        return atomicIntegerArray;
    }

    public void mapToBuckets() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(fileSplits.size());
        for (String fileSplit : fileSplits) {
            executorService.submit(
                new BucketLineProcessor(Constants.CONCURRENT_PROCESSOR_NUMBER, fileSplit));
        }
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    private class BucketLineProcessor extends LineProcessor implements Runnable {
        public BucketLineProcessor(int concurrentNum, String filePath) {
            super(concurrentNum, filePath);
        }

        @Override
        public void run() {
            try {
                scan(new BucketBufferLineProcessorFactory());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private class BucketBufferLineProcessorFactory extends BufferLineProcessorFactory {

        @Override
        public BufferLineProcessor newInstance(
            BlockingQueue<ByteBuffer> p, BlockingQueue<ByteBuffer> q) {
            return new BucketBufferLineProcessor(p, q);
        }
    }

    private class BucketBufferLineProcessor extends BufferLineProcessor {
        public BucketBufferLineProcessor(BlockingQueue<ByteBuffer> freeBufferBlockingQueue,
            BlockingQueue<ByteBuffer> bufferBlockingQueue) {
            super(freeBufferBlockingQueue, bufferBlockingQueue);
        }

        @Override
        protected void processLine(byte[] a, int i, int j) {
            int strLen = j - i, idx = getBucketIndex(strLen, a, i);
            atomicIntegerArray.incrementAndGet(idx);
        }
    }
}
