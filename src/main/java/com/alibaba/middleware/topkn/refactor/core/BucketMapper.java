package com.alibaba.middleware.topkn.refactor.core;

import com.alibaba.middleware.topkn.refactor.process.LineProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
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

    private AtomicIntegerArray atomicIntegerArray = new AtomicIntegerArray(128 * 36);

    public BucketMapper(List<String> fileSplits) {
        this.fileSplits = fileSplits;
    }

    public static int getBucketIndex(int strLen, char leading) {
        int res = (strLen - 1) * 36;
        if (leading <= '9') { res += leading - '0'; } else { res += 10 + leading - 'a'; }
        return res;
    }

    public static void main(String[] args) throws InterruptedException {
        List<String> fileSplits = Arrays.asList(
            "split1.txt", "split2.txt", "split3.txt", "split4.txt", "split5.txt");

        BucketMapper bucketMapper = new BucketMapper(fileSplits);
        bucketMapper.mapToBuckets();

        AtomicIntegerArray atomicIntegerArray = bucketMapper.getAtomicIntegerArray();

        logger.info(atomicIntegerArray.toString());
    }

    public AtomicIntegerArray getAtomicIntegerArray() {
        return atomicIntegerArray;
    }

    public void mapToBuckets() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(fileSplits.size());
        for (String fileSplit : fileSplits) {
            executorService.submit(new BucketLineProcessor(4, fileSplit));
        }
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    private class BucketLineProcessor extends LineProcessor {

        public BucketLineProcessor(int concurrentNum, String filePath) {
            super(concurrentNum, filePath);
        }

        @Override
        protected void processLine(byte[] a, int i, int j, int limit) {
            int idx = getBucketIndex(j - i + 1, (char) a[i]);
            atomicIntegerArray.incrementAndGet(idx);
        }
    }
}
