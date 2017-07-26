package com.alibaba.middleware.topkn.refactor.core;

import com.alibaba.middleware.topkn.refactor.Constants;
import com.alibaba.middleware.topkn.refactor.process.BufferLineProcessor;
import com.alibaba.middleware.topkn.refactor.process.LineProcessor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by Shunjie Ding on 26/07/2017.
 */
public class StringListReader {

    private List<String> fileSplits;

    private Set<Integer> buckets;

    public StringListReader(List<String> fileSplits, int[] buckets) {
        this.fileSplits = fileSplits;
        this.buckets = new HashSet<Integer>(buckets.length);
        for (int b : buckets) {
            this.buckets.add(b);
        }
    }

    public List<String> readAll() {
        List<String> res = new ArrayList<>();

        ExecutorService executorService = Executors.newFixedThreadPool(fileSplits.size());
        List<Future<List<String>>> futures = new ArrayList<>(fileSplits.size());
        for (String fileSplit : fileSplits) {
            futures.add(executorService.submit(
                new StringListLineProcessor(Constants.CONCURRENT_PROCESSOR_NUMBER, fileSplit)));
        }
        executorService.shutdown();

        for (Future<List<String>> future : futures) {
            try {
                res.addAll(future.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        return res;
    }

    private class StringListLineProcessor extends LineProcessor implements Callable<List<String>> {

        public StringListLineProcessor(int concurrentNum, String filePath) {
            super(concurrentNum, filePath);
        }

        @Override
        public List<String> call() throws Exception {
            List<String> pieceRes = new ArrayList<>();

            List<List<String>> res = scan(StringListBufferLineProcessor.class, pieceRes);

            for (List<String> piece : res) {
                pieceRes.addAll(piece);
            }

            return pieceRes;
        }
    }

    private class StringListBufferLineProcessor extends BufferLineProcessor
        implements Callable<List<String>> {
        private List<String> stringStore = new ArrayList<>();

        public StringListBufferLineProcessor(
            BlockingQueue<ByteBuffer> freeBufferBlockingQueue,
            BlockingQueue<ByteBuffer> bufferBlockingQueue) {
            super(freeBufferBlockingQueue, bufferBlockingQueue);
        }

        @Override
        protected void processLine(byte[] a, int i, int j) {
            int strLen = j - i;
            int idx = BucketMapper.getBucketIndex(strLen, a, i);
            if (buckets.contains(idx)) {
                stringStore.add(new String(a, i, strLen));
            }
        }

        @Override
        public List<String> call() throws Exception {
            run();
            return stringStore;
        }
    }
}
