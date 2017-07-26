package com.alibaba.middleware.topkn.refactor.core;

import com.alibaba.middleware.topkn.refactor.Constants;
import com.alibaba.middleware.topkn.refactor.process.BufferLineProcessor;
import com.alibaba.middleware.topkn.refactor.process.BufferLineProcessorFactory;
import com.alibaba.middleware.topkn.refactor.process.LineProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
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
    private static final Logger logger = LoggerFactory.getLogger(StringListReader.class);

    private List<String> fileSplits;

    private int[] buckets;

    public StringListReader(List<String> fileSplits, int[] buckets) {
        this.fileSplits = fileSplits;
        this.buckets = buckets;
    }

    // for test purpose
    public static void main(String[] args) {
        List<String> fileSplits =
            Arrays.asList("split1.txt", "split2.txt", "split3.txt", "split4.txt", "split5.txt");
        int[] buckets = new int[]{0, 1};
        StringListReader stringListReader = new StringListReader(fileSplits, buckets);

        List<String> strings = stringListReader.readAll();

        logger.info("" + strings.size());

        assert strings.size() == 343739;
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

            StringListBufferLineProcessorFactory factory = new StringListBufferLineProcessorFactory();

            scan(factory);

            for (List<String> piece : factory.stringStores) {
                pieceRes.addAll(piece);
            }

            return pieceRes;
        }
    }

    private class StringListBufferLineProcessorFactory extends BufferLineProcessorFactory {
        private List<List<String>> stringStores = new ArrayList<>();

        @Override
        public BufferLineProcessor newInstance(
            BlockingQueue<ByteBuffer> p, BlockingQueue<ByteBuffer> q) {
            List<String> stringStore = new ArrayList<>();
            stringStores.add(stringStore);
            return new StringListBufferLineProcessor(p, q, stringStore);
        }
    }

    private class StringListBufferLineProcessor
        extends BufferLineProcessor {
        private List<String> stringStore;

        StringListBufferLineProcessor(
            BlockingQueue<ByteBuffer> freeBufferBlockingQueue,
            BlockingQueue<ByteBuffer> bufferBlockingQueue, List<String> stringStore) {
            super(freeBufferBlockingQueue, bufferBlockingQueue);
            this.stringStore = stringStore;
        }

        private boolean bucketsContains(int[] buckets, int idx) {
            for (int i = 0; i < buckets.length; ++ i) {
                if (buckets[i] == idx) return true;
            }
            return false;
        }

        @Override
        protected void processLine(byte[] a, int i, int j) {
            int strLen = j - i;
            int idx = BucketMapper.getBucketIndex(strLen, a, i);
            if (bucketsContains(buckets, idx)) {
                stringStore.add(new String(a, i, strLen));
            }
        }
    }
}
