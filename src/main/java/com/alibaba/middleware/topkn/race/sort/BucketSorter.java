package com.alibaba.middleware.topkn.race.sort;

import com.alibaba.middleware.topkn.race.communication.DataIndex;
import com.alibaba.middleware.topkn.race.sort.buckets.BucketUtils;
import com.alibaba.middleware.topkn.race.sort.buckets.BufferedBucket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Created by Shunjie Ding on 24/07/2017.
 */
public class BucketSorter {
    public static final int BUCKET_LIMIT = 600;

    private static final Logger logger = LoggerFactory.getLogger(BucketSorter.class);
    private static final char[] CHARACTERS = "0123456789abcdefghijklmnopqrstuvwxyz".toCharArray();
    private final Semaphore available = new Semaphore(14700000, true);
    private String storeDir;
    private List<String> fileSplits;
    private Map<Integer, Map<Character, BufferedBucket>> buckets;
    private List<BufferedBucket> bucketList = new ArrayList<>(128 * 36);

    public BucketSorter(String storeDir, List<String> fileSplits) {
        this.storeDir = storeDir;
        this.fileSplits = fileSplits;

        initializeBuckets();
    }

    private static String getBucketName(BufferedBucket bucket) {
        return String.format("%d-%c", bucket.getStrLen(), bucket.getLeadingCharacter());
    }

    public void initializeBuckets() {
        buckets = new HashMap<>();
        for (int i = 1; i <= 128; ++i) {
            Map<Character, BufferedBucket> characterBucketMap =
                new HashMap<Character, BufferedBucket>();
            buckets.put(i, characterBucketMap);
            for (char c : CHARACTERS) {
                BufferedBucket bucket =
                    new BufferedBucket(i, c, getBucketUnsortedBlock(i, c), BUCKET_LIMIT);
                characterBucketMap.put(c, bucket);
                bucketList.add(bucket);
            }
        }
    }

    private String getBucketPath(int i, char c) {
        return String.format("%s/%d/%c", storeDir, i, c);
    }

    private String getBucketUnsortedBlock(BufferedBucket bucket) {
        return getBucketUnsortedBlock(bucket.getStrLen(), bucket.getLeadingCharacter());
    }

    private String getBucketUnsortedBlock(int i, char c) {
        return getBucketPath(i, c) + "/block-unsorted.txt";
    }

    private String getBucketSortedBlock(BufferedBucket bucket) {
        return getBucketSortedBlock(bucket.getStrLen(), bucket.getLeadingCharacter());
    }

    private String getBucketSortedBlock(int i, char c) {
        return getBucketPath(i, c) + "/block-sorted.txt";
    }

    private String getIndexFile() {
        return String.format("%s/index.json", storeDir);
    }

    private void createStoreDirectories() {
        Path storeDirPath = Paths.get(storeDir);
        if (!storeDirPath.toFile().exists()) {
            try {
                Files.createDirectories(storeDirPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void coarseGrainedSortInParallel() {
        createStoreDirectories();

        logger.info("Begin sort!");

        mapIntoBucketsInParallel();

        logger.info("Unsorted buckets' ready!");

        persistentDataIndex();

        logger.info("Index file's written!");
    }

    public void coarseGrainedSortInParallelWithBlockPersistence() {
        createStoreDirectories();

        logger.info("Begin sort!");

        mapIntoBucketsInParallelUsingProducerConsumerModel();

        logger.info("Unsorted buckets' ready!");

        persistentDataIndex();

        logger.info("Index file's written!");
    }

    public void fineGrainedSortInParallelWithBlockPersistence() {
        createStoreDirectories();

        logger.info("Begin sort!");

        mapIntoBucketsInParallelUsingProducerConsumerModel();

        logger.info("Unsorted buckets' ready!");

        sortInBuckets();

        logger.info("Sorted buckets' ready!");

        persistentDataIndex();

        logger.info("Index file's written!");
    }

    private void persistentDataIndex() {
        DataIndex index = new DataIndex(buckets);

        try {
            index.flushToDisk(getIndexFile());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void mapIntoBucketsInParallel() {
        ExecutorService mapperService = Executors.newFixedThreadPool(5);
        for (String fileSplit : fileSplits) {
            mapperService.submit(new FileSplitLineReader(fileSplit));
        }
        mapperService.shutdown();
        try {
            mapperService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void mapIntoBucketsInParallelUsingProducerConsumerModel() {
        ExecutorService producerService = Executors.newFixedThreadPool(5);
        ExecutorService consumerService = Executors.newFixedThreadPool(128);

        List<BlockingQueue<String>> queues = new ArrayList<>(128);

        // start all consumers
        for (int i = 0; i < 128; ++i) {
            queues.add(new ArrayBlockingQueue<String>(21600));
        }
        for (int i = 0; i < 128; ++i) {
            consumerService.submit(new LineBucketMapper(i, queues.get(i), buckets.get(i + 1)));
        }

        // start all producers
        for (String fileSplit : fileSplits) {
            producerService.submit(new FileSplitLineProducer(queues, fileSplit));
        }

        try {
            producerService.shutdown();
            producerService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);

            logger.info("All producers' are done!");

            producerService.shutdown();
            while (true) {
                boolean allEmpty = true;
                for (int i = 0; i < 128; ++i) {
                    if (!queues.get(i).isEmpty()) {
                        allEmpty = false;
                        break;
                    }
                }
                if (allEmpty) {
                    break;
                } else {
                    Thread.sleep(1000);
                }
            }
            logger.info("All lines' are consumed!");
            consumerService.shutdownNow();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (BufferedBucket bucket : bucketList) {
            bucket.flushToDisk();
        }
    }

    private void sortInBuckets() {
        ExecutorService executorService = Executors.newFixedThreadPool(400);

        for (BufferedBucket bucket : bucketList) {
            try {
                available.acquire(bucket.getSize());
                executorService.submit(new BucketSortThread(bucket), getBucketName(bucket));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(200, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class FileSplitLineReader implements Runnable {
        String fileSplit;

        public FileSplitLineReader(String fileSplit) {
            this.fileSplit = fileSplit;
        }

        protected void processLine(String line) {
            buckets.get(line.length()).get(line.charAt(0)).add(line);
        }

        private void mapIntoBuckets(String filePath) {
            File file = new File(filePath);
            try {
                InputStreamReader streamReader = new InputStreamReader(new FileInputStream(file));
                BufferedReader reader = new BufferedReader(streamReader);

                String line;
                int count = 0;
                while ((line = reader.readLine()) != null) {
                    processLine(line);
                    if ((++count) % 4000000 == 0) {
                        logger.info(count + " lines in " + filePath + " are emitted!");
                    }
                }

                logger.info("All lines in " + filePath + " are emitted!");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            logger.info("Mapping split " + fileSplit + " to buckets.");
            mapIntoBuckets(fileSplit);
        }
    }

    private class FileSplitLineProducer extends FileSplitLineReader {
        List<BlockingQueue<String>> queues;

        String fileSplit;

        public FileSplitLineProducer(List<BlockingQueue<String>> queues, String fileSplit) {
            super(fileSplit);
            this.queues = queues;
        }

        @Override
        protected void processLine(String line) {
            try {
                queues.get(line.length() - 1).put(line);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private class LineBucketMapper implements Runnable {
        private int name;

        private BlockingQueue<String> queue;

        private Map<Character, BufferedBucket> buckets;

        public LineBucketMapper(
            int name, BlockingQueue<String> queue, Map<Character, BufferedBucket> buckets) {
            this.name = name;
            this.queue = queue;
            this.buckets = buckets;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    String line = queue.take();
                    char leadingCharacter = line.charAt(0);

                    BufferedBucket bucket = buckets.get(leadingCharacter);
                    bucket.add(line);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private class BucketSortThread implements Runnable {
        private BufferedBucket bucket;

        public BucketSortThread(BufferedBucket bucket) {
            this.bucket = bucket;
        }

        @Override
        public void run() {
            sortInBuckets(bucket);
        }

        void sortInBuckets(BufferedBucket bucket) {
            if (bucket.getPersistenceLimit() != BufferedBucket.UNLIMITED
                && bucket.getSize() > bucket.getPersistenceLimit()) {
                BucketUtils.restoreBucketData(bucket, bucket.getBlockFile());
            }

            if (bucket.getStrLen() != 1) {
                Collections.sort(bucket.getData(), StringComparator.getSingle());
            }

            // flush to disk and free memory
            bucket.flushToDisk(getBucketSortedBlock(bucket));
            bucket.clearData();

            logger.info("Bucket " + getBucketName(bucket) + " is sorted!");
        }
    }
}
