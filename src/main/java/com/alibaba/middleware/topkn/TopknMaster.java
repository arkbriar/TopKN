package com.alibaba.middleware.topkn;

import com.alibaba.middleware.topkn.core.Bucket;
import com.alibaba.middleware.topkn.core.Index;
import com.alibaba.middleware.topkn.core.StringComparator;
import com.alibaba.middleware.topkn.utils.Logger;
import com.alibaba.middleware.topkn.utils.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Shunjie Ding on 27/07/2017.
 */
public class TopknMaster implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TopknMaster.class);
    private static final CountDownLatch waitingBucketLatch =
        new CountDownLatch(Constants.NUM_WORKERS);
    private static final CountDownLatch buildingIndexLatch = new CountDownLatch(1);
    private static final CountDownLatch waitingResultLatch =
        new CountDownLatch(Constants.NUM_WORKERS);
    private static final List<String> results = new ArrayList<>();
    private static boolean indexBuilt = false;
    private static Index index;
    private static long k;
    private static int n;
    private SocketChannel socketChannel;
    private int port;
    private Bucket bucket = Bucket.allocate(this);

    public TopknMaster(int port) {
        this.port = port;
    }

    public static Index buildIndexAndSave(List<TopknMaster> masters) throws IOException {
        List<Bucket> buckets = new ArrayList<>(masters.size());
        for (TopknMaster master : masters) {
            buckets.add(Bucket.getFromPool(master));
        }

        Index index = Index.getIndex(buckets);
        Index.writeToFile(index, Constants.MIDDLE_DIR + "/index.bin");
        return index;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        k = Long.valueOf(args[0]);
        n = Integer.valueOf(args[1]);

        logger.info("Starting top(k, n) with k = %d and n = %d...", k, n);

        logger.info("Reading index file from %s...", Constants.MIDDLE_DIR + "/index.bin");
        index = Index.readFromFile(Constants.MIDDLE_DIR + "/index.bin");
        if (index != null) {
            indexBuilt = true;
            buildingIndexLatch.countDown();
            logger.info("Index is valid when %d == 160000000.",
                index.getRangeSum(Constants.BUCKET_SIZE - 1));
        }

        // Each TopknMaster will take care of sending requests and read replies
        // Synchronization is achieved by using CountDownLatch (index building, result reading)
        // and synchronize keyword (result gathering)
        List<TopknMaster> masters = new ArrayList<>(Constants.NUM_WORKERS);
        ExecutorService executorService = Executors.newFixedThreadPool(Constants.NUM_WORKERS);
        for (int i = 0; i < Constants.NUM_WORKERS; ++i) {
            TopknMaster master = new TopknMaster(Constants.SERVER_PORTS[i]);
            masters.add(master);
            executorService.submit(master);
        }
        executorService.shutdown();

        // Build index if not found
        if (!indexBuilt) {
            logger.info("Waiting workers to get buckets...");
            waitingBucketLatch.await();
            logger.info("Building index...");
            index = buildIndexAndSave(masters);
            buildingIndexLatch.countDown();
            logger.info("Index built!");
        }

        int lower = index.binarySearch((int) (k + 1)), upper = index.binarySearch((int) (k + n));
        logger.info("Results are lied in bucket %d to %d, start querying...", lower, upper);
        logger.info("Waiting for candidates...");
        waitingResultLatch.await();

        logger.info("Get candidates, sorting...");
        Collections.sort(results, new StringComparator());

        // Persist to res.txt
        File file = new File(Constants.RESULT_DIR + "/res.txt");
        logger.info("Get results, writing to %s...", file.getAbsolutePath());

        if (!file.exists())
            file.createNewFile();
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            int startIndex = (int) (k - index.getRangeSum(lower - 1));
            for (int i = startIndex; i < startIndex + n; ++i) {
                fileOutputStream.write(results.get(i).getBytes());
                fileOutputStream.write('\n');
            }
        }
        logger.info("Result's written, done!");
    }

    private void listenAndSetSocketChannel() throws IOException {
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress(port));

        // We need just one connection for a port
        socketChannel = serverSocketChannel.accept();
        serverSocketChannel.close();
    }

    private void close() throws IOException {
        socketChannel.close();
        socketChannel = null;
    }

    private void requestBucket() throws IOException {
        ByteBuffer writeBuffer = ByteBuffer.allocate(1024);
        writeBuffer.putInt(Constants.OP_INDEX);
        writeBuffer.flip();

        while (writeBuffer.hasRemaining()) {
            socketChannel.write(writeBuffer);
        }
    }

    private void readBucket() throws IOException {
        ByteBuffer readBuffer = ByteBuffer.allocate(Constants.READ_BUFFER_SIZE);
        readBuffer.limit(4 * Constants.BUCKET_SIZE);

        while (readBuffer.hasRemaining()) {
            socketChannel.read(readBuffer);
        }
        readBuffer.flip();

        logger.info("Restoring bucket...");
        bucket.readFromBuffer(readBuffer);
        logger.info("Bucket restored...");
    }

    private void requestAndReadBucket() throws IOException {
        requestBucket();
        readBucket();
    }

    private void requestRangeQuery(int lower, int upper) throws IOException {
        ByteBuffer writeBuffer = ByteBuffer.allocate(12);
        writeBuffer.putInt(Constants.OP_QUERY);
        writeBuffer.putInt(lower);
        writeBuffer.putInt(upper);
        writeBuffer.flip();

        while (writeBuffer.hasRemaining()) {
            socketChannel.write(writeBuffer);
        }
    }

    // Save to static field results
    private void readResults() throws IOException {
        ByteBuffer readBuffer = ByteBuffer.allocate(Constants.RESULT_BUFFER_SIZE);
        readBuffer.limit(4);
        int x = socketChannel.read(readBuffer);
        readBuffer.flip();
        int size = readBuffer.getInt();
        readBuffer.clear();

        logger.info("Reading result from worker, size to read %d...", size);
        readBuffer.limit(size);
        while (readBuffer.hasRemaining()) {
            socketChannel.read(readBuffer);
        }
        readBuffer.flip();

        // Process this buffer
        byte[] buffer = readBuffer.array();
        int pos = 0;
        while (pos < readBuffer.limit()) {
            int endPos = pos + 1;
            while (buffer[endPos] != '\n') {
                endPos++;
            }
            synchronized (results) {
                results.add(new String(buffer, pos, endPos - pos));
            }
            pos = endPos + 1;
        }
    }

    private void requestRangeQueryAndReadResult(int lower, int upper) throws IOException {
        requestRangeQuery(lower, upper);
        readResults();
    }

    @Override
    public void run() {
        try {
            listenAndSetSocketChannel();

            if (!indexBuilt) {
                requestAndReadBucket();
                waitingBucketLatch.countDown();
                buildingIndexLatch.await();
            }

            int lower = index.binarySearch((int) (k + 1));
            int upper = index.binarySearch((int) (k + n));

            requestRangeQueryAndReadResult(lower, upper);
            waitingResultLatch.countDown();

            close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (RuntimeException e) {
            // print runtime exception as ExecutorService.submit
            // does not install an exception handler on threads it starts
            e.printStackTrace();
            throw e;
        }
    }
}
