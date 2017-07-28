package com.alibaba.middleware.topkn;

import com.alibaba.middleware.topkn.core.Bucket;
import com.alibaba.middleware.topkn.core.Index;
import com.alibaba.middleware.topkn.core.StringComparator;
import com.alibaba.middleware.topkn.utils.Logger;
import com.alibaba.middleware.topkn.utils.LoggerFactory;

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
    private static final CountDownLatch waitingBucketLatch = new CountDownLatch(Constants.NUM_WORKERS);
    private static final CountDownLatch buildingIndexLatch = new CountDownLatch(1);
    private static final CountDownLatch waitingResultLatch = new CountDownLatch(Constants.NUM_WORKERS);
    private static boolean indexBuilt = false;
    private static Index index;
    private static long k;
    private static int n;

    private static List<String> results = new ArrayList<>();

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

        logger.info("Reading index file from %s..." + Constants.MIDDLE_DIR + "/index.bin");
        index = Index.readFromFile(Constants.MIDDLE_DIR + "/index.bin");
        if (index != null) {
            indexBuilt = true;
            buildingIndexLatch.countDown();
        }

        List<TopknMaster> masters = new ArrayList<>(Constants.NUM_WORKERS);
        ExecutorService executorService = Executors.newFixedThreadPool(Constants.NUM_WORKERS);
        for (int i = 0; i < Constants.NUM_WORKERS; ++i) {
            TopknMaster master = new TopknMaster(Constants.SERVER_PORTS[i]);
            masters.add(master);
            executorService.submit(master);
        }

        // build index if not found
        if (!indexBuilt) {
            logger.info("Waiting workers to build index...");
            waitingBucketLatch.await();
            index = buildIndexAndSave(masters);
            logger.info("Index built!");
        }

        logger.info("Waiting for candidates...");
        waitingResultLatch.await();

        logger.info("Get candidates, sorting...");
        Collections.sort(results, new StringComparator());

        // should persist to some file, but I log them to stdout
        logger.info("Get results, logging to stdout!");
        int lower = index.BinarySearch((int) (k + 1));
        int startIndex = (int) (k - index.getRangeSum(lower - 1)) + 1;
        for (int i = startIndex; i < startIndex + n; ++i) {
            logger.info(results.get(i));
        }
    }

    private void listenAndSetSocketChannel() throws IOException {
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress(port));

        // we need just one connection for a port
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

        socketChannel.read(readBuffer);
        bucket.readFromBuffer(readBuffer);
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

    // save to static field results
    private void readResults() throws IOException {
        ByteBuffer readBuffer = ByteBuffer.allocate(Constants.RESULT_BUFFER_SIZE);
        readBuffer.limit(4);
        socketChannel.read(readBuffer);
        int size = readBuffer.getInt();
        readBuffer.clear();

        int readSize = 0;
        while (readSize < size) {
            if (size - readSize > Constants.READ_BUFFER_SIZE) {
                readBuffer.clear();
            } else {
                readBuffer.limit(size - readSize);
            }
            readSize += socketChannel.read(readBuffer);
            readBuffer.flip();

            // process this buffer
            byte[] buffer = readBuffer.array();
            int pos = 0;
            while (pos < readBuffer.limit()) {
                int endPos = pos + 1;
                while (buffer[endPos] != '\n') { endPos++; }
                synchronized (results) {
                    results.add(new String(buffer, pos, endPos - pos));
                }
                pos = endPos + 1;
            }
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

            int lower = index.BinarySearch((int) (k + 1));
            int upper = index.BinarySearch((int) (k + n));

            requestRangeQueryAndReadResult(lower, upper);
            waitingResultLatch.countDown();

            close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
