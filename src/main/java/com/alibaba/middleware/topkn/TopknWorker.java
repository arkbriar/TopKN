package com.alibaba.middleware.topkn;

import com.alibaba.middleware.topkn.core.Buckets;
import com.alibaba.middleware.topkn.core.FileSegmentLoader;
import com.alibaba.middleware.topkn.core.IndexBuilder;
import com.alibaba.middleware.topkn.core.QueryExecutor;
import com.alibaba.middleware.topkn.utils.FileUtils;
import com.alibaba.middleware.topkn.utils.Logger;
import com.alibaba.middleware.topkn.utils.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Shunjie Ding on 27/07/2017.
 */
public class TopknWorker {
    private static final Logger logger = LoggerFactory.getLogger(TopknWorker.class);

    private static String dataDir;

    private String masterHost;
    private int masterPort;

    private SocketChannel socketChannel;

    public TopknWorker(String masterHost, int masterPort) {
        this.masterHost = masterHost;
        this.masterPort = masterPort;
    }

    public static void main(String[] args) throws Exception {
        String masterHost = args[0];
        int masterPort = Integer.valueOf(args[1]);
        dataDir = args[2];

        new TopknWorker(masterHost, masterPort).run();
    }

    private void run() throws InterruptedException, IOException {
        boolean indexBuilt = false;
        while (true) {
            try {
                logger.info("Connecting to master at " + masterHost + ":" + masterPort);
                connect();

                logger.info("Building index...");
                if (!indexBuilt) {
                    buildIndex();
                    indexBuilt = true;
                }
                logger.info("Index built.");

                logger.info("Sending index to master...");
                sendIndex();

                queryRangeAndSend();

                close();
                return;
            } catch (ConnectException e) {
                // reconnect on ConnectException
                if (!indexBuilt) {
                    Buckets.getInstance().clear();
                }
                Thread.sleep(15);
            }
        }
    }

    private void connect() throws IOException {
        socketChannel = SocketChannel.open(new InetSocketAddress(masterHost, masterPort));
        socketChannel.configureBlocking(true);
    }

    private void buildIndex() throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(Constants.NUM_CORES);

        List<String> filePaths = FileUtils.listTextFilesInDir(dataDir);
        FileSegmentLoader fileSegmentLoader =
            new FileSegmentLoader(filePaths, Constants.SEGMENT_SIZE);
        for (int i = 0; i < Constants.NUM_CORES; ++i) {
            executorService.submit(new IndexBuilder(fileSegmentLoader, Constants.SEGMENT_SIZE));
        }

        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    private void sendIndex() throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 * Constants.BUCKET_SIZE);
        byteBuffer.putInt(Constants.OP_INDEX);
        Buckets.getInstance().writeToBuffer(byteBuffer);
        byteBuffer.flip();

        while (byteBuffer.hasRemaining()) {
            socketChannel.write(byteBuffer);
        }
    }

    private void queryRangeAndSend() throws IOException, InterruptedException {
        // get query request
        ByteBuffer readWriteBuffer = ByteBuffer.allocate(Constants.RESULT_BUFFER_SIZE);

        // read op code
        readWriteBuffer.limit(4);
        socketChannel.read(readWriteBuffer);
        readWriteBuffer.flip();
        int op = readWriteBuffer.getInt();
        assert op == Constants.OP_QUERY;
        readWriteBuffer.clear();

        // read the values
        readWriteBuffer.limit(8);
        socketChannel.read(readWriteBuffer);

        int lower = readWriteBuffer.getInt(), upper = readWriteBuffer.getInt();

        logger.info("Get query request from master in [%d, %d], starting...", lower, upper);

        // query and send results
        readWriteBuffer.clear();
        readWriteBuffer.putInt(Constants.OP_QUERY);
        queryRange(lower, upper, readWriteBuffer);

        readWriteBuffer.flip();
        logger.info("Query done! Sending...");

        while (readWriteBuffer.hasRemaining()) {
            socketChannel.write(readWriteBuffer);
        }
    }

    private void queryRange(int lower, int upper, ByteBuffer resultBuffer)
        throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(Constants.NUM_CORES);

        List<String> filePaths = FileUtils.listTextFilesInDir(dataDir);
        FileSegmentLoader fileSegmentLoader =
            new FileSegmentLoader(filePaths, Constants.SEGMENT_SIZE);
        for (int i = 0; i < Constants.NUM_CORES; ++i) {
            QueryExecutor executor = new QueryExecutor(
                fileSegmentLoader, Constants.SEGMENT_SIZE, lower, upper, resultBuffer);
            executorService.submit(executor);
        }

        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }

    private void close() throws IOException {
        socketChannel.close();
        socketChannel = null;
    }
}
