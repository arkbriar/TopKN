package com.alibaba.middleware.topkn;

import com.alibaba.middleware.topkn.core.Bucket;
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
        while (true) {
            try {
                logger.info("Connecting to master at " + masterHost + ":" + masterPort);
                connect();

                // listen requests and respond
                listen();

                close();
                return;
            } catch (ConnectException e) {
                // reconnect on ConnectException
                Thread.sleep(15);
            }
        }
    }

    private void connect() throws IOException {
        socketChannel = SocketChannel.open(new InetSocketAddress(masterHost, masterPort));
        socketChannel.configureBlocking(true);
    }

    private void listen() throws InterruptedException {
        Thread requestListener = new Thread(new RequestListener(socketChannel));
        requestListener.run();

        requestListener.join();
    }

    private void buildIndexAndSend() throws InterruptedException, IOException {
        logger.info("Building index...");
        buildIndex();
        logger.info("Index built.");

        logger.info("Sending index to master...");
        sendIndex();
        logger.info("Index sent.");
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
        ByteBuffer byteBuffer = ByteBuffer.allocate(4 * Constants.BUCKET_SIZE);
        Bucket.getGlobal().writeToBuffer(byteBuffer);
        byteBuffer.flip();

        while (byteBuffer.hasRemaining()) {
            socketChannel.write(byteBuffer);
        }
    }

    private void queryRangeAndSend(int lower, int upper) throws IOException, InterruptedException {
        ByteBuffer readWriteBuffer = ByteBuffer.allocate(Constants.RESULT_BUFFER_SIZE);

        logger.info("Get query request from master in [%d, %d], start querying...", lower, upper);

        // query and send results
        readWriteBuffer.putInt(0);
        queryRange(lower, upper, readWriteBuffer);

        logger.info("Query done! Sending...");
        readWriteBuffer.flip();
        // write the size
        readWriteBuffer.putInt(readWriteBuffer.limit() - 4);
        readWriteBuffer.position(0);
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

    private class RequestListener implements Runnable {
        private SocketChannel socketChannel;

        RequestListener(SocketChannel socketChannel) {
            this.socketChannel = socketChannel;
        }

        @Override
        public void run() {
            ByteBuffer readBuffer = ByteBuffer.allocate(Constants.READ_BUFFER_SIZE);
            readBuffer.limit(4);
            try {
                while (socketChannel.read(readBuffer) != -1) {
                    readBuffer.flip();
                    int op = readBuffer.getInt();
                    readBuffer.clear();

                    if (op == Constants.OP_INDEX) {
                        buildIndexAndSend();
                    } else if (op == Constants.OP_QUERY) {
                        readBuffer.clear();
                        // read the values
                        readBuffer.limit(8);
                        socketChannel.read(readBuffer);
                        int lower = readBuffer.getInt(), upper = readBuffer.getInt();

                        queryRangeAndSend(lower, upper);
                        // exit once result's sent
                        return;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
