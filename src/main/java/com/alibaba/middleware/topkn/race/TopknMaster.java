package com.alibaba.middleware.topkn.race;

import com.google.common.primitives.Bytes;

import com.alibaba.middleware.topkn.race.comm.BucketBlockReadRequest;
import com.alibaba.middleware.topkn.race.comm.BucketBlockResult;
import com.alibaba.middleware.topkn.race.sort.DataIndex;
import com.alibaba.middleware.topkn.race.sort.MergeSorter;
import com.alibaba.middleware.topkn.race.sort.buckets.BucketMeta;
import com.alibaba.middleware.topkn.race.sort.buckets.BucketUtils;
import com.alibaba.middleware.topkn.race.sort.comparator.StringComparator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by Shunjie Ding on 25/07/2017.
 */
public class TopknMaster implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TopknMaster.class);

    private static long k;
    private static int n;

    private static int[] ports = {5527, 5528};

    private SocketChannel[] socketChannels = new SocketChannel[2];
    private DataIndex[] dataIndices = new DataIndex[2];
    private String resultFile = "res.txt";
    private int kInMergedBlocks = 0;

    public static void main(String[] args) {
        k = Long.valueOf(args[0]);
        n = Integer.valueOf(args[1]);

        TopknMaster master = new TopknMaster();
        master.run();
    }

    @Override
    public void run() {
        try {
            startMaster();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void startMaster() throws ExecutionException, InterruptedException, IOException {
        List<Future<SocketChannel>> futures = startServerSocketsAndAcceptRequest();
        for (int i = 0; i < futures.size(); ++i) {
            socketChannels[i] = futures.get(i).get();
        }

        List<Future<DataIndex>> dataIndicesFutures = readDataIndices();
        for (int i = 0; i < dataIndicesFutures.size(); ++i) {
            dataIndices[i] = dataIndicesFutures.get(i).get();
        }

        List<BucketBlockReadRequest> readRequests = findBlocksAndConstructBlockReadRequests(k, n);
        List<BucketBlockResult> blockResults = readBlocks(readRequests);

        List<String> merged = new MergeSorter(StringComparator.getInstance())
            .merge(blockResults.get(0).getBlock(),
                blockResults.get(1).getBlock(), kInMergedBlocks + n - 1);
        List<String> result = merged.subList(kInMergedBlocks - 1, kInMergedBlocks + n - 1);

        BucketUtils.flushToDisk(result, resultFile);
    }

    private List<Future<SocketChannel>> startServerSocketsAndAcceptRequest() {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        List<Future<SocketChannel>> futures = new ArrayList<>(2);
        for (int i = 0; i < 2; ++i) {
            futures.add(executorService.submit(new StartServerSocketTask(ports[i])));
        }
        return futures;
    }

    private List<Future<DataIndex>> readDataIndices() {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        List<Future<DataIndex>> futures = new ArrayList<>(2);
        for (int i = 0; i < 2; ++i) {
            futures.add(executorService.submit(new ReadDataIndexTask(socketChannels[i])));
        }
        return futures;
    }

    private List<BucketBlockResult> readBlocks(List<BucketBlockReadRequest> readRequests)
        throws ExecutionException, InterruptedException {
        List<Future<BucketBlockResult>> futures = new ArrayList<>(2);
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        for (int i = 0; i < 2; ++i) {
            futures.add(executorService.submit(
                new RequestAndReadDataBlocksTask(socketChannels[i], readRequests.get(i))));
        }

        List<BucketBlockResult> blockResults = new ArrayList<>(2);
        for (Future<BucketBlockResult> future : futures) {
            blockResults.add(future.get());
        }
        return blockResults;
    }

    private List<BucketBlockReadRequest> findBlocksAndConstructBlockReadRequests(long k, int n) {
        long prevBlocksTotalSize = 0;
        int blockIndexToRead = 0;
        for (; prevBlocksTotalSize < k && blockIndexToRead < 128 * 36; ++blockIndexToRead) {
            for (DataIndex dataIndex : dataIndices) {
                prevBlocksTotalSize += dataIndex.getMetas().get(blockIndexToRead).getSize();
            }
        }
        --blockIndexToRead;

        kInMergedBlocks = (int) (k - prevBlocksTotalSize);
        long sizeToRead = k - prevBlocksTotalSize + n - 1;

        List<BucketBlockReadRequest> readRequests = new ArrayList<>(2);
        for (DataIndex dataIndex : dataIndices) {
            List<BucketMeta> bucketMetas =
                dataIndex.getMetasFromIndexWithAtLeastSize(blockIndexToRead, sizeToRead);
            readRequests.add(new BucketBlockReadRequest((int) sizeToRead, bucketMetas));
        }
        return readRequests;
    }

    private class StartServerSocketTask implements Callable<SocketChannel> {
        private int port;

        StartServerSocketTask(int port) {
            this.port = port;
        }

        @Override
        public SocketChannel call() throws Exception {
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.bind(new InetSocketAddress(port));

            logger.info("Opening port " + port + " for connecting...");

            SocketChannel socketChannel = serverSocketChannel.accept();
            socketChannel.configureBlocking(true);

            return socketChannel;
        }
    }

    private class ReadDataIndexTask implements Callable<DataIndex> {
        private SocketChannel socketChannel;

        ReadDataIndexTask(SocketChannel socketChannel) {
            this.socketChannel = socketChannel;
        }

        @Override
        public DataIndex call() throws Exception {
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

            byte[] indexBytes = new byte[0];
            while (socketChannel.read(buffer) != -1) {
                buffer.flip();
                indexBytes = Bytes.concat(indexBytes, buffer.array());
                buffer.clear();
            }

            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(indexBytes, DataIndex.class);
        }
    }

    private class RequestAndReadDataBlocksTask implements Callable<BucketBlockResult> {
        private SocketChannel socketChannel;

        private BucketBlockReadRequest readRequest;

        RequestAndReadDataBlocksTask(
            SocketChannel socketChannel, BucketBlockReadRequest readRequest) {
            this.socketChannel = socketChannel;
            this.readRequest = readRequest;
        }

        private void sendRequest(BucketBlockReadRequest readRequest) throws IOException {
            ObjectMapper mapper = new ObjectMapper();
            byte[] requestBytes = mapper.writeValueAsBytes(readRequest);

            ByteBuffer buffer = ByteBuffer.allocate(requestBytes.length);
            buffer.put(requestBytes);
            buffer.flip();

            while (buffer.hasRemaining()) {
                socketChannel.write(buffer);
            }
        }

        private BucketBlockResult readResult() throws IOException {
            ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);
            byte[] blockBytes = new byte[0];

            while (socketChannel.read(buffer) != -1) {
                buffer.flip();
                blockBytes = Bytes.concat(blockBytes, buffer.array());
                buffer.clear();
            }

            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(blockBytes, BucketBlockResult.class);
        }

        @Override
        public BucketBlockResult call() throws Exception {
            sendRequest(readRequest);
            return readResult();
        }
    }
}
