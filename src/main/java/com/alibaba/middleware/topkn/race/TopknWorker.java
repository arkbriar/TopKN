package com.alibaba.middleware.topkn.race;

import com.alibaba.middleware.topkn.race.comm.BucketBlockReadRequest;
import com.alibaba.middleware.topkn.race.comm.BucketBlockResult;
import com.alibaba.middleware.topkn.race.sort.BucketSorter;
import com.alibaba.middleware.topkn.race.sort.DataIndex;
import com.alibaba.middleware.topkn.race.sort.buckets.BucketMeta;
import com.alibaba.middleware.topkn.race.sort.buckets.BufferedBucket;
import com.alibaba.middleware.topkn.race.sort.comparator.StringComparator;
import com.alibaba.middleware.topkn.race.utils.SocketUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Shunjie Ding on 25/07/2017.
 */
public class TopknWorker implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TopknWorker.class);
    private static final int READ_BUFFER_SIZE = 1024 * 1024;
    private static String masterHostAddress;
    private static int masterPort;
    private static String dataDirPath;
    private static String indexStorePath;
    private SocketChannel socketChannel = null;

    private static void initProperties(String[] args) {
        masterHostAddress = args[0];
        masterPort = Integer.valueOf(args[1]);
        dataDirPath = args[2];
        indexStorePath = Paths.get(dataDirPath + "/index").toString();
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        initProperties(args);

        while (true) {
            try {
                TopknWorker worker = new TopknWorker();
                worker.startWork();
            } catch (RuntimeException e) {
                e.printStackTrace();
                Thread.sleep(100);
            }
        }
    }

    private static int getTotalSize(List<BucketMeta> metas) {
        int size = 0;
        for (BucketMeta meta : metas) {
            size += meta.getSize();
        }
        return size;
    }

    private static void mapToBucket(List<BufferedBucket> buckets, String s) {
        int strLen = s.length();
        char leadingCharacter = s.charAt(0);

        for (BufferedBucket bucket : buckets) {
            if (bucket.getStrLen() == strLen && bucket.getLeadingCharacter() == leadingCharacter) {
                bucket.add(s);
                break;
            }
        }
    }

    @Override
    public void run() {
        try {
            startWork();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void startWork() throws IOException {
        connect(masterHostAddress, masterPort);

        List<String> fileSplits = listTextFilesInDir(dataDirPath);

        logger.info("Reading index file " + indexStorePath + "/index.json");

        DataIndex dataIndex = readDataIndex(new File(indexStorePath + "/index.json"));
        if (dataIndex == null) {
            logger.info("Index file not found, reconstructing...");

            dataIndex = coarseGrainedSort(fileSplits, indexStorePath);
        }

        logger.info("Writing index to master...");
        writeDataIndexToMaster(dataIndex);

        logger.info("Reading block reading request from master...");
        BucketBlockReadRequest readRequest = readBucketBlockReadRequestFromMaster();

        logger.info("Reading blocks from disk...");
        List<String> blocks =
            readBlocksFromDisk(readRequest.getMetas(), fileSplits, readRequest.getN());

        logger.info("Writing blocks to master...");
        writeBucketBlocksToMaster(blocks);

        close();
    }

    private List<String> readBlocksFromDisk(
        List<BucketMeta> metas, List<String> dataSplits, int n) {
        List<BufferedBucket> buckets = new ArrayList<>(metas.size());
        for (BucketMeta meta : metas) {
            buckets.add(new BufferedBucket(meta, BufferedBucket.UNLIMITED));
        }

        List<String> res = new ArrayList<>(getTotalSize(metas));

        // Read all matching lines
        for (String dataSplit : dataSplits) {
            File file = new File(dataSplit);
            try {
                InputStreamReader streamReader = new InputStreamReader(new FileInputStream(file));
                BufferedReader reader = new BufferedReader(streamReader);

                String line;
                while ((line = reader.readLine()) != null) {
                    mapToBucket(buckets, line);
                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Sort all buckets and get first n records
        for (BufferedBucket bucket : buckets) {
            res.addAll(bucket.sort(StringComparator.getInstance()));
        }
        return res.subList(0, n);
    }

    private DataIndex readDataIndex(File indexFile) {
        if (!indexFile.exists()) { return null; }
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(indexFile, DataIndex.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void connect(String host, int port) throws IOException {
        logger.info("Begin to connect " + host + ":" + port);

        socketChannel = SocketChannel.open(new InetSocketAddress(host, port));
    }

    private void close() throws IOException {
        if (socketChannel != null) {
            socketChannel.close();
            socketChannel = null;
        }
    }

    private List<String> listTextFilesInDir(String dir) {
        List<String> files = new ArrayList<>();

        File dirFile = new File(dir);
        if (dirFile.isDirectory()) {
            File[] fileList = dirFile.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".txt");
                }
            });
            if (fileList == null) {
                return files;
            }
            for (File file : fileList) {
                files.add(file.getAbsolutePath());
            }
        }

        return files;
    }

    private DataIndex coarseGrainedSort(List<String> dataSplits, String storeDir) {
        BucketSorter sorter = new BucketSorter(storeDir, dataSplits);

        sorter.coarseGrainedSortInParallel();

        return sorter.getIndex();
    }

    private BucketBlockReadRequest readBucketBlockReadRequestFromMaster() throws IOException {
        return SocketUtils.read(socketChannel, BucketBlockReadRequest.class);
    }

    private void writeDataIndexToMaster(DataIndex index) throws IOException {
        SocketUtils.write(socketChannel, index);
    }

    private void writeBucketBlocksToMaster(List<String> blocks) throws IOException {
        BucketBlockResult result = new BucketBlockResult(blocks);
        SocketUtils.write(socketChannel, result);
    }
}
