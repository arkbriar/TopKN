package com.alibaba.middleware.topkn.race;

import com.alibaba.middleware.topkn.race.comm.BucketBlockReadRequest;
import com.alibaba.middleware.topkn.race.comm.BucketBlockWrapper;
import com.alibaba.middleware.topkn.race.comm.BucketMetasWrapper;
import com.alibaba.middleware.topkn.race.sort.BucketSorter;
import com.alibaba.middleware.topkn.race.sort.DataIndex;
import com.alibaba.middleware.topkn.race.sort.StringComparator;
import com.alibaba.middleware.topkn.race.sort.buckets.BucketMeta;
import com.alibaba.middleware.topkn.race.sort.buckets.BufferedBucket;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Shunjie Ding on 24/07/2017.
 */
public class TopknWorkerCore implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(TopknWorkerCore.class);

    private static final int READ_BUFFER_SIZE = 1024 * 1024;
    private static final int WRITE_BUFFER_SIZE = 1024;
    private SocketChannel socketChannel;

    public TopknWorkerCore(SocketChannel socketChannel) {
        this.socketChannel = socketChannel;
    }

    public static void main(String[] args) {
        String dataDirPath = args[0];
        String storeDir = args[1];

        logger.info("Sorting on data splits in " + dataDirPath
            + ", and all results will be persisted under " + storeDir);

        TopknWorkerCore core = new TopknWorkerCore(null);
        core.coarseGrainedSort(dataDirPath, storeDir);
    }

    private DataIndex coarseGrainedSort(String dataDirPath, String storeDir) {
        List<String> dataSplits = listTextFilesInDir(dataDirPath);
        return coarseGrainedSort(dataSplits, storeDir);
    }

    private DataIndex coarseGrainedSort(List<String> dataSplits, String storeDir) {
        BucketSorter sorter = new BucketSorter(storeDir, dataSplits);

        sorter.coarseGrainedSortInParallel();

        return sorter.getIndex();
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

    private DataIndex readDataIndex(File indexFile) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(indexFile, DataIndex.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void run() {
        // TODO
    }

    public void run(String dataDirPath, String storeDirPath) {
        List<String> dataSplits = listTextFilesInDir(dataDirPath);

        DataIndex index = readDataIndex(new File(storeDirPath + "/index.json"));
        if (index == null) {
            index = coarseGrainedSort(dataSplits, storeDirPath);
        }

        pushDataIndexToMaster(index);

        BucketBlockReadRequest readRequest = readBucketMetas();

        List<String> strings = getBlocks(readRequest.getMetas(), dataSplits, readRequest.getN());

        pushBlocks(strings);
    }

    private BucketBlockReadRequest readBucketMetas() {
        ByteBuffer buffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
        buffer.clear();

        int n = 0;
        List<BucketMeta> metas = new ArrayList<>(0);
        try {
            socketChannel.read(buffer);
            buffer.flip();
            n = buffer.getInt();
            ObjectMapper mapper = new ObjectMapper();
            BucketMetasWrapper wrapper = mapper.readValue(buffer.array(), BucketMetasWrapper.class);
            metas = wrapper.metas;
            buffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new BucketBlockReadRequest(n, metas);
    }

    private void pushDataIndexToMaster(DataIndex index) {
        ByteBuffer buffer = ByteBuffer.allocate(WRITE_BUFFER_SIZE);
        buffer.clear();

        try {
            ObjectMapper mapper = new ObjectMapper();
            buffer.put(mapper.writeValueAsBytes(index));
            buffer.flip();

            while (buffer.hasRemaining()) {
                socketChannel.write(buffer);
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void mapToBucket(List<BufferedBucket> buckets, String s) {
        int strLen = s.length();
        char leadingCharacter = s.charAt(0);

        for (BufferedBucket bucket : buckets) {
            if (bucket.getStrLen() == strLen && bucket.getLeadingCharacter() == leadingCharacter) {
                bucket.add(s);
                break;
            }
        }
    }

    private int getTotalSize(List<BucketMeta> metas) {
        int size = 0;
        for (BucketMeta meta : metas) {
            size += meta.getSize();
        }
        return size;
    }

    private List<String> getBlocks(List<BucketMeta> metas, List<String> dataSplits, int n) {
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
            res.addAll(bucket.sort(StringComparator.getSingle()));
        }
        return res.subList(0, n);
    }

    private void pushBlocks(List<String> strings) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            byte[] bytes = mapper.writeValueAsBytes(new BucketBlockWrapper(strings));
            ByteBuffer buffer = ByteBuffer.allocate(bytes.length);
            buffer.clear();
            buffer.put(bytes);
            buffer.flip();

            while (buffer.hasRemaining()) {
                socketChannel.write(buffer);
            }
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
