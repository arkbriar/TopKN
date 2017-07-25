package com.alibaba.middleware.topkn.race;

import com.alibaba.middleware.topkn.race.comm.BucketBlockWrapper;
import com.alibaba.middleware.topkn.race.comm.BucketMetasWrapper;
import com.alibaba.middleware.topkn.race.sort.DataIndex;
import com.alibaba.middleware.topkn.race.sort.MergeSorter;
import com.alibaba.middleware.topkn.race.sort.StringComparator;
import com.alibaba.middleware.topkn.race.sort.buckets.BucketMeta;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Shunjie Ding on 24/07/2017.
 */
public class TopknMasterCore {
    private static final Logger logger = LoggerFactory.getLogger(TopknMasterCore.class);

    BlockingQueue<ByteBuffer>[] queues;

    public TopknMasterCore(BlockingQueue<ByteBuffer>[] queues) {
        this.queues = queues;
    }

    public int findBlocks(DataIndex left, DataIndex right, int k, int n, List<BucketMeta>[] metas) {
        List<BucketMeta> leftMetas = left.getMetas(), rightMetas = right.getMetas();

        int i = 0;
        int count = 0;
        while (count < k) {
            BucketMeta leftMeta = leftMetas.get(i++), rightMeta = rightMetas.get(i++);
            count += leftMeta.getSize() + rightMeta.getSize();
        }

        metas[0] = left.getMetasFromIndexWithAtLeastSize(i - 1, k - count + n - 1);
        metas[1] = right.getMetasFromIndexWithAtLeastSize(i - 1, k - count + n - 1);
        return count - leftMetas.get(i - 1).getSize() - rightMetas.get(i - 1).getSize();
    }

    private List<String> topKN(List<String> left, List<String> right, int k, int n) {
        List<String> merged =
            new MergeSorter(StringComparator.getSingle()).merge(left, right, k + n - 1);
        return merged.subList(k - 1, k - 1 + n);
    }

    private List<String> topKN(DataIndex[] dataIndices, int k, int n) {
        List<BucketMeta>[] metas = new List[2];
        ;
        int prevBlocksSize = findBlocks(dataIndices[0], dataIndices[1], k, n, metas);

        List<String>[] blocks = new List[2];
        readDataBlocks(metas, blocks, k - prevBlocksSize + n - 1);

        return topKN(blocks[0], blocks[1], k - prevBlocksSize, n);
    }

    private List<String> topKN(int k, int n) {
        pushKN(k, n);

        DataIndex[] dataIndices = new DataIndex[2];
        readDataIndices(dataIndices);

        return topKN(dataIndices, k, n);
    }

    private void pushKN(int k, int n) {
        // TODO
    }

    private DataIndex readDataIndex(BlockingQueue<ByteBuffer> queue) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            ByteBuffer buffer = queue.take();
            return mapper.readValue(buffer.array(), DataIndex.class);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void readDataIndices(DataIndex[] dataIndices) {
        dataIndices[0] = readDataIndex(queues[0]);
        dataIndices[1] = readDataIndex(queues[1]);
    }

    private void pushReadingBlockRequest(
        BlockingQueue<ByteBuffer> queue, List<BucketMeta> metas, int n) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            byte[] bytes = mapper.writeValueAsBytes(new BucketMetasWrapper(metas));
            ByteBuffer buffer = ByteBuffer.allocate(bytes.length + 8);
            buffer.clear();
            buffer.putInt(n);
            buffer.put(bytes);
            buffer.flip();
            queue.put(buffer);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private List<String> readDataBlock(BlockingQueue<ByteBuffer> queue) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            ByteBuffer buffer = queue.take();
            BucketBlockWrapper wrapper = mapper.readValue(buffer.array(), BucketBlockWrapper.class);
            return wrapper.block;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (JsonParseException e) {
            e.printStackTrace();
        } catch (JsonMappingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private void readDataBlocks(List<BucketMeta>[] metas, List<String>[] blocks, int n) {
        pushReadingBlockRequest(queues[0], metas[0], n);
        pushReadingBlockRequest(queues[1], metas[1], n);
        blocks[0] = readDataBlock(queues[0]);
        blocks[1] = readDataBlock(queues[1]);
    }
}
