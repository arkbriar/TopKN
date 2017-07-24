package com.alibaba.middleware.topkn.race;

import com.alibaba.middleware.topkn.race.communication.DataIndex;
import com.alibaba.middleware.topkn.race.sort.MergeSorter;
import com.alibaba.middleware.topkn.race.sort.StringComparator;
import com.alibaba.middleware.topkn.race.sort.buckets.BucketMeta;

import java.util.List;

/**
 * Created by Shunjie Ding on 24/07/2017.
 */
public class TopknMasterCore {
    public static int findBlocks(
        DataIndex left, DataIndex right, int k, int n, List<BucketMeta>[] metas) {
        List<BucketMeta> leftMetas = left.getMetas(), rightMetas = right.getMetas();

        int i = 0;
        int count = 0;
        while (count < k) {
            BucketMeta leftMeta = leftMetas.get(i++), rightMeta = rightMetas.get(i++);
            count += leftMeta.getSize() + rightMeta.getSize();
        }

        metas[0] = left.getMetasFromIndexWithAtLeastSize(i - 1, n);
        metas[1] = right.getMetasFromIndexWithAtLeastSize(i - 1, n);
        return count - leftMetas.get(i - 1).getSize() - rightMetas.get(i - 1).getSize();
    }

    private static List<String> topKN(List<String> left, List<String> right, int k, int n) {
        List<String> merged =
            new MergeSorter(StringComparator.getSingle()).merge(left, right, k + n - 1);
        return merged.subList(k - 1, k - 1 + n);
    }

    private static List<String> topKN(DataIndex[] dataIndices, int k, int n) {
        List<BucketMeta>[] metas = new List[2];
        ;
        int prevBlocksSize = findBlocks(dataIndices[0], dataIndices[1], k, n, metas);

        List<String>[] blocks = new List[2];
        readDataBlocks(metas, blocks);

        return topKN(blocks[0], blocks[1], k - prevBlocksSize, n);
    }

    private static List<String> topKN(int k, int n) {
        pushKN(k, n);

        DataIndex[] dataIndices = new DataIndex[2];
        readDataIndices(dataIndices);

        return topKN(dataIndices, k, n);
    }

    public static void pushKN(int k, int n) {
        // TODO
    }

    public static void readDataIndices(DataIndex[] dataIndices) {
        // TODO
    }

    public static void readDataBlocks(List<BucketMeta>[] metas, List<String>[] blocks) {
        // TODO
    }
}
