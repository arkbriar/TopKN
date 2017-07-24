package com.alibaba.middleware.topkn.race;

import com.alibaba.middleware.topkn.race.communication.DataIndex;
import com.alibaba.middleware.topkn.race.sort.MergeSorter;
import com.alibaba.middleware.topkn.race.sort.StringComparator;
import com.alibaba.middleware.topkn.race.sort.buckets.BucketMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Shunjie Ding on 24/07/2017.
 */
public class TopknMasterCore {
    public static List<List<BucketMeta>> topKN(DataIndex left, DataIndex right, int k, int n) {
        List<BucketMeta> leftMetas = left.getMetas(), rightMetas = right.getMetas();

        int l = 0, r = 0;
        int count = 0;
        while (count < k) {
            BucketMeta leftMeta = leftMetas.get(l++), rightMeta = rightMetas.get(r++);
            count += leftMeta.getSize() + rightMeta.getSize();
        }

        List<List<BucketMeta>> res = new ArrayList<>(2);
        res.add(left.getMetasFromIndexWithAtLeastSize(l - 1, n));
        res.add(right.getMetasFromIndexWithAtLeastSize(r - 1, n));
        return res;
    }

    private static List<String> topKN(List<String> left, List<String> right, int k, int n) {
        List<String> merged = new MergeSorter(StringComparator.getSingle()).merge(left, right);

        return merged.subList(k - 1, k - 1 + n);
    }
}
