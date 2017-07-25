package com.alibaba.middleware.topkn.race.sort;

import com.alibaba.middleware.topkn.race.sort.buckets.BucketMeta;
import com.alibaba.middleware.topkn.race.sort.buckets.BufferedBucket;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Shunjie Ding on 24/07/2017.
 */
public class DataIndex {
    private List<BucketMeta> metas = new ArrayList<>(128 * 36);

    public DataIndex(Map<Integer, Map<Character, BufferedBucket>> buckets) {
        int count = 0;
        for (Integer i : buckets.keySet()) {
            Map<Character, BufferedBucket> sBuckets = buckets.get(i);
            for (Character c : sBuckets.keySet()) {
                metas.add(sBuckets.get(c).getMeta());
            }
        }
    }

    public static DataIndex restore(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new File(filePath), DataIndex.class);
    }

    public List<BucketMeta> getMetas() {
        return metas;
    }

    public List<BucketMeta> getMetasFromIndexWithAtLeastSize(int i, long n) {
        List<BucketMeta> res = new ArrayList<>();
        while (n > 0) {
            BucketMeta meta = metas.get(i++);
            if (meta.getSize() != 0) {
                res.add(meta);
                n -= meta.getSize();
            }
        }
        return res;
    }

    public void flushToDisk(String filePath) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        File file = new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
        }
        mapper.writeValue(file, this);
    }
}
