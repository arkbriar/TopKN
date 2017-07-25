package com.alibaba.middleware.topkn.race.comm;

import com.alibaba.middleware.topkn.race.sort.buckets.BucketMeta;

import java.util.List;

/**
 * Created by Shunjie Ding on 25/07/2017.
 */
public class BucketMetasWrapper {
    public List<BucketMeta> metas = null;

    public BucketMetasWrapper() {}

    public BucketMetasWrapper(List<BucketMeta> metas) {
        this.metas = metas;
    }
}
