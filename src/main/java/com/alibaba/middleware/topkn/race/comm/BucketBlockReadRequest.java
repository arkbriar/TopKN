package com.alibaba.middleware.topkn.race.comm;

import com.alibaba.middleware.topkn.race.sort.buckets.BucketMeta;

import java.util.List;

/**
 * Created by Shunjie Ding on 25/07/2017.
 */
public class BucketBlockReadRequest {
    private int n;

    private List<BucketMeta> metas;

    private BucketBlockReadRequest() {}

    public BucketBlockReadRequest(int n, List<BucketMeta> metas) {
        this.n = n;
        this.metas = metas;
    }

    public int getN() {
        return n;
    }

    public List<BucketMeta> getMetas() {
        return metas;
    }
}
