package com.alibaba.middleware.topkn.race.comm;

import java.util.List;

/**
 * Created by Shunjie Ding on 25/07/2017.
 */
public class BucketBlockResult {
    private List<String> block = null;

    private BucketBlockResult() {}

    public BucketBlockResult(List<String> block) {
        this.block = block;
    }

    public List<String> getBlock() {
        return block;
    }
}
