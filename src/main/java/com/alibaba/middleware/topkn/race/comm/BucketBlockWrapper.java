package com.alibaba.middleware.topkn.race.comm;

import java.util.List;

/**
 * Created by Shunjie Ding on 25/07/2017.
 */
public class BucketBlockWrapper {
    public List<String> block = null;

    public BucketBlockWrapper() {}

    public BucketBlockWrapper(List<String> block) {
        this.block = block;
    }
}
