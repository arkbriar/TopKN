package com.alibaba.middleware.topkn.refactor.process;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Shunjie Ding on 27/07/2017.
 */
public abstract class BufferLineProcessorFactory {
    public abstract BufferLineProcessor newInstance(
        BlockingQueue<ByteBuffer> p, BlockingQueue<ByteBuffer> q);
}
