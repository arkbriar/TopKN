package com.alibaba.middleware.topkn.race.utils;

import com.google.common.primitives.Bytes;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Created by Shunjie Ding on 26/07/2017.
 */
public class SocketUtils {

    private static final int READ_BUFFER_SIZE = 1024 * 1024;

    public static <T> T read(SocketChannel socketChannel, Class<T> clazz) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(READ_BUFFER_SIZE);

        buffer.limit(4);

        socketChannel.read(buffer);
        buffer.flip();
        int sizeToRead = buffer.getInt();
        buffer.clear();

        byte[] res = new byte[0];
        int sizeRead = 0;

        while (sizeRead < sizeToRead) {
            int sizeCurRead = socketChannel.read(buffer);
            sizeRead += sizeCurRead;
            buffer.flip();

            byte[] read = new byte[sizeCurRead];
            buffer.get(read);
            res = Bytes.concat(res, read);
            buffer.clear();
        }

        return new ObjectMapper().readValue(res, clazz);
    }

    public static <T> void write(SocketChannel socketChannel, T t)
        throws IOException {
        if (t == null) { return; }

        ObjectMapper mapper = new ObjectMapper();
        byte[] bytes = mapper.writeValueAsBytes(t);

        ByteBuffer buffer = ByteBuffer.allocate(bytes.length + 4);
        buffer.putInt(bytes.length);
        buffer.put(bytes);
        buffer.flip();

        while (buffer.hasRemaining()) {
            socketChannel.write(buffer);
        }
    }
}
