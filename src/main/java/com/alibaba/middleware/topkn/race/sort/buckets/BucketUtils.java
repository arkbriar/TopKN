package com.alibaba.middleware.topkn.race.sort.buckets;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by Shunjie Ding on 24/07/2017.
 */
public class BucketUtils {

    public static void FlushToDisk(final List<String> strings, String filePath) throws IOException {
        Path destFilePath = Paths.get(filePath);
        Files.createDirectories(destFilePath.getParent());

        File destFile = destFilePath.toFile();
        if (!destFile.exists()) {
            destFile.createNewFile();
        }

        //获取可写的file channel；使用FileInputStream是只读
        RandomAccessFile raf = new RandomAccessFile(destFile, "rw");
        //设置指针位置为文件末尾
        long fileLength = raf.length();
        raf.seek(fileLength);

        for (String s : strings) {
            raf.writeBytes(s + "\n");
        }
    }

    static void saveBucketMeta(BucketMeta meta, String metaFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.writeValue(new File(metaFile), meta);
    }

    static BucketMeta readBucketMeta(String metaFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new File(metaFile), BucketMeta.class);
    }

    public static BufferedBucket restoreEmptyBucket(String metaFile) throws IOException {
        BucketMeta meta = BucketUtils.readBucketMeta(metaFile);
        return new BufferedBucket(meta, BufferedBucket.UNLIMITED);
    }

    public static void restoreBucketData(BufferedBucket bucket, String blockFile) {

    }
}
