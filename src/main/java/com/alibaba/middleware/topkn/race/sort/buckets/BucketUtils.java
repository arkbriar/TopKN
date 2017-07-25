package com.alibaba.middleware.topkn.race.sort.buckets;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by Shunjie Ding on 24/07/2017.
 */
public class BucketUtils {
    public static void flushToDisk(final List<String> strings, String filePath) throws IOException {
        Path destFilePath = Paths.get(filePath);
        Files.createDirectories(destFilePath.getParent());

        File destFile = destFilePath.toFile();
        if (!destFile.exists()) {
            destFile.createNewFile();
        }

        FileOutputStream fileOutputStream = new FileOutputStream(destFile, true);

        for (String s : strings) {
            fileOutputStream.write(s.getBytes());
            fileOutputStream.write('\n');
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

    public static void restoreBucketData(BufferedBucket bucket, String blockFile) {}
}
