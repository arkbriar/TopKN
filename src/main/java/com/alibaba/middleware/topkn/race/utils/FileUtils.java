package com.alibaba.middleware.topkn.race.utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Created by Shunjie Ding on 26/07/2017.
 */
public class FileUtils {
    public static void flushToDisk(final List<String> strings, String filePath) throws IOException {
        Path destFilePath = Paths.get(filePath);
        Path parentPath = destFilePath.toAbsolutePath().getParent();
        if (parentPath != null) { Files.createDirectories(parentPath); }

        File destFile = destFilePath.toFile();
        if (!destFile.exists()) {
            destFile.createNewFile();
        }

        FileOutputStream fileOutputStream = new FileOutputStream(destFile);

        for (String s : strings) {
            fileOutputStream.write(s.getBytes());
            fileOutputStream.write('\n');
        }
    }
}
