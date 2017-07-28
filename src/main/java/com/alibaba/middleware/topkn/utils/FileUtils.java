package com.alibaba.middleware.topkn.utils;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Shunjie Ding on 27/07/2017.
 */
public class FileUtils {
    public static List<String> listTextFilesInDir(String dir) {
        List<String> files = new ArrayList<>();

        File dirFile = new File(dir);
        if (dirFile.isDirectory()) {
            File[] fileList = dirFile.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.matches("split(\\d+).txt");
                }
            });
            if (fileList == null) {
                return files;
            }
            for (File file : fileList) {
                files.add(file.getAbsolutePath());
            }
        }

        return files;
    }
}
