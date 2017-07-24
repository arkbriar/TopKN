package com.alibaba.middleware.topkn.race;

import com.alibaba.middleware.topkn.race.sort.BucketSorter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Shunjie Ding on 24/07/2017.
 */
public class TopknWorkerCore {
    private static final Logger logger = LoggerFactory.getLogger(TopknWorkerCore.class);

    protected TopknWorkerCore() {}

    public static void coarseGrainedSort(String dataDirPath, String storeDir) {
        List<String> dataSplits = listTextFilesInDir(dataDirPath);
        BucketSorter sorter = new BucketSorter(storeDir, dataSplits);

        sorter.coarseGrainedSort();
    }

    private static List<String> listTextFilesInDir(String dir) {
        List<String> files = new ArrayList<>();

        File dirFile = new File(dir);
        if (dirFile.isDirectory()) {
            File[] fileList = dirFile.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.endsWith(".txt");
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

    public static void main(String[] args) {
        String dataDirPath = args[0];
        String storeDir = args[1];

        logger.info("Sorting on data splits in " + dataDirPath
            + ", and all results will be persisted under " + storeDir);
        coarseGrainedSort(dataDirPath, storeDir);
    }
}
