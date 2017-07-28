package com.alibaba.middleware.topkn.core;

import com.alibaba.middleware.topkn.Constants;
import com.alibaba.middleware.topkn.utils.Logger;
import com.alibaba.middleware.topkn.utils.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by Shunjie Ding on 28/07/2017.
 */
public class Index {
    private static final Logger logger = LoggerFactory.getLogger(Index.class);

    private static final byte[] MAGIC_CODE = new byte[] {0x0, 0x0, 0x0, 0x0, 0xb, 0xc, 0x1, 0xd};
    // count of index i is the sum of sizes of buckets from [0, i]
    private int[] bucketRangeSums = new int[Constants.BUCKET_SIZE];

    private Index() {}

    public static Index getIndex(List<Bucket> buckets) {
        Index index = new Index();
        int[] localRangeSums = new int[Constants.BUCKET_SIZE];
        for (int i = 0; i < buckets.size(); ++i) {
            buckets.get(i).getRangeSums(localRangeSums);
            for (int j = 0; j < Constants.BUCKET_SIZE; ++j) {
                index.bucketRangeSums[j] += localRangeSums[j];
            }
        }
        return index;
    }

    public static Index readFromFile(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            logger.warn("Index file %s does not exists!", filePath);
            return null;
        }
        if (file.length() != MAGIC_CODE.length + Constants.BUCKET_SIZE * 4) {
            logger.warn("File length mismatch! %s is not an index file!", filePath);
            return null;
        }

        Index index = null;
        // read magic code
        try (FileInputStream fileInputStream = new FileInputStream(file)) {
            byte[] magicCode = new byte[MAGIC_CODE.length];
            fileInputStream.read(magicCode);

            // verify
            for (int i = 0; i < MAGIC_CODE.length; ++i) {
                if (magicCode[i] != MAGIC_CODE[i]) {
                    logger.warn("%s isn't a valid index file!", filePath);
                    return null;
                }
            }

            index = new Index();

            byte[] readBuffer = new byte[fileInputStream.available()];
            fileInputStream.read(readBuffer);
            ByteBuffer buffer = ByteBuffer.wrap(readBuffer);
            for (int i = 0; i < Constants.BUCKET_SIZE; ++i) {
                index.bucketRangeSums[i] = buffer.getInt();
            }
        }

        return index;
    }

    public static void writeToFile(Index index, String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            file.createNewFile();
        }

        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            fileOutputStream.write(MAGIC_CODE);

            ByteBuffer writeBuffer = ByteBuffer.allocate(4 * Constants.BUCKET_SIZE);
            for (int i = 0; i < Constants.BUCKET_SIZE; ++i) {
                writeBuffer.putInt(index.bucketRangeSums[i]);
            }
            fileOutputStream.write(writeBuffer.array());
        }
    }

    public int BinarySearch(int k) {
        int start = 0, end = Constants.BUCKET_SIZE - 1;
        while (start < end) {
            int mid = (start + end) / 2;
            if (bucketRangeSums[mid] < k) {
                start = mid + 1;
            } else if (bucketRangeSums[mid] > k) {
                end = mid;
            } else {
                return mid;
            }
        }
        return bucketRangeSums[start] >= k ? start : start + 1;
    }

    public int getRangeSum(int i) {
        return bucketRangeSums[i];
    }
}
