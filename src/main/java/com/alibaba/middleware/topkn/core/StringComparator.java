package com.alibaba.middleware.topkn.core;

import java.util.Comparator;

/**
 * Created by Shunjie Ding on 28/07/2017.
 */
public class StringComparator implements Comparator<String> {
    @Override
    public int compare(String o1, String o2) {
        if (o1.length() != o2.length()) {
            return o1.length() < o2.length() ? -1 : 1;
        }

        for (int i = 0; i < o1.length(); ++i) {
            if (o1.codePointAt(i) < o2.codePointAt(i)) {
                return -1;
            } else if (o1.codePointAt(i) > o2.codePointAt(i)) {
                return 1;
            }
        }

        return 0;
    }
}
