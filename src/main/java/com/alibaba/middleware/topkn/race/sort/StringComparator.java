package com.alibaba.middleware.topkn.race.sort;

import java.util.Comparator;

/**
 * Created by Shunjie Ding on 24/07/2017.
 */
public class StringComparator implements Comparator<String> {
    private static final Comparator<String> single = new StringComparator();

    public static Comparator<String> getSingle() {
        return single;
    }

    @Override
    public int compare(String o1, String o2) {
        if (o1.length() != o2.length()) { return o1.length() < o2.length() ? -1 : 1; }

        for (int i = 0; i < o1.length(); ++i) {
            int c1 = o1.codePointAt(i), c2 = o2.codePointAt(i);
            if (c1 < c2) {
                return -1;
            } else if (c1 > c2) {
                return 1;
            }
        }

        return 0;
    }
}
