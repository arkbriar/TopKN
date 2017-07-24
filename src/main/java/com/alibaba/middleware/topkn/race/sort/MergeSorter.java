package com.alibaba.middleware.topkn.race.sort;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Created by Shunjie Ding on 24/07/2017.
 */
public class MergeSorter {
    private Comparator<String> comparator;

    public MergeSorter(Comparator<String> comparator) {
        this.comparator = comparator;
    }

    public List<String> merge(List<String> lhs, List<String> rhs) {
        int i = 0, j = 0;
        List<String> res = new ArrayList<>(lhs.size() + rhs.size());

        while (i < lhs.size() || j < rhs.size()) {
            if (i >= lhs.size()) {
                res.add(rhs.get(j++));
            } else if (j >= rhs.size()) {
                res.add(lhs.get(i++));
            } else {
                String l = lhs.get(i), r = rhs.get(j);
                int c = comparator.compare(l, r);
                if (c <= 0) {
                    res.add(l);
                    ++i;
                } else {
                    res.add(r);
                    ++j;
                }
            }
        }

        return res;
    }
}
