/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.preprocess.hashid;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HashIdKeyGroupingComparator extends WritableComparator {

    protected HashIdKeyGroupingComparator() {
        super(HashIdCompositeKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        HashIdCompositeKey key1 = (HashIdCompositeKey) w1;
        HashIdCompositeKey key2 = (HashIdCompositeKey) w2;
        int ret;
        if (key1.getKey() < key2.getKey()) {
            ret = -1;
        } else if (key1.getKey() > key2.getKey()) {
            ret = 1;
        } else {
            ret = 0;
        }
        return ret;
    }
}