/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.normalizeids.hashid;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HashIdCompositeKeyComparator extends WritableComparator {

    protected HashIdCompositeKeyComparator() {
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

        if (ret == 0) {
            ret = key1.getKeyValue().compareTo(key2.getKeyValue());
        }
        return ret;
    }
}