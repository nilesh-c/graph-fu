/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.preprocess;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * This key is a composite key. The "actual" key is the "key". The secondary
 * sort will be performed against the "keyValue".
 */
public class HashIdCompositeKey implements WritableComparable<HashIdCompositeKey> {

    private long key;
    private String keyValue;

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public String getKeyValue() {
        return keyValue;
    }

    public void setKeyValue(String keyValue) {
        this.keyValue = keyValue;
    }

    public HashIdCompositeKey() {
    }

    public HashIdCompositeKey(long key, String keyValue) {
        this.key = key;
        this.keyValue = keyValue;
    }

    @Override
    public String toString() {
        return (new StringBuilder()).append(key).append(',').append(keyValue).toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key = WritableUtils.readVLong(in);
        keyValue = WritableUtils.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, key);
        WritableUtils.writeString(out, keyValue);
    }

    @Override
    public int compareTo(HashIdCompositeKey o) {
        if (key == o.key && (keyValue.compareTo(o.keyValue)) == 0) {
            return 0;
        } else {
            return -1;
        }
    }
}