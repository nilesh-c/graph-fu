/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.preprocess;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class HashIdKeyPartitioner extends Partitioner<HashIdCompositeKey, Text> {

    HashPartitioner<LongWritable, Text> hashPartitioner = new HashPartitioner<LongWritable, Text>();
    LongWritable newKey = null;

    @Override
    public int getPartition(HashIdCompositeKey key, Text value, int numReduceTasks) {
        try {
            // Execute the default partitioner over the first part of the key
            newKey = new LongWritable(key.getKey());
            return hashPartitioner.getPartition(newKey, value, numReduceTasks);
        } catch (Exception e) {
            e.printStackTrace();
            return (int) (Math.random() * numReduceTasks); // this would return a random value in the range [0,numReduceTasks)
        }
    }
}