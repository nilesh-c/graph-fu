/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.normalizeids.partitiondict;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class PartitionDictMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private static final Logger LOG = Logger.getLogger(PartitionDictMapper.class);
    private int numChunks = 0;
    private IntWritable hashInt = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.numChunks = conf.getInt("numChunks", 256);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        String newId = tokenizer.nextToken();
        String rawId = tokenizer.nextToken();
        int hash = rawId.hashCode() % numChunks;
        if (hash < 0) {
            hash += numChunks;
        }
        hashInt.set(hash);
        context.write(hashInt, value);
    }
}