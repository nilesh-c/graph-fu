/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.preprocess;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class PartitionEdgeMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private static final Logger LOG = Logger.getLogger(PartitionEdgeMapper.class);
    private int numChunks = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.numChunks = conf.getInt("numChunks", 256);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        String sourceId = tokenizer.nextToken();
        int hash = sourceId.hashCode() % numChunks;
        if (hash < 0) {
            hash += numChunks;
        }
        context.write(new IntWritable(hash), value);
    }
}