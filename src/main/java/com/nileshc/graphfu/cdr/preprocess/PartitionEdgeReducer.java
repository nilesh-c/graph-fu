/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.preprocess;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class PartitionEdgeReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

    private static final Logger LOG = Logger.getLogger(PartitionEdgeReducer.class);

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(NullWritable.get(), value);
        }
    }
}