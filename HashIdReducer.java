/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.preprocess;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *
 * @author nilesh
 */
public class HashIdReducer extends Reducer<LongWritable, Text, NullWritable, NullWritable> {

    protected MultipleOutputs multipleOutputs = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
    }

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String vidmap = conf.get("vidmap");
        long splitsize = Long.parseLong(conf.get("mapred.line.input.format.linespermap"));
        long baseid = key.get();
        int split = 0;
        for (Text value : values) {
            long newId = baseid + splitsize * split;
            multipleOutputs.write(vidmap, new LongWritable(newId), value);
            split++;
        }
    }
}
