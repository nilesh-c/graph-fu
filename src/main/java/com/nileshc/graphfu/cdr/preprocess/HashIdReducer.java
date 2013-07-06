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
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

/**
 *
 * @author nilesh
 */
public class HashIdReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    private static final Logger LOG = Logger.getLogger(HashIdReducer.class);
    private MultipleOutputs multipleOutputs = null;
    private String vidmap = "";
    private long splitsize = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        multipleOutputs = new MultipleOutputs<LongWritable, Text>(context);
        Configuration conf = context.getConfiguration();
        this.vidmap = conf.get("vidmap");
        this.splitsize = Long.parseLong(conf.get("mapred.line.input.format.linespermap"));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long baseid = key.get();
        int split = 0;
        String temp = "";
        for (Text value : values) {
            long newId = baseid + splitsize * split;
            multipleOutputs.write(new LongWritable(newId), value, vidmap);
            ++split;
        }
    }
}
