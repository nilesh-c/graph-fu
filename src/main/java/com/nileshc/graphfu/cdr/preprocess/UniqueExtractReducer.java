/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.preprocess;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
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
public class UniqueExtractReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    private static final Logger LOG = Logger.getLogger(UniqueExtractReducer.class);
    private MultipleOutputs multipleOutputs = null;
    private String vidmap = "";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
        Configuration conf = context.getConfiguration();
        this.vidmap = conf.get("vdata");
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        multipleOutputs.write(key, NullWritable.get(), vidmap);
    }
}