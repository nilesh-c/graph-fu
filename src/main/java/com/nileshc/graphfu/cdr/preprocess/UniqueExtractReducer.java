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

    private static final Logger LOG = Logger.getLogger(PreprocessRunner.class);
    protected MultipleOutputs multipleOutputs = null;

    @Override
    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<Text, NullWritable>(context);
    }

    @Override
    public void cleanup(Reducer.Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String vidmap = conf.get("vdata");
        multipleOutputs.write(key, NullWritable.get(), vidmap);
        context.write(key, NullWritable.get());
    }
}