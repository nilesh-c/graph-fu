/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.pagerank.vectornorm;

import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import com.nileshc.graphfu.matrix.mvmult.mult.MultReducer;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class VectorElementSumReducer extends Reducer<LongWritable, MultRowIntermediate, NullWritable, DoubleWritable> {

    private static final Logger LOG = Logger.getLogger(MultReducer.class);
    private double vectorSum = 0;

    @Override
    public void reduce(LongWritable key, Iterable<MultRowIntermediate> values, Context context) throws IOException, InterruptedException {
        for (MultRowIntermediate value : values) {
            vectorSum += value.getVectorValue().get();
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), new DoubleWritable(vectorSum));
    }
}
