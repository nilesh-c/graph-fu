/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.mvmult.mult;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import com.nileshc.graphfu.matrix.io.MatrixElementListWritable;
import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import com.nileshc.graphfu.matrix.io.MultiValueWritable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

/**
 *
 * @author nilesh
 */
public class MultReducer extends Reducer<LongWritable, MultiValueWritable, LongWritable, MultRowIntermediate> {

    private static final Logger LOG = Logger.getLogger(MultReducer.class);
    private DoubleWritable doubleOutput = new DoubleWritable();
    private MultRowIntermediate outputValue = new MultRowIntermediate();
    private double rightHandValue;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LOG.addAppender(new ConsoleAppender(new SimpleLayout(), "System.err"));
        Configuration conf = context.getConfiguration();
        rightHandValue = conf.getFloat("righthand", 0);
    }

    @Override
    public void reduce(LongWritable key, Iterable<MultiValueWritable> values, Context context) throws IOException, InterruptedException {
        double newVectorValue = 0;
        for (MultiValueWritable value : values) {
            Writable writable = value.get();
            if (writable instanceof DoubleWritable) {
                newVectorValue += ((DoubleWritable) writable).get();
            } else {
                outputValue.setMatrixElements((MatrixElementListWritable) writable);
            }
        }
        outputValue.setVectorRow(key);
        doubleOutput.set(newVectorValue + rightHandValue);
        outputValue.setVectorValue(doubleOutput);
        LOG.info("After adding " + rightHandValue + " output becomes: " + outputValue);
        context.write(key, outputValue);
    }
}
