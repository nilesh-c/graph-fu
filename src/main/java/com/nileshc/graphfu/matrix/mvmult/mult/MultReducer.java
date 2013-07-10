/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.mvmult.mult;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import com.nileshc.graphfu.matrix.io.MultiValueWritable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class MultReducer extends Reducer<LongWritable, MultiValueWritable, LongWritable, MultRowIntermediate> {
    
    private static final Logger LOG = Logger.getLogger(MultReducer.class);
    private MultipleOutputs multipleOutputs = null;
    private DoubleWritable doubleOutput = new DoubleWritable();
    private String outputPath = null;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        multipleOutputs = new MultipleOutputs<LongWritable, MultRowIntermediate>(context);
        Configuration conf = context.getConfiguration();
        this.outputPath = conf.get("matrixoutput");
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
    
    @Override
    public void reduce(LongWritable key, Iterable<MultiValueWritable> values, Context context) throws IOException, InterruptedException {
        double newVectorValue = 0;
        MultRowIntermediate outputValue = new MultRowIntermediate();
        for (MultiValueWritable value : values) {
            Writable writable = value.get();
            if (writable instanceof DoubleWritable) {
                newVectorValue += ((DoubleWritable) writable).get();
            } else {
                outputValue.getMatrixElements().add((MatrixElement) writable);
            }
        }
        outputValue.setVectorRow(key);
        doubleOutput.set(newVectorValue);
        outputValue.setVectorValue(doubleOutput);
        multipleOutputs.write(key, outputValue, outputPath);
    }
}
