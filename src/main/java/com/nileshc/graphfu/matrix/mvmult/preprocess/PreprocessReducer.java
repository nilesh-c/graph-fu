/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.mvmult.preprocess;

import com.nileshc.graphfu.cdr.normalizeids.hashid.HashIdReducer;
import com.nileshc.graphfu.matrix.io.MatrixElement;
import com.nileshc.graphfu.matrix.io.MatrixElementListWritable;
import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

/**
 *
 * @author nilesh
 */
public class PreprocessReducer extends Reducer<LongWritable, MatrixElement, LongWritable, MultRowIntermediate> {

    private static final Logger LOG = Logger.getLogger(PreprocessReducer.class);
    private MultRowIntermediate outputValue = new MultRowIntermediate();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        LOG.addAppender(new ConsoleAppender(new SimpleLayout(), "System.err"));
    }

    @Override
    public void reduce(LongWritable key, Iterable<MatrixElement> values, Context context) throws IOException, InterruptedException {
        outputValue.setMatrixElements(new MatrixElementListWritable());
        for (MatrixElement element : values) {
            LOG.info(element);
            if (element.isVector()) {
                outputValue.setVectorValue(element.getValue());
            } else {
                outputValue.getMatrixElements().add(element);
            }
        }
        outputValue.setVectorRow(key);
        if (outputValue.getVectorValue() == null) {
            outputValue.setVectorValue(new DoubleWritable(0));
        }
        LOG.info(outputValue);
        context.write(key, outputValue);
    }
}
