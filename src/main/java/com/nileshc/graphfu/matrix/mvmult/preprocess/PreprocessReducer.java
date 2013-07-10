/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.mvmult.preprocess;

import com.nileshc.graphfu.cdr.normalizeids.hashid.HashIdReducer;
import com.nileshc.graphfu.matrix.io.MatrixElement;
import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class PreprocessReducer extends Reducer<LongWritable, MatrixElement, LongWritable, MultRowIntermediate> {

    private static final Logger LOG = Logger.getLogger(PreprocessReducer.class);
    private MultRowIntermediate outputValue = new MultRowIntermediate();

    @Override
    public void reduce(LongWritable key, Iterable<MatrixElement> values, Context context) throws IOException, InterruptedException {
        for (MatrixElement element : values) {
            if (element.isVector()) {
                outputValue.setVectorValue(element.getValue());
            } else {
                outputValue.getMatrixElements().add(element);
            }
        }
        context.write(key, outputValue);
    }
}
