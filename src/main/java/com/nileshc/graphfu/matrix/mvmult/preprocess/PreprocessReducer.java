/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.mvmult.preprocess;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import com.nileshc.graphfu.matrix.io.MatrixElementListWritable;
import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

    @Override
    public void reduce(LongWritable key, Iterable<MatrixElement> values, Context context) throws IOException, InterruptedException {
        MultRowIntermediate outputValue = new MultRowIntermediate();
        MatrixElementListWritable list = new MatrixElementListWritable();
        for (Iterator<MatrixElement> it = values.iterator(); it.hasNext();) {
            MatrixElement element = it.next();
            LOG.info("Matrix element form iterator: " + element);
            if (element.isVector() == true) {
                outputValue.setVectorValue(new DoubleWritable(element.getValue().get()));
            } else {
                LOG.info("Adding to list");
                list.add(new MatrixElement(new LongWritable(element.getRow().get()),
                        new LongWritable(element.getColumn().get()),
                        new DoubleWritable(element.getValue().get())));
            }
        }
        outputValue.setVectorRow(key);
        outputValue.setMatrixElements(list);
        context.write(key, outputValue);
    }
}
