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
    private MultipleOutputs multipleOutputs;
    private int count = 0;
    enum test {
        test
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
        LOG.addAppender(new ConsoleAppender(new SimpleLayout(), "System.err"));
        context.getCounter(test.test).setValue(0);
    }

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
                context.getCounter(test.test).increment(1);
                list.add(new MatrixElement(new LongWritable(element.getRow().get()),
                        new LongWritable(element.getColumn().get()),
                        new DoubleWritable(element.getValue().get())));
            }
        }
        outputValue.setVectorRow(key);
        outputValue.setMatrixElements(list);
        LOG.info(count++ + "PREPROCESSOR SAYS:" + outputValue);
        multipleOutputs.write("blah", key, new Text(outputValue.toString()), "/blah");
        context.write(key, outputValue);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        LOG.info("TIMES:" + context.getCounter(test.test).getValue());
    }
    
    
}
