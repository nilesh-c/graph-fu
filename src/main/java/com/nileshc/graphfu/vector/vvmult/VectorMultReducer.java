/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.vector.vvmult;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import com.nileshc.graphfu.matrix.mvmult.mult.MultReducer;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

/**
 *
 * @author nilesh
 */
public class VectorMultReducer extends Reducer<LongWritable, MatrixElement, NullWritable, DoubleWritable> {

    private static final Logger LOG = Logger.getLogger(MultReducer.class);
    private DoubleWritable productOutput = new DoubleWritable();
    private double productSum = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LOG.addAppender(new ConsoleAppender(new SimpleLayout(), "System.err"));
    }

    @Override
    public void reduce(LongWritable key, Iterable<MatrixElement> values, Context context) throws IOException, InterruptedException {
        Iterator<MatrixElement> it = values.iterator();
        double value1 = it.next().getValue().get();
        if (it.hasNext()) {
            double value2 = it.next().getValue().get();
            productSum += value1 * value2;
            LOG.info("Multiplying " + value1 + " with " + value2 + " gives " + productSum);
        }
    }

    @Override
    protected void cleanup(Reducer.Context context) throws IOException, InterruptedException {
        productOutput.set(productSum);
        context.write(NullWritable.get(), productOutput);
    }
}
