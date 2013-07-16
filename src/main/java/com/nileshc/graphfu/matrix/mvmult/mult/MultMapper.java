/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.mvmult.mult;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import com.nileshc.graphfu.matrix.io.MultiValueWritable;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

/**
 *
 * @author nilesh
 */
public class MultMapper extends Mapper<LongWritable, MultRowIntermediate, LongWritable, MultiValueWritable> {

    private static final Logger LOG = Logger.getLogger(MultMapper.class);
    private DoubleWritable productValue = new DoubleWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LOG.addAppender(new ConsoleAppender(new SimpleLayout(), "System.err"));
    }

    @Override
    public void map(LongWritable key, MultRowIntermediate value, Context context) throws IOException, InterruptedException {
        double vectorValue = value.getVectorValue().get();
        if (vectorValue != 0) {
            LOG.info("MultRowInter. contains : " + value);
            for (MatrixElement element : value.getMatrixElements()) {
                productValue.set(element.getValue().get() * vectorValue);
                LOG.info("Multiplying " + element + " WITH " + vectorValue + " GIVES: " + productValue);
                context.write(element.getRow(), new MultiValueWritable(productValue));
            }
        }
        context.write(value.getVectorRow(), new MultiValueWritable(value.getMatrixElements()));
    }
}
