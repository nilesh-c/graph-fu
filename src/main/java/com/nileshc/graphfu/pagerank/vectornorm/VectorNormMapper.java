/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.pagerank.vectornorm;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import com.nileshc.graphfu.matrix.io.MultiValueWritable;
import com.nileshc.graphfu.matrix.mvmult.mult.MultMapper;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
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
public class VectorNormMapper extends Mapper<LongWritable, MultRowIntermediate, LongWritable, MultRowIntermediate> {

    private static final Logger LOG = Logger.getLogger(MultMapper.class);
    private double vectorSum = 0;
    private DoubleWritable newValueOutput = new DoubleWritable();

    @Override
    public void setup(Context context) {
        Configuration configuration = context.getConfiguration();
        this.vectorSum = configuration.getFloat("vectorsum", 1);
        LOG.addAppender(new ConsoleAppender(new SimpleLayout(), "System.err"));
    }

    @Override
    public void map(LongWritable key, MultRowIntermediate value, Context context) throws IOException, InterruptedException {
        LOG.info("old value was:" + value.getVectorValue().get());
        double newVectorValue = value.getVectorValue().get() / vectorSum;
        LOG.info("new value is:" + newVectorValue);
        newValueOutput.set(newVectorValue);
        value.setVectorValue(newValueOutput);
        context.write(key, value);
    }
}
