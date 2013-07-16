/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.vector;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import com.nileshc.graphfu.matrix.io.MultiValueWritable;
import com.nileshc.graphfu.matrix.mvmult.mult.MultMapper;
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
public class VectorJoinMapper extends Mapper<LongWritable, MultRowIntermediate, LongWritable, MatrixElement> {

    private static final Logger LOG = Logger.getLogger(VectorJoinMapper.class);
    private MatrixElement matrixElement = new MatrixElement();
    private LongWritable longRow = new LongWritable();
    private DoubleWritable doubleValue = new DoubleWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LOG.addAppender(new ConsoleAppender(new SimpleLayout(), "System.err"));
    }

    @Override
    public void map(LongWritable key, MultRowIntermediate value, Context context) throws IOException, InterruptedException {
        longRow.set(value.getVectorRow().get());
        doubleValue.set(value.getVectorValue().get());
        matrixElement.setVectorData(longRow, doubleValue);
        LOG.info("Joinmapper sending : " + longRow + " and " + matrixElement);
        context.write(longRow, matrixElement);
    }
}
