/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.pagerank.initvectors;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import com.nileshc.graphfu.matrix.io.MultiValueWritable;
import com.nileshc.graphfu.matrix.mvmult.mult.MultMapper;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class InitVectorsMapper extends Mapper<LongWritable, MatrixElement, LongWritable, MatrixElement> {

    private static final Logger LOG = Logger.getLogger(MultMapper.class);
    private DoubleWritable productValue = new DoubleWritable();

    @Override
    public void map(LongWritable key, MatrixElement value, Context context) throws IOException, InterruptedException {
        context.write(value.getRow(), value);
    }
}
