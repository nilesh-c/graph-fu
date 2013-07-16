/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.mvmult.preprocess;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class PreprocessMapper extends Mapper<NullWritable, MatrixElement, LongWritable, MatrixElement> {

    private static final Logger LOG = Logger.getLogger(PreprocessMapper.class);

    @Override
    public void map(NullWritable key, MatrixElement value, Context context) throws IOException, InterruptedException {
        if (value.isVector()) {
            context.write(value.getRow(), value);
        } else {
            context.write(value.getColumn(), value);
        }
    }
}
