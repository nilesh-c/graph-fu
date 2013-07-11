/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.mvmult.preprocess;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

/**
 *
 * @author nilesh
 */
public class PreprocessMapper extends Mapper<LongWritable, MatrixElement, LongWritable, MatrixElement> {

    private static final Logger LOG = Logger.getLogger(PreprocessMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        LOG.addAppender(new ConsoleAppender(new SimpleLayout(), "System.err"));
    }

    @Override
    public void map(LongWritable key, MatrixElement value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");
        LOG.info(value);
        if (value.isVector()) {
            context.write(value.getColumn(), value);
        } else {
            context.write(value.getRow(), value);
        }
    }
}
