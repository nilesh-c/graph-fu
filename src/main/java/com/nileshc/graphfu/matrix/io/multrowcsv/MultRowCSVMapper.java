/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.io.multrowcsv;

import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import com.nileshc.graphfu.matrix.mvmult.mult.MultMapper;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

/**
 *
 * @author nilesh
 */
public class MultRowCSVMapper extends Mapper<LongWritable, MultRowIntermediate, NullWritable, Text> {

    private static final Logger LOG = Logger.getLogger(MultMapper.class);
    Text output = new Text();

    @Override
    public void map(LongWritable key, MultRowIntermediate value, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        sb.append(value.getVectorRow().get()).append(",").append(value.getVectorValue());
        output.set(sb.toString());
        context.write(NullWritable.get(), output);
    }
}
