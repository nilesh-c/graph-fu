/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.preprocess;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

/**
 *
 * @author nilesh
 */
public class HashIdMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private static final Logger LOG = Logger.getLogger(HashIdMapper.class);
    private long currentID;

    @Override
    public void setup(Context context) {
        this.currentID = 0;
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String id1 = value.toString();
        context.write(new LongWritable(currentID), new Text(id1));
        ++currentID;
    }
}
