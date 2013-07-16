/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.normalizeids.hashid;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class HashIdMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static final Logger LOG = Logger.getLogger(HashIdMapper.class);
    private Text value = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");
        try {
            value.set(tokenizer.nextToken());
            context.write(value, NullWritable.get());
            value.set(tokenizer.nextToken());
            context.write(value, NullWritable.get());
        } catch (NoSuchElementException nsee) {
        }
    }
}
