/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.mvmult.preprocess;

import com.nileshc.graphfu.cdr.normalizeids.hashid.HashIdMapper;
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
public class PreprocessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static final Logger LOG = Logger.getLogger(HashIdMapper.class);

    @Override
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");
        try {
            context.write(new Text(tokenizer.nextToken()), NullWritable.get());
            context.write(new Text(tokenizer.nextToken()), NullWritable.get());
        } catch (NoSuchElementException nsee) {
        }
    }
}
