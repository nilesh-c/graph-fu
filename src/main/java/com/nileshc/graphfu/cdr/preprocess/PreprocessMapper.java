/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.preprocess;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author nilesh
 *
 */
public class PreprocessMapper {

    public static class Map extends Mapper<LongWritable, Text, LongWritable, ArrayWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");
            String id2 = tokenizer.nextToken();
            int count = Integer.parseInt(tokenizer.nextToken());
            int total = Integer.parseInt(tokenizer.nextToken());
            int max = Integer.parseInt(tokenizer.nextToken());
            int min = Integer.parseInt(tokenizer.nextToken());

            int weight = total + max * count;
            context.write(new LongWritable(key), new ArrayWritable())
        }
    }
}
