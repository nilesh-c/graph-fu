/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.preprocess;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author nilesh
 */
public class PreprocessReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    protected MultipleOutputs multipleOutputs = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs(context);
    }

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder list = new StringBuilder();
        Configuration conf = context.getConfiguration();
        String qVectorOutput = conf.get("qvector");
        float alpha = Float.parseFloat(conf.get("alpha"));

        for (Text value : values) {
            String[] valueArray = value.toString().split(",");
            long id2 = Long.parseLong(valueArray[0]);
            int weight = Integer.parseInt(valueArray[1]);
            list.append(valueArray[0]).append("|").append(valueArray[1]).append(",");
        }
        if (list.length() != 0) {
            list.deleteCharAt(list.length() - 1);
            context.write(key, new Text(list.toString()));
            multipleOutputs.write(qVectorOutput, key, new FloatWritable(alpha));
        } else {
            context.write(key, new Text(""));
            multipleOutputs.write(qVectorOutput, key, new FloatWritable(1));
        }
    }

    private class ValuePair<L, R> {

        private final L left;
        private final R right;

        public ValuePair(L left, R right) {
            this.left = left;
            this.right = right;
        }

        public L getLeft() {
            return left;
        }

        public R getRight() {
            return right;
        }
    }
}