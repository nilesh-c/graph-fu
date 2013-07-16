/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.normalizeids.hashid;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class HashIdReducer extends Reducer<Text, NullWritable, LongWritable, Text> {

    private static final Logger LOG = Logger.getLogger(HashIdReducer.class);
    private MultipleOutputs multipleOutputs1 = null;
    private MultipleOutputs multipleOutputs2 = null;
    private LongWritable countLong = new LongWritable(1);
    private DoubleWritable dummy = new DoubleWritable(0);
    private MatrixElement matrixElement = new MatrixElement();
    private String vidmap = "";
    private String binaryVdata;

    public static enum NodeCounter {

        Counter
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs1 = new MultipleOutputs(context);
        multipleOutputs2 = new MultipleOutputs(context);
        Configuration conf = context.getConfiguration();
        this.vidmap = conf.get("vidmap");
        this.binaryVdata = conf.get("binvidmap");
        context.getCounter(NodeCounter.Counter).setValue(0);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs1.close();
        multipleOutputs2.close();
    }

    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        countLong.set(context.getCounter(NodeCounter.Counter).getValue());
        matrixElement.setRowData(countLong);
        multipleOutputs1.write(vidmap, countLong, key, vidmap + "/part");
        multipleOutputs2.write(binaryVdata, NullWritable.get(), matrixElement, binaryVdata + "/part");
        context.getCounter(NodeCounter.Counter).increment(1);
    }
}
