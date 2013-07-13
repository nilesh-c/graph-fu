/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.pagerank.initvectors;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import com.nileshc.graphfu.matrix.io.MatrixElementListWritable;
import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import com.nileshc.graphfu.matrix.io.MultiValueWritable;
import com.nileshc.graphfu.matrix.mvmult.mult.MultReducer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author nilesh
 */
public class InitVectorsReducer extends Reducer<LongWritable, MatrixElement, NullWritable, MatrixElement> {

    private static final Logger LOG = Logger.getLogger(MultReducer.class);
    private MultipleOutputs multipleOutputs = null;
    private String danglingVectorOutput = null;
    private String rankVectorOutput = null;
    private long numNodes = 0;
    private DoubleWritable rankOutputDouble = new DoubleWritable();
    private MatrixElement rankElement = new MatrixElement();
    private MatrixElement danglingElement = new MatrixElement();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        multipleOutputs = new MultipleOutputs<NullWritable, MatrixElement>(context);
        Configuration conf = context.getConfiguration();
        this.danglingVectorOutput = conf.get("danglingvector");
        this.rankVectorOutput = conf.get("rankvector");
        this.numNodes = conf.getLong("numnodes", 1);
        rankOutputDouble.set(1 / numNodes);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    @Override
    public void reduce(LongWritable key, Iterable<MatrixElement> values, Context context) throws IOException, InterruptedException {

        for (MatrixElement value : values) {
            
            multipleOutputs.write(NullWritable.get(), rankOutputDouble, rankVectorOutput);
        }
    }
}
