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
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.SimpleLayout;

/**
 *
 * @author nilesh
 */
public class InitVectorsReducer extends Reducer<LongWritable, MatrixElement, NullWritable, NullWritable> {

    private static final Logger LOG = Logger.getLogger(MultReducer.class);
    private MultipleOutputs multipleOutputs = null;
    private String danglingVectorOutput = null;
    private String rankVectorOutput = null;
    private long numNodes = 0;
    private DoubleWritable rankOutputDouble = new DoubleWritable();
    private DoubleWritable danglingOutputDouble = new DoubleWritable();
    private MatrixElement rankElement = new MatrixElement();
    //private MatrixElement danglingElement = new MatrixElement();
    private MultRowIntermediate danglingNode = new MultRowIntermediate();
    private MatrixElementListWritable dummy = new MatrixElementListWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LOG.addAppender(new ConsoleAppender(new SimpleLayout(), "System.err"));
        multipleOutputs = new MultipleOutputs(context);
        Configuration conf = context.getConfiguration();
        this.danglingVectorOutput = conf.get("danglingvector");
        this.rankVectorOutput = conf.get("rankvector");
        this.numNodes = conf.getLong("numnodes", 1);
        rankOutputDouble.set((double) 1 / numNodes);
        danglingOutputDouble.set(1);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }

    @Override
    public void reduce(LongWritable key, Iterable<MatrixElement> values, Context context) throws IOException, InterruptedException {
        Iterator<MatrixElement> it = values.iterator();
        MatrixElement value = it.next();
        if (!it.hasNext() && value.isRow()) {
            danglingNode.setVectorRow(key);
            danglingNode.setVectorValue(danglingOutputDouble);
            danglingNode.setMatrixElements(dummy);
            LOG.info("Dangling:" + danglingNode);
            multipleOutputs.write(danglingVectorOutput, danglingNode.getVectorRow(), danglingNode, danglingVectorOutput + "/part");
        }
        rankElement.setVectorData(key, rankOutputDouble);
        LOG.info("Rank:" + rankElement);
        multipleOutputs.write(rankVectorOutput, NullWritable.get(), rankElement, rankVectorOutput + "/part");
    }
}
