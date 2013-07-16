/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.pagerank.initvectors;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import com.nileshc.graphfu.matrix.io.MatrixElementInputFormat;
import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import com.nileshc.graphfu.matrix.io.MultiValueWritable;
import com.nileshc.graphfu.matrix.mvmult.preprocess.PreprocessMapper;
import com.nileshc.graphfu.matrix.mvmult.preprocess.PreprocessReducer;
import com.nileshc.graphfu.matrix.mvmult.preprocess.PreprocessRunner;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class InitVectorsRunner {

    private static final Logger LOG = Logger.getLogger(InitVectorsRunner.class);
    private final long numNodes;

    public InitVectorsRunner(long numNodes) {
        this.numNodes = numNodes;
    }

    public boolean run(String matrixInputPath, String vdataInputPath, String outputPath, String danglingVector, String rankVector) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("danglingvector", danglingVector);
        configuration.set("rankvector", rankVector);
        configuration.setLong("numnodes", numNodes);
        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(InitVectorsRunner.class);

            FileInputFormat.addInputPath(job, new Path(matrixInputPath));
            FileInputFormat.addInputPath(job, new Path(vdataInputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setMapperClass(InitVectorsMapper.class);
            job.setReducerClass(InitVectorsReducer.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(MatrixElement.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(NullWritable.class);

            //set MultipleOutputs
            MultipleOutputs.addNamedOutput(job, rankVector, SequenceFileOutputFormat.class, NullWritable.class, MatrixElement.class);
            MultipleOutputs.addNamedOutput(job, danglingVector, SequenceFileOutputFormat.class, LongWritable.class, MultRowIntermediate.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        } catch (Exception e) {
            LOG.error("Unable to initialize job", e);
        }

        LOG.info("====== Job: Initialize rank vector and compute dangling node vector ==========");
        LOG.info("Matrix input = " + matrixInputPath);
        LOG.info("vdata input = " + vdataInputPath);
        LOG.info("Output = " + outputPath);

        try {
            job.waitForCompletion(true);
            LOG.info("Finished");
            return job.isSuccessful();
        } catch (Exception e) {
            LOG.error("Unable to wait for job to finish", e);
        }
        return false;
    }
}