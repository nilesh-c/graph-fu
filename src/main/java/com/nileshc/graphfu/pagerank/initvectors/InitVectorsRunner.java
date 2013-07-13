/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.pagerank.initvectors;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import com.nileshc.graphfu.matrix.io.MatrixElementInputFormat;
import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
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

    private static final Logger LOG = Logger.getLogger(PreprocessRunner.class);
    private final long numNodes;

    public InitVectorsRunner(long numNodes) {
        this.numNodes = numNodes;
    }

    public boolean run(String inputPath, String outputPath, String danglingVector, String rankVector) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("danglingvector", danglingVector);
        configuration.set("rankvector", rankVector);
        configuration.setLong("numnodes", numNodes);
        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(PreprocessRunner.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setMapperClass(InitVectorsMapper.class);
            job.setReducerClass(InitVectorsReducer.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(MatrixElement.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(MatrixElement.class);

            //set MultipleOutputs
            MultipleOutputs.addNamedOutput(job, rankVector, SequenceFileOutputFormat.class, NullWritable.class, MatrixElement.class);
            MultipleOutputs.addNamedOutput(job, danglingVector, SequenceFileOutputFormat.class, NullWritable.class, MatrixElement.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        } catch (Exception e) {
            LOG.error("Unable to initialize job", e);
        }

        LOG.info("====== Job: Stage 1 of matrix-vector multiplication ==========");
        LOG.info("Matrix Input = " + inputPath);
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