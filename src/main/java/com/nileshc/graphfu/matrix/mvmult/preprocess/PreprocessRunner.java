/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.mvmult.preprocess;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import com.nileshc.graphfu.matrix.io.MatrixElementInputFormat;
import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class PreprocessRunner {

    private static final Logger LOG = Logger.getLogger(PreprocessRunner.class);

    public boolean run(String matrixInputPath, String vectorInputPath, String outputPath) throws IOException {
        Configuration configuration = new Configuration();
        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(PreprocessRunner.class);

            FileInputFormat.addInputPath(job, new Path(vectorInputPath));
            FileInputFormat.addInputPath(job, new Path(matrixInputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setMapperClass(PreprocessMapper.class);
            job.setReducerClass(PreprocessReducer.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(MatrixElement.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(MultRowIntermediate.class);

            MultipleOutputs.addNamedOutput(job, "blah", SequenceFileOutputFormat.class, LongWritable.class, Text.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
        } catch (Exception e) {
            LOG.error("Unable to initialize job", e);
        }

        LOG.info("====== Job: Stage 1 of matrix-vector multiplication (preprocessing stage) ==========");
        LOG.info("Matrix Input = " + matrixInputPath);
        LOG.info("Vector Input = " + vectorInputPath);
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
