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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class PreprocessRunner {

    private static final Logger LOG = Logger.getLogger(PreprocessRunner.class);

    public void run(String matrixInputPath, String vectorInputPath, String outputpath) throws IOException {
        Configuration configuration = new Configuration();
        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(PreprocessRunner.class);

            FileInputFormat.addInputPath(job, new Path(vectorInputPath));
            FileInputFormat.addInputPath(job, new Path(matrixInputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputpath));

            job.setMapperClass(PreprocessMapper.class);
            job.setReducerClass(PreprocessReducer.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(MatrixElement.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(MultRowIntermediate.class);

            job.setInputFormatClass(MatrixElementInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
        } catch (Exception e) {
            LOG.error("Unable to initialize job", e);
        }

        try {
            job.waitForCompletion(true);
        } catch (Exception e) {
            LOG.error("Unable to wait for job to finish", e);
        }

        LOG.info("Finished");
        LOG.info("====== Job: Stage 1 of matrix-vector multiplication ==========");
        LOG.info("Matrix Input = " + matrixInputPath);
        LOG.info("Vector Input = " + vectorInputPath);
        LOG.info("Output = " + outputpath);
        LOG.info("=======================Done ==============================\n");
    }
}
