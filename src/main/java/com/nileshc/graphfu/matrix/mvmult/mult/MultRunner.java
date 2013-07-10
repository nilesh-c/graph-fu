/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.mvmult.mult;

import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import com.nileshc.graphfu.matrix.io.MultiValueWritable;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class MultRunner {

    private static final Logger LOG = Logger.getLogger(MultRunner.class);

    public void run(String inputpath, String outputpath) throws IOException {
        Configuration configuration = new Configuration();
        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(MultRunner.class);

            FileInputFormat.addInputPath(job, new Path(inputpath));
            FileOutputFormat.setOutputPath(job, new Path(outputpath));

            job.setMapperClass(MultMapper.class);
            job.setReducerClass(MultReducer.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(MultiValueWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(MultRowIntermediate.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
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
        LOG.info("====== Job: Stage 2 of matrix-vector multiplication (iterative stage) ==========");
        LOG.info("Input = " + inputpath);
        LOG.info("Output = " + outputpath);
        LOG.info("=======================Done ==============================\n");
    }
}
