/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.pagerank.vectornorm;

import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import com.nileshc.graphfu.matrix.mvmult.mult.MultRunner;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class VectorNormRunner {

    private static final Logger LOG = Logger.getLogger(MultRunner.class);
    private final double vectorSum;

    public VectorNormRunner(double vectorSum) {
        this.vectorSum = vectorSum;
    }

    public boolean run(String inputPath, String outputPath) throws IOException {
        Configuration configuration = new Configuration();
        configuration.setFloat("vectorsum", (float) vectorSum);
        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(MultRunner.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setMapperClass(VectorNormMapper.class);
            job.setReducerClass(Reducer.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(MultRowIntermediate.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(MultRowIntermediate.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
        } catch (Exception e) {
            LOG.error("Unable to initialize job", e);
        }

        LOG.info("====== Job: Stage 2 for normalizing rank vector ==========");
        LOG.info("Input = " + inputPath);
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
