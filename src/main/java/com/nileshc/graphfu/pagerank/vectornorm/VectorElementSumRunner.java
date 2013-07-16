/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.pagerank.vectornorm;

import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import com.nileshc.graphfu.matrix.mvmult.preprocess.PreprocessRunner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
public class VectorElementSumRunner {

    private static final Logger LOG = Logger.getLogger(PreprocessRunner.class);
    double vectorSum = 0;

    public boolean run(String inputPath, String outputPath) throws IOException {
        Configuration configuration = new Configuration();
        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(VectorElementSumRunner.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setMapperClass(Mapper.class);
            job.setReducerClass(VectorElementSumReducer.class);
            job.setNumReduceTasks(1);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(MultRowIntermediate.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
        } catch (Exception e) {
            LOG.error("Unable to initialize job", e);
        }

        LOG.info("====== Job: Stage 1 for normalizing rank vector - find column sum ==========");
        LOG.info("Input = " + inputPath);
        LOG.info("Output = " + outputPath);

        try {
            job.waitForCompletion(true);
            if (job.isSuccessful()) {
                FileSystem fs = FileSystem.get(configuration);
                Path sumFile = new Path(outputPath + "/part-r-00000");
                if (fs.exists(sumFile)) {
                    FSDataInputStream in = fs.open(sumFile);
                    BufferedReader br = new BufferedReader(new InputStreamReader(in));
                    String line = br.readLine();
                    vectorSum = Double.parseDouble(line.trim());
                }
            }
            LOG.info("Finished");
            return job.isSuccessful();
        } catch (Exception e) {
            LOG.error("Unable to wait for job to finish", e);
        }
        return false;
    }

    public double getVectorSum() {
        return vectorSum;
    }
}