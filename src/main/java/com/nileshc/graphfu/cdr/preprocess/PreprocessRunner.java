/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.preprocess;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh Preprocesses the CDR dataset and does the following: i)
 * Anonymize the dataset ii) Calculate the weights for each edge iii)
 */
public class PreprocessRunner {

    private static final Logger LOG = Logger.getLogger(PreprocessRunner.class);

    public void run(String edgeListLocation, String adjacencyOutputLocation, String qVectorOutputLocation, float alpha) {
        Configuration configuration = new Configuration();
        configuration.set("qvector", qVectorOutputLocation);
        configuration.set("alpha", "" + alpha);
        Job job = null;
        
        try {
            job = new Job(configuration);
            job.setJarByClass(PreprocessRunner.class);
            FileInputFormat.addInputPath(job, new Path(edgeListLocation));
            FileOutputFormat.setOutputPath(job, new Path(adjacencyOutputLocation));

            LOG.info("Using native MultipleOutputs");
            job.setMapperClass(PreprocessMapper.class);
            job.setReducerClass(PreprocessReducer.class);

            //set MultipleOutputs
            MultipleOutputs.addNamedOutput(job, qVectorOutputLocation, TextOutputFormat.class, LongWritable.class, FloatWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
        } catch (Exception e) {
            LOG.error("Unable to initialize job", e);
        }

        try {
            job.waitForCompletion(true);
        } catch (Exception e) {
            LOG.error("Unable to wait for job to finish", e);
        }

        LOG.info("Finished");
    }
}
