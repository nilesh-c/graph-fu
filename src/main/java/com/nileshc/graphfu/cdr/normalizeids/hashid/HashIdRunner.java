/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.normalizeids.hashid;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class HashIdRunner {

    private static int linespermap = 6000000;
    private static final Logger LOG = Logger.getLogger(HashIdRunner.class);

    public void run(String inputpath, String outputpath, String vidmap) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("vidmap", vidmap);

        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(HashIdRunner.class);
            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job, new Path(inputpath));
            FileOutputFormat.setOutputPath(job, new Path(outputpath));

            job.setMapperClass(HashIdMapper.class);
            job.setReducerClass(HashIdReducer.class);

            //set MultipleOutputs
            MultipleOutputs.addNamedOutput(job, vidmap, TextOutputFormat.class, LongWritable.class, Text.class);

            //job.setInputFormatClass(NLineInputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);

            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        } catch (Exception e) {
            LOG.error("Unable to initialize job", e);
        }

        try {
            job.waitForCompletion(true);
        } catch (Exception e) {
            LOG.error("Unable to wait for job to finish", e);
        }

        LOG.info("Finished");
        LOG.info("====== Job: Create integer Id maps for vertices ==========");
        LOG.info("Input = " + inputpath);
        LOG.info("Output = " + outputpath);
        LOG.debug("Lines per map = " + linespermap);
        LOG.info("=======================Done ==============================\n");
    }
}
