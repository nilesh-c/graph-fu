/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.preprocess;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class UniqueExtractRunner {

    private static final Logger LOG = Logger.getLogger(UniqueExtractRunner.class);

    public void run(String inputpath, String outputpath, String vdata) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("vdata", vdata);

        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(HashIdRunner.class);

            FileInputFormat.addInputPath(job, new Path(inputpath));
            FileOutputFormat.setOutputPath(job, new Path(outputpath));

            job.setMapperClass(UniqueExtractMapper.class);
            job.setReducerClass(UniqueExtractReducer.class);

            //set MultipleOutputs
            MultipleOutputs.addNamedOutput(job, vdata, TextOutputFormat.class, Text.class, NullWritable.class);

            //job.setInputFormatClass(NLineInputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(NullWritable.class);

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
        LOG.info("====== Job: Extract the unique raw vertex IDs ==========");
        LOG.info("Input = " + inputpath);
        LOG.info("Output = " + outputpath);
        LOG.info("=======================Done ==============================\n");
    }
}
