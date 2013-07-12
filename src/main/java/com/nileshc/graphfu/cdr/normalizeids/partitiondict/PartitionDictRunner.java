/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.normalizeids.partitiondict;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class PartitionDictRunner {

    private static int linespermap = 6000000;
    private static final Logger LOG = Logger.getLogger(PartitionDictRunner.class);
    private int numChunks = 0;

    public PartitionDictRunner(int numChunks) {
        this.numChunks = numChunks;
    }

    public boolean run(String inputPath, String outputPath) throws IOException {
        Configuration configuration = new Configuration();
        configuration.setInt("numChunks", numChunks);
        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(PartitionDictRunner.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setMapperClass(PartitionDictMapper.class);
            job.setReducerClass(PartitionDictReducer.class);

            //job.setInputFormatClass(NLineInputFormat.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            String outprefix = "vidhashmap";
            for (int i = 0; i < numChunks; i++) {
                MultipleOutputs.addNamedOutput(job, outprefix + i, TextOutputFormat.class, Text.class, Text.class);
            }

            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        } catch (Exception e) {
            LOG.error("Unable to initialize job", e);
        }

        LOG.info("====== Job: Partition the map of rawid -> id ==========");
        LOG.info("Input = " + inputPath);
        LOG.info("Output = " + outputPath);
        LOG.debug("numChunks = " + numChunks);

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
