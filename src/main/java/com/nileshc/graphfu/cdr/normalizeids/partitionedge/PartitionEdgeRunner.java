/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.normalizeids.partitionedge;

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
public class PartitionEdgeRunner {

    private static int linespermap = 6000000;
    private static final Logger LOG = Logger.getLogger(PartitionEdgeRunner.class);
    private int numChunks = 0;

    public PartitionEdgeRunner(int numChunks) {
        this.numChunks = numChunks;
    }

    public void run(String inputpath, String outputpath) throws IOException {
        Configuration configuration = new Configuration();
        configuration.setInt("numChunks", numChunks);
        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(PartitionEdgeRunner.class);

            FileInputFormat.addInputPath(job, new Path(inputpath));
            FileOutputFormat.setOutputPath(job, new Path(outputpath));

            job.setMapperClass(PartitionEdgeMapper.class);
            job.setReducerClass(PartitionEdgeReducer.class);

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

        try {
            job.waitForCompletion(true);
        } catch (Exception e) {
            LOG.error("Unable to wait for job to finish", e);
        }

        LOG.info("Finished");
        LOG.info("====== Job: Partition the input edges by hash(sourceid) ==========");
        LOG.info("Input = " + inputpath);
        LOG.info("Output = " + outputpath);
        LOG.debug("numChunks = " + numChunks);
        LOG.info("=======================Done ==============================\n");
    }
}
