/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.preprocess.translateedge;

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
public class TranslateEdgeRunner {

    private static final Logger LOG = Logger.getLogger(TranslateEdgeRunner.class);
    private int numChunks;
    private String dictionaryPath;

    public TranslateEdgeRunner(int numChunks, String dictionaryPath) {
        this.numChunks = numChunks;
        this.dictionaryPath = dictionaryPath;
    }

    public void run(String inputpath, String outputpath) throws IOException {
        Configuration configuration = new Configuration();
        configuration.setInt("numChunks", numChunks);
        configuration.set("dictionaryPath", dictionaryPath);
        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(TranslateEdgeRunner.class);

            FileInputFormat.addInputPath(job, new Path(inputpath));
            FileOutputFormat.setOutputPath(job, new Path(outputpath));

            job.setMapperClass(TranslateEdgeMapper.class);
            job.setReducerClass(TranslateEdgeReducer.class);

            //job.setInputFormatClass(NLineInputFormat.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
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
        LOG.info("====== Job: Partition the input edges by hash(sourceid) ==========");
        LOG.info("Input = " + inputpath);
        LOG.info("Output = " + outputpath);
        LOG.debug("numChunks = " + numChunks);
        LOG.info("=======================Done ==============================\n");
    }
}
