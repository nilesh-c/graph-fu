/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.preprocess;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class HashIdRunner {

    private static int linespermap = 6000000;
    private static final Logger LOG = Logger.getLogger(PreprocessRunner.class);

    public void run(String inputpath, String outputpath, String vidmap) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
        configuration.set("vidmap", vidmap);
        
        Job job = null;
        
        try {
            job = new Job(configuration);
            job.setJarByClass(HashIdRunner.class);
            
            FileInputFormat.addInputPath(job, new Path(inputpath));
            FileOutputFormat.setOutputPath(job, new Path(outputpath));

            job.setMapperClass(HashIdMapper.class);
            job.setReducerClass(HashIdReducer.class);

            //set MultipleOutputs
            MultipleOutputs.addNamedOutput(job, vidmap, TextOutputFormat.class, LongWritable.class, Text.class);
            
            job.setInputFormatClass(NLineInputFormat.class);
            
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(NullWritable.class);
        } catch (Exception e) {
            LOG.error("Unable to initialize job", e);
        }

        try {
            job.waitForCompletion(true);
        } catch (Exception e) {
            LOG.error("Unable to wait for job to finish", e);
        }

        LOG.info("Finished");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setMapperClass(HashIdMapper.class);
        conf.setReducerClass(HashIdReducer.class);

        conf.setInputFormat(NLineInputFormat.class);
        conf.setOutputFormat(MultiDirOutputFormat.class);

        conf.setInt("mapred.line.input.format.linespermap", linespermap);
        conf.set("GraphParser", graphparser.getClass().getName());
        conf.set("VidParser", vidparser.getClass().getName());
        conf.set("VdataParser", vdataparser.getClass().getName());

        FileInputFormat.setInputPaths(conf, new Path(inputpath));
        FileOutputFormat.setOutputPath(conf, new Path(outputpath));

        LOG.info("====== Job: Create integer Id maps for vertices ==========");
        LOG.info("Input = " + inputpath);
        LOG.info("Output = " + outputpath);
        LOG.debug("Lines per map = 6000000");
        LOG.debug("GraphParser = " + graphparser.getClass().getName());
        LOG.debug("VidParser = " + vidparser.getClass().getName());
        LOG.debug("VdataParser = " + vdataparser.getClass().getName());
        LOG.info("==========================================================");
        JobClient.runJob(conf);
        LOG.info("=======================Done =====================\n");
    }
}
