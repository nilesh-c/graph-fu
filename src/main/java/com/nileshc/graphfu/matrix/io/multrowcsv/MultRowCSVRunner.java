/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.io.multrowcsv;

import com.nileshc.graphfu.matrix.io.MultRowIntermediate;
import com.nileshc.graphfu.matrix.mvmult.mult.MultRunner;
import com.nileshc.graphfu.pagerank.vectornorm.VectorNormMapper;
import com.nileshc.graphfu.pagerank.vectornorm.VectorNormRunner;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
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
public class MultRowCSVRunner {

    private static final Logger LOG = Logger.getLogger(MultRunner.class);

    public boolean run(String inputPath, String outputPath) throws IOException {
        Configuration configuration = new Configuration();
        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(VectorNormRunner.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setMapperClass(MultRowCSVMapper.class);
            job.setReducerClass(Reducer.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(MultRowIntermediate.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
        } catch (Exception e) {
            LOG.error("Unable to initialize job", e);
        }

        LOG.info("====== Job: Print the final rank vector as row,value pairs ==========");
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
