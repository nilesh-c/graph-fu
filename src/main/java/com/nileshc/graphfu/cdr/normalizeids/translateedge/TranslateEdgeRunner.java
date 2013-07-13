/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.normalizeids.translateedge;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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

    public boolean run(String inputPath, String outputPath) throws IOException {
        Configuration configuration = new Configuration();
        configuration.setInt("numChunks", numChunks);
        configuration.set("dictionaryPath", dictionaryPath);
        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(TranslateEdgeRunner.class);

            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setMapperClass(TranslateEdgeMapper.class);
            job.setReducerClass(TranslateEdgeReducer.class);

            //job.setInputFormatClass(NLineInputFormat.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(MatrixElement.class);
        } catch (Exception e) {
            LOG.error("Unable to initialize job", e);
        }

        LOG.info("====== Job: Partition the input edges by hash(sourceid) ==========");
        LOG.info("Input = " + inputPath);
        LOG.info("Output = " + outputPath);
        LOG.debug("numChunks = " + numChunks);
        LOG.debug("vidmap dictionary = " + dictionaryPath);

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
