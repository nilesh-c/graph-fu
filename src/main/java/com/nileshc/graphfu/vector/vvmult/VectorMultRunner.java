/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.vector.vvmult;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import com.nileshc.graphfu.matrix.mvmult.mult.MultRunner;
import com.nileshc.graphfu.vector.VectorJoinMapper;
import com.nileshc.graphfu.vector.l2norm.VectorDifferenceL2NormReducer;
import com.nileshc.graphfu.vector.l2norm.VectorDifferenceL2NormRunner;
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
public class VectorMultRunner {

    private static final Logger LOG = Logger.getLogger(MultRunner.class);
    private double product;

    public boolean run(String vector1InputPath, String vector2InputPath, String outputPath) throws IOException {
        Configuration configuration = new Configuration();
        Job job = null;

        try {
            job = new Job(configuration);
            job.setJarByClass(VectorMultRunner.class);
            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job, new Path(vector1InputPath));
            FileInputFormat.addInputPath(job, new Path(vector2InputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            job.setMapperClass(VectorJoinMapper.class);
            job.setReducerClass(VectorMultReducer.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(MatrixElement.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
        } catch (Exception e) {
            LOG.error("Unable to initialize job", e);
        }

        LOG.info("====== Job: Compute scalar product of two vectors ==========");
        LOG.info("Vector 1 Input = " + vector1InputPath);
        LOG.info("Vector 2 Input = " + vector2InputPath);
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
                    product = Double.parseDouble(line.trim());
                }
            }
            LOG.info("Finished");
            return job.isSuccessful();
        } catch (Exception e) {
            LOG.error("Unable to wait for job to finish", e);
        }
        return false;
    }

    public double getProduct() {
        return product;
    }
}
