/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.normalizeids.hashid;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class HashIdRunner {
    
    private static int linespermap = 6000000;
    private static final Logger LOG = Logger.getLogger(HashIdRunner.class);
    private long totalVertexCount = 0;
    
    public boolean run(String inputPath, String outputPath, String vidmap, String binaryVdata) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("vidmap", vidmap);
        configuration.set("binvidmap", binaryVdata);
        
        Job job = null;
        
        try {
            job = new Job(configuration);
            job.setJarByClass(HashIdRunner.class);
            job.setNumReduceTasks(1);
            
            FileInputFormat.addInputPath(job, new Path(inputPath));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            
            job.setMapperClass(HashIdMapper.class);
            job.setReducerClass(HashIdReducer.class);

            //set MultipleOutputs
            MultipleOutputs.addNamedOutput(job, vidmap, TextOutputFormat.class, LongWritable.class, Text.class);
            MultipleOutputs.addNamedOutput(job, binaryVdata, SequenceFileOutputFormat.class, NullWritable.class, MatrixElement.class);

            //job.setInputFormatClass(NLineInputFormat.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);
            
            LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        } catch (Exception e) {
            LOG.error("Unable to initialize job", e);
        }
        
        LOG.info("====== Job: Create integer Id maps for vertices ==========");
        LOG.info("Input = " + inputPath);
        LOG.info("Output = " + outputPath);
        LOG.debug("Lines per map = " + linespermap);
        
        try {
            job.waitForCompletion(true);
            if (job.isSuccessful()) {                
                totalVertexCount = job.getCounters().findCounter(HashIdReducer.NodeCounter.Counter).getValue();
            }
            LOG.info("Finished");
            return job.isSuccessful();
        } catch (Exception e) {
            LOG.error("Unable to wait for job to finish", e);
        }
        return false;
    }
    
    public long getTotalVertexCount() {
        return totalVertexCount;
    }
}
