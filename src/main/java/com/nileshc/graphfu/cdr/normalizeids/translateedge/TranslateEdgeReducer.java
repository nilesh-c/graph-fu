/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.normalizeids.translateedge;

import com.nileshc.graphfu.cdr.normalizeids.partitionedge.PartitionEdgeMapper;
import com.nileshc.graphfu.cdr.normalizeids.partitionedge.PartitionEdgeReducer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class TranslateEdgeReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

    private static final Logger LOG = Logger.getLogger(TranslateEdgeReducer.class);
    private int numChunks = 0;
    private String dictionaryPath;
    private int dictionaryId;
    private HashMap<String, Long> dict;
    private FileSystem fs;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        this.numChunks = conf.getInt("numChunks", 256);
        this.dictionaryPath = conf.get("dictionaryPath");
        this.dict = new HashMap<String, Long>();
        this.dictionaryId = -1;
        fs = FileSystem.get(conf);
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (key.get() != dictionaryId) {
            dictionaryId = key.get();
            loadDictionary();
        }
        for (Text value : values) {
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");
            String sourceId = tokenizer.nextToken();
            String targetId = tokenizer.nextToken();
            if (dict.containsKey(targetId)) {
                long newTargetId = dict.get(targetId);
                StringBuilder output = new StringBuilder(); // Feed in the edge data
                output.append(newTargetId).append(",");
                while (tokenizer.hasMoreTokens()) {
                    output.append(tokenizer.nextToken()).append(",");
                }
                output.deleteCharAt(output.length() - 1);
                context.write(NullWritable.get(), new Text(sourceId + "," + output.toString()));
            }
        }
    }

    private void loadDictionary() throws IOException {
        dict.clear();
        String prefix = "vidhashmap" + dictionaryId;
        FileStatus[] stats = fs.listStatus(new Path(dictionaryPath));
        for (FileStatus stat : stats) {
            if (stat.getPath().getName().matches(".*" + prefix + "-r-.*")) {
                LOG.debug(("Mapper Load dictionary: " + stat.getPath().getName()));
                Scanner sc = new Scanner(new BufferedReader(new InputStreamReader(
                        fs.open(stat.getPath()))));
                while (sc.hasNextLine()) {
                    String line = sc.nextLine();
                    StringTokenizer tokenizer = new StringTokenizer(line);
                    try {
                        long id = Long.valueOf(tokenizer.nextToken());
                        String rawId = tokenizer.nextToken();
                        dict.put(rawId, id);
                    } catch (NoSuchElementException e) {
                        e.printStackTrace();
                        LOG.error("Error in loading vidmap entry:" + line);
                    }
                }
            }
        }
    }
}