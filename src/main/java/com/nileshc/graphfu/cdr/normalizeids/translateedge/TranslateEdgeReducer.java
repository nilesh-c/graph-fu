/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.cdr.normalizeids.translateedge;

import com.nileshc.graphfu.matrix.io.MatrixElement;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.logging.Level;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class TranslateEdgeReducer extends Reducer<IntWritable, Text, NullWritable, MatrixElement> {

    private static final Logger LOG = Logger.getLogger(TranslateEdgeReducer.class);
    private int numChunks = 0;
    private String dictionaryPath;
    private int dictionaryId;
    private HashMap<String, Long> dict;
    private FileSystem fs;
    private MatrixElement edgeOutput = new MatrixElement();
    private DoubleWritable weightOut = new DoubleWritable();
    private LongWritable sourceIdOut = new LongWritable();
    private LongWritable targetIdOut = new LongWritable();
    private EdgeWeightCalculator edgeWeightCalculator = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        this.numChunks = conf.getInt("numChunks", 256);
        this.dictionaryPath = conf.get("dictionaryPath");
        this.dict = new HashMap<String, Long>();
        this.dictionaryId = -1;
        fs = FileSystem.get(conf);
        try {
            edgeWeightCalculator = (EdgeWeightCalculator) Class.forName(conf.get("eweightcalc")).newInstance();
        } catch (ClassNotFoundException ex) {
            LOG.info(ex);
        } catch (InstantiationException ex) {
            LOG.info(ex);
        } catch (IllegalAccessException ex) {
            LOG.info(ex);
        }
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
                long targetVertex = dict.get(targetId);
                long sourceVertex = Long.parseLong(sourceId);

                StringBuilder output = new StringBuilder(); // Feed in the edge data and calculate weight
                while (tokenizer.hasMoreTokens()) {
                    output.append(tokenizer.nextToken()).append(",");
                }
                output.deleteCharAt(output.length() - 1);
                double weight = edgeWeightCalculator.getWeightFor(output.toString());
                weightOut.set(weight);
                sourceIdOut.set(sourceVertex);
                targetIdOut.set(targetVertex);
                edgeOutput.setMatrixData(sourceIdOut, targetIdOut, weightOut);
                context.write(NullWritable.get(), edgeOutput);
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