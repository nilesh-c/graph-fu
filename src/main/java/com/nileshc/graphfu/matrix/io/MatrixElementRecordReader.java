/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.io;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.log4j.Logger;

/**
 *
 * @author nilesh
 */
public class MatrixElementRecordReader extends RecordReader<LongWritable, MatrixElement> {

    private static final Logger LOG = Logger.getLogger(MatrixElementRecordReader.class);
    private LongWritable key = null;
    private MatrixElement value = null;
    private LineRecordReader reader = new LineRecordReader();

    @Override
    public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
        reader.initialize(is, tac);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean gotNextKeyValue = reader.nextKeyValue();
        if (gotNextKeyValue) {
            if (value == null) {
                value = new MatrixElement();
            }
            key = reader.getCurrentKey();
            Text line = reader.getCurrentValue();
            String[] tokens = line.toString().split(",");
            if (tokens.length == 3) { // We have a matrix element
                value.setMatrixData(new LongWritable(Long.parseLong(tokens[0])),
                        new LongWritable(Long.parseLong(tokens[1])),
                        new DoubleWritable(Double.parseDouble(tokens[2])));
            } else { // We have a vector element
                value.setVectorData(new LongWritable(Long.parseLong(tokens[0])),
                        new DoubleWritable(Double.parseDouble(tokens[1])));
            }
        } else {
            key = null;
            value = null;
        }
        return gotNextKeyValue;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public MatrixElement getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return reader.getProgress();
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
