/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author nilesh
 */
public class MatrixElement implements Writable {

    private LongWritable row;
    private LongWritable column;
    private DoubleWritable value;
    private BooleanWritable isVector;

    public MatrixElement() {
        this.row = null;
        this.column = null;
        this.value = null;
        this.isVector = null;
    }

    public MatrixElement(LongWritable row, LongWritable column, DoubleWritable value) {
        this.row = row;
        this.column = column;
        this.value = value;
        this.isVector = new BooleanWritable(false);
    }

    public MatrixElement(LongWritable row, DoubleWritable value) {
        this.row = row;
        this.column = null;
        this.value = value;
        this.isVector = new BooleanWritable(true);
    }

    public void write(DataOutput d) throws IOException {
        row.write(d);
        column.write(d);
        value.write(d);
        isVector.write(d);
    }

    public void readFields(DataInput di) throws IOException {
        if (row != null) {
            row = new LongWritable();
        }
        if (column != null) {
            column = new LongWritable();
        }
        if (value != null) {
            value = new DoubleWritable();
        }
        if (isVector != null) {
            isVector = new BooleanWritable();
        }

        row.readFields(di);
        column.readFields(di);
        value.readFields(di);
        isVector.readFields(di);
    }
}
