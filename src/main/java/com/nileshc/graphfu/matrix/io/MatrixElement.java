/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.io;

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

    private LongWritable row = null;
    private LongWritable column = null;
    private DoubleWritable value = null;
    private BooleanWritable isVector = null;

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
        this.column = new LongWritable(0);
        this.value = value;
        this.isVector = new BooleanWritable(true);
    }

    public void setVectorData(LongWritable row, DoubleWritable value) {
        this.row = row;
        this.column = new LongWritable(0);
        this.value = value;
        this.isVector = new BooleanWritable(true);
    }

    public void setMatrixData(LongWritable row, LongWritable column, DoubleWritable value) {
        this.row = row;
        this.column = column;
        this.value = value;
        this.isVector = new BooleanWritable(false);
    }

    public void write(DataOutput d) throws IOException {
        row.write(d);
        column.write(d);
        value.write(d);
        isVector.write(d);
    }

    public void readFields(DataInput di) throws IOException {
        if (row == null) {
            row = new LongWritable();
        }
        if (column == null) {
            column = new LongWritable();
        }
        if (value == null) {
            value = new DoubleWritable();
        }
        if (isVector == null) {
            isVector = new BooleanWritable();
        }

        row.readFields(di);
        column.readFields(di);
        value.readFields(di);
        isVector.readFields(di);
    }

    public boolean isVector() {
        return isVector.get();
    }
    
    public LongWritable getColumn() {
        return column;
    }

    public LongWritable getRow() {
        return row;
    }

    public DoubleWritable getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Row:" + row + "\nColumn:" + column + "\nValue:" + value;
    }
}
