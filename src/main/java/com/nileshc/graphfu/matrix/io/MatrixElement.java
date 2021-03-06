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
    private ElementType elementType;

    private enum ElementType {

        MATRIX,
        VECTOR,
        ROW
    };

    public MatrixElement() {
        this.row = null;
        this.column = null;
        this.value = null;
        elementType = null;
    }

    public MatrixElement(LongWritable row, LongWritable column, DoubleWritable value) {
        this.row = row;
        this.column = column;
        this.value = value;
        elementType = ElementType.MATRIX;
    }

    public MatrixElement(LongWritable row, DoubleWritable value) {
        this.row = row;
        this.column = new LongWritable(0);
        this.value = value;
        elementType = ElementType.VECTOR;
    }

    public MatrixElement(LongWritable row) {
        this.row = row;
        this.column = new LongWritable(0);
        this.value = new DoubleWritable(0);
        elementType = ElementType.ROW;
    }

    public void setMatrixData(LongWritable row, LongWritable column, DoubleWritable value) {
        this.row = row;
        this.column = column;
        this.value = value;
        elementType = ElementType.MATRIX;
    }

    public void setVectorData(LongWritable row, DoubleWritable value) {
        this.row = row;
        this.column = new LongWritable(0);
        this.value = value;
        elementType = ElementType.VECTOR;
    }

    public void setRowData(LongWritable row) {
        this.row = row;
        this.column = new LongWritable(0);
        this.value = new DoubleWritable(0);
        elementType = ElementType.ROW;
    }

    public void write(DataOutput d) throws IOException {
        row.write(d);
        column.write(d);
        value.write(d);
        d.writeUTF(elementType.name());
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

        row.readFields(di);
        column.readFields(di);
        value.readFields(di);
        elementType = ElementType.valueOf(di.readUTF());
    }

    public boolean isVector() {
        return elementType == ElementType.VECTOR;
    }

    public boolean isMatrix() {
        return elementType == ElementType.MATRIX;
    }

    public boolean isRow() {
        return elementType == ElementType.ROW;
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
        return "MatrixElement Row:" + row + " Column:" + column + " Value:" + value + " elementType:" + elementType.name();
    }
}
