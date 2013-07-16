/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author nilesh
 */
public class MultRowIntermediate implements Writable {

    private LongWritable vectorRow;
    private DoubleWritable vectorValue;
    private MatrixElementListWritable matrixElements;

    public MultRowIntermediate() {
        this.vectorRow = null;
        this.vectorValue = null;
        this.matrixElements = null;
    }

    public MultRowIntermediate(LongWritable vectorRow, DoubleWritable vectorValue, MatrixElementListWritable matrixColumnElements) {
        this.vectorRow = vectorRow;
        this.vectorValue = vectorValue;
        this.matrixElements = matrixColumnElements;
    }

    public MatrixElementListWritable getMatrixElements() {
        return matrixElements;
    }

    public void setMatrixElements(MatrixElementListWritable matrixColumnElements) {
        this.matrixElements = matrixColumnElements;
    }

    public LongWritable getVectorRow() {
        return vectorRow;
    }

    public void setVectorRow(LongWritable vectorRow) {
        this.vectorRow = vectorRow;
    }

    public DoubleWritable getVectorValue() {
        return vectorValue;
    }

    public void setVectorValue(DoubleWritable vectorValue) {
        this.vectorValue = vectorValue;
    }

    public void write(DataOutput d) throws IOException {
        vectorRow.write(d);
        vectorValue.write(d);
        matrixElements.write(d);
    }

    public void readFields(DataInput di) throws IOException {
        if (vectorRow == null) {
            vectorRow = new LongWritable();
        }
        if (vectorValue == null) {
            vectorValue = new DoubleWritable();
        }
        if (matrixElements == null) {
            matrixElements = new MatrixElementListWritable();
        }

        vectorRow.readFields(di);
        vectorValue.readFields(di);
        matrixElements.readFields(di);
    }

    @Override
    public String toString() {
        return "MultRowIntermediate vectorRow:" + vectorRow + " vectorValue:" + vectorValue + "\nmatrixElements:\n" + matrixElements;
    }
}
