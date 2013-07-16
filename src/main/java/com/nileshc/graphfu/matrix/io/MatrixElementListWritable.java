/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.io;

/**
 *
 * @author nilesh
 */
public class MatrixElementListWritable extends ArrayListWritable<MatrixElement> {

    @Override
    public void setClass() {
        setClass(MatrixElement.class);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (MatrixElement element : this) {
            sb.append(element.toString()).append("\n");
        }
        return sb.toString();
    }
}
