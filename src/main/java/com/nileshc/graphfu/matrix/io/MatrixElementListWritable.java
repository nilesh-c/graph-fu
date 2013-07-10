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
}
