/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.matrix.io;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author nilesh
 */
public class MultiValueWritable extends GenericWritable {

    private static Class[] CLASSES = {
        MultRowIntermediate.class,
        DoubleWritable.class
    };

    public MultiValueWritable() {
    }

    public MultiValueWritable(Writable value) {
        set(value);
    }

    protected Class[] getTypes() {
        return CLASSES;
    }
}
