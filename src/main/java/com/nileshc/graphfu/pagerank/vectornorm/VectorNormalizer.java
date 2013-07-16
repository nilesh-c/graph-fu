/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.pagerank.vectornorm;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author nilesh
 */
public class VectorNormalizer {
    
    public static void normalize(String input, String output) throws IOException {
        VectorElementSumRunner vesr = new VectorElementSumRunner();
        vesr.run(input, output);
        double vectorSum = vesr.getVectorSum();
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(output), true);
        VectorNormRunner vnr = new VectorNormRunner(vectorSum);
        vnr.run(input, output);
    }
}
