/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.pagerank;

import com.nileshc.graphfu.matrix.mvmult.preprocess.PreprocessRunner;
import java.io.IOException;

/**
 *
 * @author nilesh
 */
public class PageRank {

    private final double alpha;
    private double epsilon;
    private int iterations;

    public double getEpsilon() {
        return epsilon;
    }

    public void setEpsilon(double epsilon) {
        this.epsilon = epsilon;
    }

    public int getIterations() {
        return iterations;
    }

    public void setIterations(int iterations) {
        this.iterations = iterations;
    }

    public PageRank(double alpha, double epsilon) {
        this.alpha = alpha;
        this.epsilon = epsilon;
        this.iterations = 0;
    }

    public PageRank(double alpha, int iterations) {
        this.alpha = alpha;
        this.iterations = iterations;
        this.epsilon = 0;
    }

    public void run(String matrixInput, String rankVectorInput, String qVectorInput, String outputPrefix) throws IOException {
//        PreprocessRunner pr = new PreprocessRunner();
//        pr.run(matrixInput, rankVectorInput, outputPrefix);
    }
}
