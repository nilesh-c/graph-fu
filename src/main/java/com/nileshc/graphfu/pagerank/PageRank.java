/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu.pagerank;

import com.nileshc.graphfu.cdr.normalizeids.hashid.HashIdRunner;
import com.nileshc.graphfu.matrix.mvmult.mult.MultRunner;
import com.nileshc.graphfu.matrix.mvmult.preprocess.PreprocessRunner;
import com.nileshc.graphfu.pagerank.initvectors.InitVectorsRunner;
import com.nileshc.graphfu.vector.l2norm.VectorDifferenceL2NormRunner;
import com.nileshc.graphfu.vector.vvmult.VectorMultRunner;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author nilesh
 */
public class PageRank {

    private final double alpha;
    private double epsilon;
    private int iterations;
    private int currentIteration;
    private int checkConvergeInterval;
    private String probabilityMatrixInput;
    private String vDataInput;
    private long numNodes;
    private String rankVectorOutput;
    private String temp;
    private FileSystem fs;

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

    public PageRank(double alpha, int iterations) throws IOException {
        this.alpha = alpha;
        this.iterations = iterations;
        this.epsilon = 0;
        fs = FileSystem.get(new Configuration());
    }

    public void run(String probabilityMatrixInput, String vDataInput, long numNodes, String rankVectorOutput, String temp) throws IOException {
        this.checkConvergeInterval = 10;
        this.currentIteration = 0;
        this.probabilityMatrixInput = probabilityMatrixInput;
        this.vDataInput = vDataInput;
        this.numNodes = numNodes;
        this.rankVectorOutput = rankVectorOutput;
        this.temp = temp;

        InitVectorsRunner ivr = new InitVectorsRunner(numNodes);
        ivr.run(probabilityMatrixInput, vDataInput, temp + "/initvectors", temp + "/danglingvector", temp + "/rankvectors/rankvector-0");

        PreprocessRunner pr = new PreprocessRunner();
        pr.run(probabilityMatrixInput, temp + "/rankvectors/rankvector-0", temp + "/intermatrix-0");

        VectorMultRunner vmr = new VectorMultRunner();
        vmr.run(temp + "/intermatrix-0", temp + "/danglingvector", temp + "/vectormult");
        
        
//        MultRunner mr = new MultRunner();
//        mr.run(temp + "/intermatrix-0", temp + "/intermatrix-1");

        do {
            
        } while (!isFinished());
    }

    public void run(String matrixInput, String vDataInput, long numNodes, String vectorOutput) throws IOException {
        run(matrixInput, vDataInput, numNodes, vectorOutput, "/tmp/output");
    }

    private boolean isFinished() throws IOException {
        if (iterations == 0) {
            if (currentIteration % checkConvergeInterval == 0) {
                VectorDifferenceL2NormRunner vdnr = new VectorDifferenceL2NormRunner();
                vdnr.run(temp + "/rankvectors/rankvector-" + currentIteration, temp + "/rankvectors/rankvector-" + (currentIteration - 1), temp + "l2norm");
                deleteFromHDFS(temp + "l2norm", true);
                return vdnr.getL2Norm() <= epsilon;
            } else {
                return false;
            }
        } else {
            return currentIteration > iterations;
        }
    }

    private void deleteFromHDFS(String file, boolean recursive) throws IOException {
        fs.delete(new Path(file), recursive);
    }
}
