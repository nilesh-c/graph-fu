/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu;

import com.nileshc.graphfu.cdr.EdgeWeightCalculatorImpl;
import com.nileshc.graphfu.cdr.Preprocessor;
import com.nileshc.graphfu.cdr.normalizeids.hashid.HashIdRunner;
import com.nileshc.graphfu.cdr.normalizeids.translateedge.TranslateEdgeRunner;
import com.nileshc.graphfu.matrix.io.multrowcsv.MultRowCSVRunner;
import com.nileshc.graphfu.matrix.mvmult.mult.MultRunner;
import com.nileshc.graphfu.matrix.mvmult.preprocess.PreprocessRunner;
import com.nileshc.graphfu.pagerank.PageRank;
import com.nileshc.graphfu.pagerank.initvectors.InitVectorsRunner;
import com.nileshc.graphfu.pagerank.vectornorm.VectorNormalizer;
import com.nileshc.graphfu.vector.l2norm.VectorDifferenceL2NormRunner;
import com.nileshc.graphfu.vector.vvmult.VectorMultRunner;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

/**
 *
 * @author nilesh
 */
public class Main {
    
    private static final Logger LOG = Logger.getLogger(InitVectorsRunner.class);
    
    public static void main(String... args) throws IOException {
        LOG.addAppender(new ConsoleAppender(new SimpleLayout(), "System.err"));
        String inputMatrixCSV = args[0];
        String outputRankVector = args[1];
        String temp = args[2];
        
        Preprocessor p = new Preprocessor();
        p.run(inputMatrixCSV, temp + "/idnormalized", "binvdata", temp + "/output");
        
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(temp), true);
        
        long numNodes = p.getNumNodes();
        LOG.info("Number of nodes=" + numNodes);
        
        PageRank pr = new PageRank(0.85, 0.00001);
        pr.run(inputMatrixCSV, temp + "/output/binvdata", numNodes, temp + outputRankVector, temp);
        
        MultRowCSVRunner mr = new MultRowCSVRunner();
        mr.run(temp + outputRankVector, outputRankVector);
    }
}
