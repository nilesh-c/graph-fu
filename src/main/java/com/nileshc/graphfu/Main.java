/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.nileshc.graphfu;

import com.nileshc.graphfu.cdr.EdgeWeightCalculatorImpl;
import com.nileshc.graphfu.cdr.Preprocessor;
import com.nileshc.graphfu.cdr.normalizeids.hashid.HashIdRunner;
import com.nileshc.graphfu.cdr.normalizeids.translateedge.TranslateEdgeRunner;
import com.nileshc.graphfu.matrix.mvmult.mult.MultRunner;
import com.nileshc.graphfu.matrix.mvmult.preprocess.PreprocessRunner;
import com.nileshc.graphfu.pagerank.initvectors.InitVectorsRunner;
import com.nileshc.graphfu.pagerank.vectornorm.VectorNormalizer;
import com.nileshc.graphfu.vector.l2norm.VectorDifferenceL2NormRunner;
import com.nileshc.graphfu.vector.vvmult.VectorMultRunner;
import java.io.IOException;
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
//        LOG.addAppender(new ConsoleAppender(new SimpleLayout(), "System.err"));
//        Preprocessor p = new Preprocessor();
//        p.run("/input", "/output/idnormalized", "binvdata", "/tmp/output");
//        long numNodes = p.getNumNodes();
//        LOG.info("numnodes=" + numNodes);
//        InitVectorsRunner ivr = new InitVectorsRunner(3);
//        boolean a = ivr.run("/output/idnormalized", "/tmp/output/binvdata*", "/tmp/output/initvectors", "danglingvector", "rankvector0");
//        PreprocessRunner pr = new PreprocessRunner();
//        pr.run("/output/idnormalized/part*", "/tmp/output/initvectors/rankvector0-r*", "/tmp/output/intermatrix0");
//        MultRunner mr = new MultRunner(3);
//        mr.run("/tmp/output/intermatrix0", "/tmp/output/intermatrix1");
//        VectorNormalizer.normalize("/tmp/output/intermatrix1", "/tmp/output/intermatrix1-norm");
//        mr.run("/tmp/output/intermatrix1-norm", "/tmp/output/intermatrix2");
//        VectorNormalizer.normalize("/tmp/output/intermatrix2", "/tmp/output/intermatrix2-norm");
//        VectorDifferenceL2NormRunner vdnr = new VectorDifferenceL2NormRunner();
//        vdnr.run("/tmp/output/intermatrix1-norm", "/tmp/output/intermatrix2-norm", "/tmp/output/vdnr");
//        LOG.info("l2norm is : " + vdnr.getL2Norm());
        VectorMultRunner vmr = new VectorMultRunner();
        vmr.run("/tmp/output/intermatrix1-norm", "/tmp/output/initvectors/danglingvector", "/tmp/output/vmr");
        LOG.info("Product is : " + vmr.getProduct());
    }
}
