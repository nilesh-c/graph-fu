package com.nileshc.graphfu.cdr;

import com.nileshc.graphfu.cdr.EdgeWeightCalculatorImpl;
import com.nileshc.graphfu.cdr.normalizeids.hashid.HashIdRunner;
import com.nileshc.graphfu.cdr.normalizeids.partitiondict.PartitionDictRunner;
import com.nileshc.graphfu.cdr.normalizeids.partitionedge.PartitionEdgeRunner;
import com.nileshc.graphfu.cdr.normalizeids.translateedge.TranslateEdgeRunner;
import com.nileshc.graphfu.matrix.mvmult.mult.MultRunner;
import com.nileshc.graphfu.matrix.mvmult.preprocess.PreprocessReducer;
import com.nileshc.graphfu.matrix.mvmult.preprocess.PreprocessRunner;
import com.nileshc.graphfu.pagerank.vectornorm.VectorNormRunner;
import com.nileshc.graphfu.pagerank.vectornorm.VectorElementSumRunner;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class Preprocessor {

    private long numNodes = 0;

    public void run(String cdrMatrixEdges, String normalizedMatrix, String binaryVdata, String temp) throws IOException {

        System.out.println("Hello World!");

        HashIdRunner hir = new HashIdRunner();
        hir.run(cdrMatrixEdges, temp, "vidmap", binaryVdata);
        numNodes = hir.getTotalVertexCount();

        PartitionDictRunner pdr = new PartitionDictRunner(64);
        pdr.run(temp + "/vidmap", temp + "/preprocess/partitionedvidmap");

        PartitionEdgeRunner per = new PartitionEdgeRunner(64);
        per.run(cdrMatrixEdges, temp + "/preprocess/partitionededata");

        TranslateEdgeRunner ter = new TranslateEdgeRunner(64, temp + "/preprocess/partitionedvidmap/");
        ter.run(temp + "/preprocess/partitionededata", normalizedMatrix, EdgeWeightCalculatorImpl.class);
    }

    public void run(String cdrMatrixEdges, String normalizedMatrix, String binaryVidMap) throws IOException {
        run(cdrMatrixEdges, normalizedMatrix, binaryVidMap, "/tmp/output");
    }

    public long getNumNodes() {
        return numNodes;
    }
}
