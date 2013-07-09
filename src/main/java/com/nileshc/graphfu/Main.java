package com.nileshc.graphfu;

import com.nileshc.graphfu.cdr.normalizeids.hashid.HashIdRunner;
import com.nileshc.graphfu.cdr.normalizeids.partitiondict.PartitionDictRunner;
import com.nileshc.graphfu.cdr.normalizeids.partitionedge.PartitionEdgeRunner;
import com.nileshc.graphfu.cdr.normalizeids.translateedge.TranslateEdgeRunner;
import com.nileshc.graphfu.matrix.mvmult.preprocess.PreprocessReducer;
import com.nileshc.graphfu.matrix.mvmult.preprocess.PreprocessRunner;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class Main {

    public static void main(String[] args) throws IOException {
        System.out.println("Hello World!");
        String input = "/input";
        String output = "/output";

        HashIdRunner hir = new HashIdRunner();
        hir.run(input + "/edata", output + "/hashidoutput", "vidmap");

        PartitionDictRunner pdr = new PartitionDictRunner(64);
        pdr.run(output + "/hashidoutput/vidmap-r-*", output + "/temp/partitionedvidmap");

        PartitionEdgeRunner per = new PartitionEdgeRunner(64);
        per.run(input + "/edata", output + "/temp/partitionededata");

        TranslateEdgeRunner ter = new TranslateEdgeRunner(64, output + "/temp/partitionedvidmap/");
        ter.run(output + "/temp/partitionededata", output + "/edata");
    }
}
