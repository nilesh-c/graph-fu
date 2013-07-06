package com.nileshc.graphfu;

import com.nileshc.graphfu.cdr.preprocess.HashIdRunner;
import com.nileshc.graphfu.cdr.preprocess.PartitionDictRunner;
import com.nileshc.graphfu.cdr.preprocess.PartitionEdgeRunner;
import com.nileshc.graphfu.cdr.preprocess.UniqueExtractRunner;
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
        
        UniqueExtractRunner uer = new UniqueExtractRunner();
        uer.run(input + "/edata", "/output", "vdata");
        
        HashIdRunner hir = new HashIdRunner();
        hir.run(input + "/vdata", output, "vidmap");
        
        PartitionDictRunner pdr = new PartitionDictRunner(64);
        pdr.run(output + "/vidmap-r-*", output + "/temp/partitionedvidmap");
        
        PartitionEdgeRunner per = new PartitionEdgeRunner(64);
        per.run(input + "/edata", output + "/temp/partitionededata");
    }
}
