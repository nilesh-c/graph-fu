package com.nileshc.graphfu;

import com.nileshc.graphfu.cdr.preprocess.HashIdRunner;
import com.nileshc.graphfu.cdr.preprocess.PartitionDictRunner;
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
        HashIdRunner hir = new HashIdRunner();
        hir.run(input, output, "vidmap");
//        UniqueExtractRunner uer = new UniqueExtractRunner();
//        uer.run("/input", "/output", "vdata");
        PartitionDictRunner pdr = new PartitionDictRunner(64);
        pdr.run(output + "/vidmap-r-*", output + "/temp/partitionedvidmap");
        
    }
}
