Source code for PageRank implementation in Hadoop
=================================================

Note: How-to-use wiki pages will be added soon.

## CDR Preprocessing
The CDR dataset is anonymized and the output is stored in a custom Writable-subclass format, using Hadoop’s *SequenceFileOutputFormat*. For anonymization, the approach used by [Intel Graph-builder](http://graphlab.org/intel-graphbuilder/) is used. Dictionary based compression is applied to normalize the graph IDs, essentially hiding the telecom subscriber’s phone number and replacing it with a new mapped node ID. A map task creates a keyvalue dictionary, indexed by ordered integers and alphabetically, of all string values appearing in the adjacency list and edge/vertex data structures. The dictionary is then sharded across machines and co-located with shards of the unconverted edge list sorted alphabetically by source vertex. The local dictionaries are then used to convert the source vertex labels to integers. The edge list is then reshuffled to alphabetically shard it by destination vertex and the integer substitution repeated. This way we get a new edge list with normalized IDs, starting from 1 to N (number of vertices in the graph). After this, the obtained graph has to be row-normalized to obtained PageRank’s sparse probability vector H. The source code for this can be found in the **com.nileshc.graphfu.cdr.normalizeids** package. The row normalizer can be found at **com.nileshc.graphfu.matrix.rownormalize**.

## Single Stage Matrix Vector Multiplication tuned for PageRank
Many graph mining operations (PageRank, spectral clustering, diameter estimation, connected components etc.) are essentially a repeated matrix-vector multiplication. [Kang et al.](web.kaist.ac.kr/~ukang/‎) have proposed a highly optimized algorithm for matrix-vector multiplication called GIM-V (Generalized Iterated Matrix-Vector multiplication). **GIM-V** is a 2-stage algorithm, that is, every time a matrix is multiplied with a vector, two Hadoop jobs need to be executed sequentially. It consists of 3 steps, combine2 (multiplication of a matrix element mij and a vector element vj ), combineAll (summing up n multiplication results for a node i in the vector) and assign (replace the previous value of vi with the new value obtained from combineAll), the first step being implemented with one Hadoop job, and the other two steps in another job.

[Setiawan](http://math.uwaterloo.ca/computational-mathematics/sites/ca.computational-mathematics/files/uploads/files/Johann_Setiawan.pdf) has proposed an algorithm, SSMV (Single-Stage Matrix Vector Multiplication) that may be faster than GIM-V for medium scale datasets - it is more suited to iterative MapReduce algorithms such as PageRank. In this project, a modification of SSMV has  been implemented. The second stage of the SSMV algorithm has been modified to take an extra parameter. The source code for this can be found in the **com.nileshc.graphfu.matrix.mvmult** package.

## Generating initial vectors
The PageRank implementation contains a Hadoop job for generating the dangling node vector and the initial rank vector. The MapReduce algorithm for it goes as follows - The Mapper takes *MatrixElement* (extends *Writable*) values as input and writes (element.row, element) as output, where the row is the key, and the element is sent as the value. The Mapper is fed the exhaustive list of vertices, and also the whole edge list. The Reducer takes the Mapper’s key value pair output as input, with all values for a key grouped together. For each key it writes (key, 1/n) to the rank vector output, where n is the total number of vertices, already given as as a parameter. If no value is present for this key, the Reducer writes an entry to the dangling node vector output. *MultipleOutputs* is used to write to two output paths from the Reducer. The source code for this can be found in the **com.nileshc.graphfu.pagerank.initvectors** package.

**TODO:** Suffers from out of memory errors on large dataset - use block multiplication or switch to GIM-V

## Computing Scalar Product of Two Vectors
This Hadoop job is used to compute the product of a row vector and a column vector, thus yielding a scalar value. Both vectors are fed into the Mapper, which writes (vector.row, vector) as output (internally both vectors are stored as single column matrices). The reducer takes the pairs of values for each key, multiplies them and adds them to a sum variable which is written to the output as value with a *NullWritable* key, in the cleanup() method.

The source code for this can be found in the **com.nileshc.graphfu.vector.vvmult** package.

## Normalizing the PageRank Vector
A 2-stage implementation has been built to normalize the PageRank vector by the total sum of its elements (L1-Norm). It consists of two Hadoop jobs, the first one used to find the sum of all the elements in the vector, and the second one used to divide all elements in the vector by that sum. In stage 1, a naive implementation is used where an *Identity Mapper* is used to write the input *MatrixElement* values to the output. A single Reducer is run to iterate over those values and compute their sum. This will be optimized in the future.

The source code for this can be found in the **com.nileshc.graphfu.vector.normalize** package.

## Testing for Convergence : L2-Norm in MapReduce
This Hadoop job is used to compute the L2-Norm of the difference of two vectors. To compute the L2-Norm in Hadoop, a similar naive implementation has been built, where an identity mapper pushes all the vector elements for both vectors to the reducer by writing (vector.row, vector) as output. The reducer takes a pair of value for each key, subtracts one from the other and adds the square of the difference to a sum variable. In the cleanup() method of the reducer, the square root of this sum is calculated and written to output.

After every K iterations (where K is a constant interval like 5 or 10), the previous rank vector and the current rank vector is fed into the *VectorDifferenceL2NormRunner*. If the resultant value is less than a very small constant epsilon, it can be said that the algorithm has converged, and further iterations are stopped.

The source code for this can be found in the **com.nileshc.graphfu.vector.l2norm** package.

## Iterative MapReduce-based PageRank Algorithm
The above MapReduce jobs are used together iteratively to compute the PageRank, given a pre-processed matrix as input.

First, the initial PageRank vector and the dangling node vector are computed. Stage 1 of PageRank-tuned SSMV is run just once.

Until convergence is achieved, a loop is run where the current rank vector is multiplied with the dangling node vector to get a scalar result. This scalar multiplied by α and the result is added with (1 − α), then the sum is divided by n (total number of vertices). The final result is, therefore, (απ (k)T a + 1 − α)/n. This value is passed as a parameter to PageRank-tuned SSMV Stage 2, which gives a new rank vector. This rank vector is normalized as shown above using
**com.nileshc.graphfu.vector.normalize**.

The **com.nileshc.graphfu.pagerank** package has the iterative PageRank driver code.
