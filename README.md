# Dynamic Partition Forest: An Efficient and Distributed Indexing Scheme for Similarity Search based on Hashing

This repository is the official implementation of Dynamic Partition Forest: An Efficient and Distributed Indexing Scheme for Similarity Search based on Hashing.

![Alt text](https://github.com/MacLLL/SimilaritySearchByRDF/blob/master/framework.png)

The similarity search over large-scale feature-rich data(e.g. image, video or text) is a fundamental problem and has become increasingly important in data mining research. Hashing based methods, especially Locality Sensitive Hashing(LSH), have been widely used for fast Approximate Nearest Neighbor search(ANNs). However, there are still two flaws in existing methods: (1) The state-of-the-art distribution scheme sacrificed too much accuracy for speeding up the query in practice. (2) Most LSH-based index approaches directly used the static number of compound hash values without considering the data distribution, resulting in system performance degradation. In this paper, a new index structure called Dynamic Partition Forest(DPF) is designed to hierarchically divide the high collision areas with dynamic hashing, which leads itself to auto-adapt various data distributions. A multiple-step search strategy is integrated with DPF to mitigate the accuracy loss with distributed scheme. The experiment results show that DPF increases the accuracy by 3% to 5% within the same timeframe compared to DPF without multiple-step search. Additionally, DPF with our partition scheme is 1.4 times faster than DPF without partition, which demonstrates the efficiency of our content-based distributed scheme. Experimental comparisons with other two state-of-the-art methods on three popular datasets show that DPF is 3.2 to 9 times faster to achieve the same accuracy with 17% to 78% decrease of index space.

## Requirements

This codebase is written for `java 7` and `scala 2.10.4`, and is built by `sbt 0.13.12`. Other necessary packages are including

- "com.typesafe.akka" % "akka-contrib_2.10" % "2.3.15"
- "com.typesafe" % "config" % "1.2.1"
- "com.typesafe.akka" % "akka-testkit_2.10" % "2.3.15"
- "junit" % "junit" % "4.11"
- "com.novocode" % "junit-interface" % "0.11" % "test"
- "org.scalatest" % "scalatest_2.10" % "2.2.5" % "test"
- "com.google.guava" % "guava" % "18.0"
- "org.scalanlp" %% "breeze" % "0.13.2"

## The steps to use the code to build the DPF (use scala as a simple example):

1. First, if you have the densevector, and all the vector is 100 dimensional vector, you need to set the parameter mclab.lsh.vectorDim=100;
2. Then, you need to create a LSH object based on the parameters that you set in TestSettings.scala. For example, `LSHServer.lshEngine = new LSH(TestSettings.testBaseConf)`;
3. If you use the Densevector, set the `LSHServer.isUseDense=true`, otherwise the `LSHserver.isUseDense = false`;
4. Then you need to fit the data into RDF. If the vector is Densevector, use the object DensevectorRDFInit, otherwise use the object SparsevectorRDFInit. For example, `DensevectorRDFInit.newMultiThreadFit(fileurl,TestSettings.testBaseConf)`;
5. Now you need to generate the query, which is also the Densevector. For example, we generate two random 100 dimensional vectors, values1 and values2.
6. Then we use the method NewMultiThreadQueryBatch to get the results.
7. The code is as following:
```
test("simple test"){
  LSHServer.lshEngine = new LSH(TestSettings.testBaseConf)
  LSHServer.isUseDense= true
  val allDenseVectors = DensevectorRDFInit.newMultiThreadFit("glove.twitter.27B/glove.twitter.27B.100d.20k.DenseVector.txt",
    TestSettings.testBaseConf)
  val values1 = new Array[Double](100).map(_ => Random.nextDouble())
  val values2 = new Array[Double](100).map(_ => Random.nextDouble())
  val results=DensevectorRDFInit.NewMultiThreadQueryBatch(Array(0,1),Array(new DenseVector(0,values1),new DenseVector(1,values2)),0)
  println(results(0))
  println(results(1))
}
```

The results array contains the all result for each query objects. More test is given in the `test.scala.mclab.Experiments.TestSingleRDFSuite.scala`. 

## Results

![Alt text](https://github.com/MacLLL/SimilaritySearchByRDF/blob/master/results.png)

## Papers

If you find the code useful in your research, please consider citing our paper:
```
@inproceedings{lu2018dynamic,
  title={Dynamic Partition Forest: An Efficient and Distributed Indexing Scheme for Similarity Search based on Hashing},
  author={Lu, Yangdi and Bo, Yang and He, Wenbo and Nabatchian, Amir},
  booktitle={2018 IEEE International Conference on Big Data (Big Data)},
  pages={1059--1064},
  year={2018},
  organization={IEEE}
}
```

```
@inproceedings{lu2018random,
  title={Random draw forest: A salient index for similarity search over multimedia data},
  author={Lu, Yangdi and He, Wenbo and Nabatchian, Amir},
  booktitle={2018 IEEE Fourth International Conference on Multimedia Big Data (BigMM)},
  pages={1--5},
  year={2018},
  organization={IEEE}
}
```
