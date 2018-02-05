package mclab.Experiments

import mclab.TestSettings
import mclab.deploy.{LSHServer, SingleFeatureRDFInit}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import mclab.lsh.LSH
import mclab.lsh.vector.SparseVector

import scala.util.Random


/**
  * Test the methods in object SingleFeatureRDFInit
  */
class TestSingleRDFSuite extends FunSuite with BeforeAndAfterAll {
  /**
    * initialize the hash functions based on configuration file
    */
  override def beforeAll(): Unit = {
    LSHServer.lshEngine = new LSH(TestSettings.testBaseConf)
  }

  test("test fit the feature data into index: (non-multi-thread version) and (multi-threads version)") {
    val timeA = System.currentTimeMillis()
    SingleFeatureRDFInit.newFastFit("glove.twitter.27B/glove.twitter.27B.100d.20k.SparseVector.txt", TestSettings.testBaseConf)
    val timeB = System.currentTimeMillis()
    println("fit the feature data into index(non-multi-thread version), time is " + (timeB - timeA) / 1000.0 + "s.")
    SingleFeatureRDFInit.clearAndClose()
    val timeC = System.currentTimeMillis()
    SingleFeatureRDFInit.newMultiThreadFit("glove.twitter.27B/glove.twitter.27B.100d.20k.SparseVector.txt", TestSettings.testBaseConf)
    val timeD = System.currentTimeMillis()
    println("fit the feature data into index(multi-threads version), time is " + (timeD - timeC) / 1000.0 + "s.")
  }

  test("test query the index: (non-multi-threads version) and (multi-threads version)") {
    SingleFeatureRDFInit.newMultiThreadFit("glove.twitter.27B/glove.twitter.27B.100d.20k.SparseVector.txt", TestSettings.testBaseConf)
    //generate the queryArray
    val queryArray = new Array[Int](100)
    for (i <- 0 until 100) {
      queryArray(i) = i
    }
    //Todo: check why different
    val timeA = System.currentTimeMillis()
    val resultArray_multiThread = SingleFeatureRDFInit.NewMultiThreadQueryBatch(queryArray, 5)
    val timeB = System.currentTimeMillis()
    println("multiThread query time is " + (timeB - timeA) + "ms")
    val timeC = System.currentTimeMillis()
    val resultArray_non = SingleFeatureRDFInit.queryBatch(queryArray, 0)
    val timeD = System.currentTimeMillis()
    println("non-multiThread query time is " + (timeD - timeC) + "ms")
    //check whether the results are the same!
    for (i <- 0 until 100) {
      assert(resultArray_multiThread(i) == resultArray_non(i))
    }
  }

  test("test read the ground truth from file") {
    val groundTruth = SingleFeatureRDFInit.getTopKGroundTruth("glove.twitter.27B/glove100d120k.txtQueryAndTop10NNResult1200", 10);
    for (x <- groundTruth) {
      println(x)
    }
  }

  test("test the topK and precision") {
    val allDenseVectors = SingleFeatureRDFInit.newMultiThreadFit("glove.twitter.27B/glove.twitter.27B.100d.20k.SparseVector.txt", TestSettings.testBaseConf)
    val groundTruth = SingleFeatureRDFInit.getTopKGroundTruth("glove.twitter.27B/glove.twitter.27B.100d.20k.groundtruth",
      TestSettings.testBaseConf.getInt("mclab.lsh.topK"))
    val (topK, precision) = SingleFeatureRDFInit.topKAndPrecisionScore(allDenseVectors,
      groundTruth, TestSettings.testBaseConf)
    topK.foreach(x => println(x.toSet))
    println("The precision is " + precision)
  }


  test("test NewMultiThreadQueryBatch by using SparseVector,rather than key in dataTable") {
    SingleFeatureRDFInit.newMultiThreadFit("glove.twitter.27B/glove.twitter.27B.100d.20k.SparseVector.txt", TestSettings.testBaseConf)
    assert(SingleFeatureRDFInit.vectorIdToVector.size() == 20000)

    val indices = Range(0, 100).toArray
    val values1 = new Array[Double](100).map(_ => Random.nextDouble())
    val values2 = new Array[Double](100).map(_ => Random.nextDouble())

    val x = SingleFeatureRDFInit.NewMultiThreadQueryBatch(Array(new SparseVector(0, 100, indices, values1),
      new SparseVector((0, 100, indices, values2))), 0, 5)
    for (item <- x)
      println(item)
  }

  test("test getSimilarWithStepWise methods in RDF ") {
    SingleFeatureRDFInit.newMultiThreadFit("glove.twitter.27B/glove.twitter.27B.100d.20k.SparseVector.txt", TestSettings.testBaseConf)
    println("normal search, result number is " + SingleFeatureRDFInit.vectorDatabase(0).getSimilar(0).size())
    println("step-0 search, result number is " + SingleFeatureRDFInit.vectorDatabase(0).getSimilarWithStepWise(0, 0).size())
    println("step-1 search, result number is " + SingleFeatureRDFInit.vectorDatabase(0).getSimilarWithStepWise(0, 1).size())
    println("Step-2 search, result number is " + SingleFeatureRDFInit.vectorDatabase(0).getSimilarWithStepWise(0, 2).size())
  }

  test("test performance of different steps") {
    val allDenseVectors = SingleFeatureRDFInit.newMultiThreadFit("glove.twitter.27B/glove120k100dReverse.txt",
      TestSettings.testBaseConf)
    val groundTruth = SingleFeatureRDFInit.getTopKGroundTruth("glove.twitter.27B/glove100d120k.txtQueryAndTop10NNResult1200",
      TestSettings.testBaseConf.getInt("mclab.lsh.topK"))
    for (step <- 0 to TestSettings.testBaseConf.getInt("mclab.lsh.partitionBits")) {
      val timeA = System.currentTimeMillis()
      val (topK, precision) = SingleFeatureRDFInit.topKAndPrecisionScore(allDenseVectors,
        groundTruth, TestSettings.testBaseConf, step)
      val timeB = System.currentTimeMillis()
      println("for step=" + step + ", The precision is " + precision + ". Time is " + (timeB - timeA) / groundTruth.size.toDouble + "ms/per query")
    }
  }
  test("test number of objects in sub-indexes distribution") {
    SingleFeatureRDFInit.newMultiThreadFit("glove.twitter.27B/glove120k100dReverse.txt",
      TestSettings.testBaseConf)
    //see the dataTable distribution, since it's default hash salt, each sub-index has the same percentage
    //but the hashTable distribution are different
    val (dtDistribution, htDistribution) = SingleFeatureRDFInit.getDtAndHtNumDistribution()
    print("dataTable distribution: ")
    dtDistribution.foreach(x => print(x*100 + "% "))
    println("\nhashTable distribution: ")
    htDistribution.foreach(x => print(x*100 + "% "))
  }


}
