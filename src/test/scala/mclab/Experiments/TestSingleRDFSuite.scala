package mclab.Experiments

import mclab.TestSettings
import mclab.deploy.{LSHServer, SingleFeatureRDFInit}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import mclab.lsh.LSH


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
    val resultArray_non = SingleFeatureRDFInit.queryBatch(queryArray)
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
    val (topK, precision) = SingleFeatureRDFInit.topKAndPrecisionScore("glove.twitter.27B/glove.twitter.27B.100d.20k.SparseVector.txt",
      "glove.twitter.27B/glove.twitter.27B.100d.20k.groundtruth", TestSettings.testBaseConf)
    topK.foreach(x => println(x.toSet))
    println("The precision is " + precision)
  }
}
