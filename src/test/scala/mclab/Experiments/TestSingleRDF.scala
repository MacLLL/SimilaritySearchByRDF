package mclab.Experiments

import java.util

import com.typesafe.config.ConfigFactory
import mclab.TestSettings
import mclab.deploy.{LSHServer, SingleFeatureRDFInit}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import mclab.lsh.LSH

/**
  * Created by eternity on 1/22/18.
  */
class TestSingleRDF extends FunSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    LSHServer.lshEngine = new LSH(TestSettings.testBaseConf)
  }

  test("read from file") {
    val startTime = System.currentTimeMillis()
    val allDenseVectors = SingleFeatureRDFInit.newFastFit("glove.twitter.27B/glove120k100dReverse.txt", TestSettings.testBaseConf)
    val endTime = System.currentTimeMillis()
    println(endTime - startTime)

  }
  test("test multiThread read from file") {
    val startTime = System.currentTimeMillis()
    val allDenseVector = SingleFeatureRDFInit.newMultiThreadFit("glove.twitter.27B/glove120k100dReverse.txt", TestSettings.testBaseConf)
    val endTime = System.currentTimeMillis()
    println("fit time is " + (endTime - startTime) / 1000 + "s.")
    val a = System.currentTimeMillis()
    val resultArray_multiThread = SingleFeatureRDFInit.NewMultiThreadQueryBatch(Array(5, 1, 2), 5)
    val b = System.currentTimeMillis()
    println("multiThread query time is " + (b - a) + "ms")
    val c = System.currentTimeMillis()
    val resultArray_non = SingleFeatureRDFInit.queryBatch(Array(5, 1, 2))
    val d = System.currentTimeMillis()
    println("non-multiThread query time is " + (d - c) + "ms")
    println(resultArray_multiThread(0) == resultArray_non(0))
    println(resultArray_multiThread(1) == resultArray_non(1))
    println(resultArray_multiThread(2) == resultArray_non(2))
  }

  test("test read from file"){
    val groundTruth=SingleFeatureRDFInit.getTopKGroundTruth("glove.twitter.27B/glove100d120k.txtQueryAndTop10NNResult1200",10);
    for(x <- groundTruth){
      println(x)
    }
  }


}
