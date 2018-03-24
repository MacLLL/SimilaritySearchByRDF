package mclab.Experiments

import mclab.TestSettings
import mclab.deploy.{LSHServer, SingleFeatureRDFInit}
import mclab.lsh.LSH
import org.scalatest.FunSuite


class BestHashFamilySuite extends FunSuite{
  test("test performance of different steps") {
    val testNum=10
    var highestP=0.0
    for(testID <- 0 until testNum) {
      println("runing test " + testID + "***********************")
      LSHServer.lshEngine = new LSH(TestSettings.testBaseConf)
      val allDenseVectors = SparsevectorRDFInit.newMultiThreadFit("glove.twitter.27B/glove120k100dReverse.txt",
        TestSettings.testBaseConf)
      //    LSHServer.lshEngine.outPutTheHashFunctionsIntoFile()

      val groundTruth = SparsevectorRDFInit.getTopKGroundTruth("glove.twitter.27B/glove100d120k.txtQueryAndTop10NNResult1200",
        TestSettings.testBaseConf.getInt("mclab.lsh.topK"))
      var lastInOnePrecision=0.0
      for (step <- 0 to TestSettings.testBaseConf.getInt("mclab.lsh.partitionBits")) {
        val timeA = System.currentTimeMillis()
        val (topK,precision) = SparsevectorRDFInit.topKAndPrecisionScore(allDenseVectors,
          groundTruth, TestSettings.testBaseConf, step)
        val timeB = System.currentTimeMillis()
        if(lastInOnePrecision < precision){
          lastInOnePrecision = precision
        }
        println("for step=" + step + ", The precision is " + precision + ". Time is " + (timeB - timeA) / groundTruth.size.toDouble + "ms/per query")
      }
      if(highestP < lastInOnePrecision){
        highestP=lastInOnePrecision
        LSHServer.lshEngine.outPutTheHashFunctionsIntoFile()
      }
      SparsevectorRDFInit.clearAndClose()
    }
  }

}
