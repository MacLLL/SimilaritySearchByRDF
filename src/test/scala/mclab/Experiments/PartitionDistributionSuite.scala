package mclab.Experiments

import com.typesafe.config.ConfigFactory
import mclab.TestSettings
import mclab.deploy.LSHServer
import mclab.lsh.LSH
import mclab.lsh.vector.{SparseVector, Vectors}
import mclab.utils.LocalitySensitivePartitioner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.math._

/**
  * Created by eternity on 10/27/17.
  */
class PartitionDistributionSuite extends FunSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    LSHServer.lshEngine = new LSH(TestSettings.testBaseConf)
  }

  val partitionBits = 4
  val confForPartitioner = ConfigFactory.parseString(
    s"""
       |mclab.lsh.vectorDim=32
       |mclab.lsh.chainLength=$partitionBits
      """.stripMargin).withFallback(TestSettings.testBaseConf)

  val partition = new LocalitySensitivePartitioner[Int](confForPartitioner, 1, partitionBits)

  /**
    * Load the feature data file and ground truth file
    *
    * @param filename1 feature data file
    * @param filename2 ground truth file
    * @param k         top k to be analyzed, default as 10
    * @return the list of feature vectors, and the list of ground truth
    */
  def loadVectorFile(filename1: String, filename2: String, k: Int = 10): (ListBuffer[SparseVector], ListBuffer[Array[Int]]) = {
    println("Start load the data...")
    val allSparseVector = new ListBuffer[SparseVector]
    var count = 0
    for (line <- Source.fromFile(getClass.getClassLoader.getResource(filename1).getFile).getLines()) {
      val tmp = Vectors.fromPythonString(line)
      val currentSparseVector = new SparseVector(tmp._1, tmp._2, tmp._3, tmp._4)
      allSparseVector += currentSparseVector
      count += 1
      if (count % 10000 == 0) {
        println(count)
      }
    }
    val resultsArray: ListBuffer[Array[Int]] = new ListBuffer[Array[Int]]
    for (line <- Source.fromFile(getClass.getClassLoader.getResource(filename2).getFile).getLines()) {
      resultsArray += Vectors.analysisKNN(line, k)
    }
    println("Finish load the feature data and ground truth.")
    (allSparseVector, resultsArray)
  }
  /**
    * Calculate the distribution of objects into different sub-indexs
    *
    * @param allSparseVector
    * @param resultsArray
    * @param hashFamilyID
    * @param k
    * @return
    */
  def stepwiseDistribution(allSparseVector: ListBuffer[SparseVector],
                           resultsArray: ListBuffer[Array[Int]], hashFamilyID: Int = 1, k: Int = 10): (Double, Double, Double) = {
    //highest, step one, step two. step 3
    val finalStepsResult: Array[Double] = new Array(partitionBits + 1)
    for (eachGT <- resultsArray) {
      val stepsResults: Array[Double] = new Array(partitionBits + 1)
      val distribution: Array[Int] = new Array(pow(2, 4).toInt)
      for (oneObject <- eachGT.slice(0, k)) {
        val sub_indexID = partition.getPartition(LSHServer.lshEngine.calculateIndex(allSparseVector(oneObject),
          hashFamilyID)(0))
        distribution(sub_indexID) += 1
      }
      //sort the distribution
      val sortedDistribution = distribution.zipWithIndex.sorted
      //take the sub-index-ID of highest one
      val highest = sortedDistribution(pow(2, partitionBits).toInt - 1)._2
      //put the highest one in result
      stepsResults(0) += sortedDistribution(pow(2, partitionBits).toInt - 1)._1
      for (i <- sortedDistribution.indices) {
        //add the ?-step distribution into result
        if (sortedDistribution(i)._2 != highest)
          stepsResults(Integer.bitCount(sortedDistribution(i)._2 ^ highest)) += sortedDistribution(i)._1
      }
      for (i <- stepsResults.indices) {
        finalStepsResult(i) += stepsResults(i) / resultsArray.toArray.length.toDouble
      }
    }
    println("Finish calculate hash family ID = " + hashFamilyID)
    finalStepsResult.foreach(x => print(x / k * 100.0 + "% "))
    println()
    (finalStepsResult(0), finalStepsResult(1), finalStepsResult(2))
  }

  test("TOP k partition distribution-------->dataset:Glove") {
//    val topKArray = Array(10,30,50,70,90)
    val topKArray = Array(10)
    val tuneRatio = 3
    var flag = false
    val (allSparseVector, resultsArray) = loadVectorFile("glove.twitter.27B/glove.twitter.27B.100d.20k.SparseVector.txt",
      "glove.twitter.27B/glove.twitter.27B.100d.20k.groundtruth", 10)
    var highestHashFamilyID = 0
    for (k <- topKArray) {
      println("********************k=" + k + "*************************")
      //to pick the best hash function wisely
      if (!flag) {
        var highestPercent = 0.0
        var secondHighestPercent = 0.0
        for (hashId <- 0 until TestSettings.testBaseConf.getInt("mclab.lsh.tableNum")) {
          val (tmp1, tmp2, tmp3) = stepwiseDistribution(allSparseVector, resultsArray, hashId, k)
          if (tmp1 >= highestPercent && tmp1 - highestPercent > secondHighestPercent - tmp2 - tuneRatio / 100.0 * k) {
            highestPercent = tmp1
            secondHighestPercent = tmp2
            highestHashFamilyID = hashId
          }
        }
        LSHServer.lshEngine.outPutTheHashFunctionsIntoFile(highestHashFamilyID)
        flag = true
      } else {
        stepwiseDistribution(allSparseVector, resultsArray, highestHashFamilyID, k)
      }
    }
  }
}