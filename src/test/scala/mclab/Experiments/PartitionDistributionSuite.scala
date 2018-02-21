package mclab.Experiments

import com.typesafe.config.ConfigFactory
import mclab.TestSettings
import mclab.deploy.LSHServer
import mclab.lsh.LSH
import mclab.lsh.vector.{DenseVector, SparseVector, Vectors}
import mclab.utils.LocalitySensitivePartitioner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source
import scala.math._

/**
  * Created by eternity on 10/27/17.
  */
class PartitionDistributionSuite extends FunSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    LSHServer.lshEngine = new LSH(TestSettings.testBaseConf)
    LSHServer.isUseDense= true
  }

  val partitionBits = 2
  val confForPartitioner = ConfigFactory.parseString(
    s"""
       |mclab.confType=partition
       |mclab.lsh.vectorDim=32
       |mclab.lsh.tableNum = 1
       |mclab.lshTable.chainLength=$partitionBits
       |mclab.lsh.generateMethod=default
      """.stripMargin).withFallback(TestSettings.testBaseConf)



  /**
    * Load the feature data file and ground truth file
    *
    * @param filename1 feature data file
    * @param filename2 ground truth file
    * @param k         top k to be analyzed, default as 10
    * @return the list of feature vectors, and the list of ground truth
    */
  def loadVectorFile(filename1: String, filename2: String, k: Int = 10): (ListBuffer[DenseVector], ListBuffer[Array[Int]]) = {
    println("Start load the data...")
//    val allSparseVector = new ListBuffer[DenseVector]
    val allDenseVector =new ListBuffer[DenseVector]
    var count = 0
    for (line <- Source.fromFile(getClass.getClassLoader.getResource(filename1).getFile).getLines()) {
      val tmp = Vectors.parseDense(line)
//      val currentSparseVector = new SparseVector(tmp._1, tmp._2, tmp._3, tmp._4)
      val currentDenseVector = new DenseVector(tmp._1,tmp._2)
//      allSparseVector += currentSparseVector
      allDenseVector += currentDenseVector
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
    (allDenseVector, resultsArray)
  }
  /**
    * Calculate the distribution of objects into different sub-indexs
    *
    * @param allDenseVector
    * @param resultsArray
    * @param hashFamilyID
    * @param k
    * @return
    */
  def stepwiseDistribution(partition:LocalitySensitivePartitioner[Int],allDenseVector: ListBuffer[DenseVector],
                           resultsArray: ListBuffer[Array[Int]], hashFamilyID: Int = 1, k: Int = 10,paritionID:Int): (Double, Double, Double) = {
    //highest, step one, step two. step 3
    val finalStepsResult: Array[Double] = new Array(partitionBits + 1)
    for (eachGT <- resultsArray) {
      val stepsResults: Array[Double] = new Array(partitionBits + 1)
      val distribution: Array[Int] = new Array(pow(2, partitionBits).toInt)
      for (oneObject <- eachGT.slice(0, k)) {
        val sub_indexID = partition.getPartition(LSHServer.lshEngine.calculateIndex(allDenseVector(oneObject),
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
    println("Finish calculate partition " + paritionID)
    finalStepsResult.foreach(x => print(x / k * 100.0 + "% "))
    println()
    (finalStepsResult(0), finalStepsResult(1), finalStepsResult(2))
  }


//  def pickTheBestPerformanceHashForPartition(): Unit ={
//
//    val topKArray=Array(10)
//    val runRatio=3
//    val testIterNum=10
//    val (allSparseVector, resultsArray) = loadVectorFile("glove.twitter.27B/glove.twitter.27B.100d.20k.SparseVector.txt",
//      "glove.twitter.27B/glove.twitter.27B.100d.20k.groundtruth", 10)
//    var highest=0.0
//    var secHighest=0.0
//    for(hashId <- 0 until TestSettings.testBaseConf.getInt("mclab.lsh.tableNum")){
//      val highestPercent=new Array[Double](10)
//      val secHighestPercent=new Array[Double](10)
//      val curPartitioner=new LocalitySensitivePartitioner[Int](confForPartitioner,0,partitionBits)
//      (highestPercent(hashId), secHighestPercent(hashId), _) = stepwiseDistribution(curPartitioner,allSparseVector, resultsArray, hashId, 10)
//    }
//    highest=highestPercent.zipWithIndex.max._1
//
//  }

  test("TOP k partition distribution-------->dataset:Glove") {
    //Todo: change different partitioner
    val partition: ArrayBuffer[LocalitySensitivePartitioner[Int]] = new ArrayBuffer[LocalitySensitivePartitioner[Int]](10)
    //      new LocalitySensitivePartitioner[Int](confForPartitioner, 0, partitionBits)
    for (i <- 0 until 50) {
      partition += new LocalitySensitivePartitioner[Int](confForPartitioner, 0, partitionBits)
    }
        val topKArray = Array(10,30,50,70,90)
//    val topKArray = Array(10)
    val tuneRatio = 3
    var flag = false
    val (allDenseVector, resultsArray) = loadVectorFile("partition/glove.twitter.27B.100d.DenseVector",
      "partition/glove.twitter.27B.100d.2000queryGT.top100", 100)
//    val (allDenseVector, resultsArray) = loadVectorFile("glove.twitter.27B/glove.twitter.27B.100d.20k.DenseVector.txt",
//      "glove.twitter.27B/glove.twitter.27B.100d.20k.groundtruth", 10)
    //    var highestHashFamilyID = 0
    var bestPartition:Int=0
    for (k <- topKArray) {
      println("********************k=" + k + "*************************")
      if (!flag) {
        var highestPercent = 0.0
        var secondHighestPercent = 0.0
        for (i <- partition.indices) {
          val (tmp1, tmp2, tmp3) = stepwiseDistribution(partition(i), allDenseVector, resultsArray, 0, k,i)
          if (tmp1 >= highestPercent && tmp1 - highestPercent > secondHighestPercent - tmp2 - tuneRatio / 100.0 * k) {
            highestPercent = tmp1
            secondHighestPercent = tmp2
            bestPartition = i
          }
        }
        println("The best partition is " + bestPartition)
        partition(bestPartition).getLSH().outPutTheHashFunctionsIntoFile()
        flag = true
      } else {
        stepwiseDistribution(partition(bestPartition), allDenseVector, resultsArray, 0, k,bestPartition)
      }
    }
  }

//    for (i <- partition.indices){
//      println("the partition id is " + i)
//      for (k <- topKArray) {
//        println("********************k=" + k + "*************************")
//        //to pick the best hash function wisely
//        if (!flag) {
//          var highestPercent = 0.0
//          var secondHighestPercent = 0.0
//          for (partitionId <- 0 until partition.size) {
//            val (tmp1, tmp2, tmp3) = stepwiseDistribution(partition(parti),allDenseVector, resultsArray, 0, k)
//            if (tmp1 >= highestPercent && tmp1 - highestPercent > secondHighestPercent - tmp2 - tuneRatio / 100.0 * k) {
//              highestPercent = tmp1
//              secondHighestPercent = tmp2
//              bestPartition=partition(i)
////              highestHashFamilyID = hashId
//            }
//          }
//          //        LSHServer.lshEngine.outPutTheHashFunctionsIntoFile(highestHashFamilyID)
//          bestPartition.getLSH().outPutTheHashFunctionsIntoFile()
//          flag = true
//        } else {
//          stepwiseDistribution(partition(i),allDenseVector, resultsArray, highestHashFamilyID, k)
//        }
//      }
//    }
//
//  }
}
