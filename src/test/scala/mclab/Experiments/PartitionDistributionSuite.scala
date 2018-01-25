package mclab.Experiments

import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.math._

/**
  * Created by eternity on 10/27/17.
  */
class PartitionDistributionSuite extends FunSuite with BeforeAndAfterAll{
  override def beforeAll(): Unit = {
    LSHServer.lshEngine = new LSH(TestSettings.testBaseConf)
  }
  val partitionBits=4

  val confForPartitioner = ConfigFactory.parseString(
    s"""
       |cpslab.lsh.vectorDim=32
       |cpslab.lsh.chainLength=$partitionBits
      """.stripMargin).withFallback(TestSettings.testBaseConf)

  val partition=new LocalitySensitivePartitioner[Int](confForPartitioner, 1, partitionBits)

  def loadVectorFile(filename1:String,filename2:String,k:Int=10):(ListBuffer[SparseVector],ListBuffer[Array[Int]])={
    val allSparseVectorFile=getClass.getClassLoader.getResource(filename1).getFile
    val allSparseVector=new ListBuffer[SparseVector]
    var i=0
    for(line <- Source.fromFile(allSparseVectorFile).getLines()){
      val tmp=Vectors.fromPythonString(line)
      val currentSparseVector=new SparseVector(tmp._1,tmp._2,tmp._3,tmp._4)
      //      println(currentSparseVector.toString)
      allSparseVector += currentSparseVector
      i += 1
      if(i%10000==0){
        println(i + " objects loaded")
      }

    }
    val resultsArray:ListBuffer[Array[Int]]=new ListBuffer[Array[Int]]
    for(line <- Source.fromFile(getClass.getClassLoader.getResource(filename2).getFile).getLines()){
      resultsArray += Vectors.analysisKNN(line,k)
    }
    println("finish load")
    (allSparseVector,resultsArray)
  }
  def partitionDistribution(allSparseVector:ListBuffer[SparseVector],
                            resultsArray:ListBuffer[Array[Int]],hashFamilyID:Int=1,k:Int=10):Unit={
    val totalResults=Array(0.0,0.0,0.0)
    for (eachGT <- resultsArray) {
      val distibution=Array(0,0,0,0)
      for (oneObject <- eachGT.slice(0,k)) {
        val sub_indexID = partition.getPartition(LSHServer.lshEngine.calculateIndex(allSparseVector(oneObject),
          hashFamilyID)(0))
        distibution(sub_indexID) += 1
      }
      distibution.map(x => print(x + " "))
      println()
      scala.util.Sorting.quickSort(distibution)
      for(i<-distibution.indices){totalResults(i) += distibution(i)/3000.0}
//      distibution.map(x => print(x + " "))

    }
    println("finish calculate hashid=" + hashFamilyID)
    totalResults.map(x => print(x/k*100.0 + "% "))
    println()
    //    for(x <- similarityDegree) println(x)
    //    println(averageSimilarityDegree)

  }

  def stepwiseDistribution(allSparseVector:ListBuffer[SparseVector],
                            resultsArray:ListBuffer[Array[Int]],hashFamilyID:Int=1,k:Int=10):(Double,Double,Double)={
    //highest, step one, step two. step 3
    val finalStepsResult=Array(0.0,0.0,0.0,0.0,0.0)
    for (eachGT <- resultsArray) {
      val stepsResults=Array(0.0,0.0,0.0,0.0,0.0)
      var distibution=Array(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0)
      for (oneObject <- eachGT.slice(0,k)) {
        val sub_indexID = partition.getPartition(LSHServer.lshEngine.calculateIndex(allSparseVector(oneObject),
          hashFamilyID)(0))
        distibution(sub_indexID) += 1
      }
//      distibution.map(x => print(x + " "))
//      println()

      val sortedDistribution=distibution.zipWithIndex.sorted

      val highest=sortedDistribution(pow(2,partitionBits).toInt - 1)._2
      stepsResults(0) += sortedDistribution(pow(2,partitionBits).toInt - 1)._1

      for(i<-sortedDistribution.indices){
        if(Integer.bitCount(sortedDistribution(i)._2^highest)==1){
          stepsResults(1) += sortedDistribution(i)._1
        }else if(Integer.bitCount(sortedDistribution(i)._2^highest)==2){
          stepsResults(2) += sortedDistribution(i)._1
        }else if(Integer.bitCount(sortedDistribution(i)._2^highest)==3){
          stepsResults(3) += sortedDistribution(i)._1
        }else if(Integer.bitCount(sortedDistribution(i)._2^highest)==4){
          stepsResults(4) += sortedDistribution(i)._1
        }
      }
//      stepsResults.map(x => print(x + " "))
//      println()
//      scala.util.Sorting.quickSort(distibution)

      for(i<-stepsResults.indices){finalStepsResult(i) += stepsResults(i)/3000.0}
      //      distibution.map(x => print(x + " "))

    }
    println("finish calculate hashid=" + hashFamilyID)
    finalStepsResult.map(x => print(x/k*100.0 + "% "))
    println()
    (finalStepsResult(0) , finalStepsResult(1), finalStepsResult(2))
//    finalStepsResult(0)
    //    for(x <- similarityDegree) println(x)
    //    println(averageSimilarityDegree)

  }


  test("TOP k partition distribution-------->dataset:Glove"){
    val kArray=Array(10,30,50,70,90)
//    val k=20
//    println("k=" + k)
    var flag=false
    val (allSparseVector,resultsArray)=loadVectorFile("partition/glove.100d.Sparsevector.txt",
      "partition/partition_100d_top100",100)
    var highestID=0
    for(k <- kArray){
      println("********************k=" + k + "*************************")
      if(flag==false){
        var highestPercent=0.0
        var highestPercent1=0.0
        for(id<- 0 until 100){
          val (tmp1,tmp2,tmp3)=stepwiseDistribution(allSparseVector,resultsArray,id,k)
          if(tmp1 >= highestPercent && tmp1 - highestPercent > highestPercent1 - tmp2 - 3/100.0*k) {
//          if(tmp1 > highestPercent){
            highestPercent = tmp1
            highestPercent1 = tmp2
            highestID = id
          }
        }
        flag=true
      }else{
        stepwiseDistribution(allSparseVector,resultsArray,highestID,k)
      }
    }
  }
//  test("TOP k partition distribution-------->dataset:Glove------print"){
//    val kArray=Array(10)
//    //    val k=20
//    //    println("k=" + k)
//    val (allSparseVector,resultsArray)=loadVectorFile("glove.twitter.27B/glove120k100dReverse.txt",
//      "glove.twitter.27B/glove100d120k.txtQueryAndTop10NNResult1200",10)
//    for(k <- kArray){
//      println("********************k=" + k + "*************************")
//      for(id<- 0 until 20){
//        stepwiseDistribution(allSparseVector,resultsArray,id,k)
//      }
//    }
//  }


//  test("TOP k partition distribution-------->dataset:CC_WEB"){
//
//    val (allSparseVector,resultsArray)=loadVectorFile("video256/Vector256dForCategory_blue_1",
//      "CC_WEB_VIDEO/new_Test_ES.rst",10)
//    for(id<- 1 until 10){
//      partitionDistribution(allSparseVector,resultsArray,id)
//    }
//  }
}
