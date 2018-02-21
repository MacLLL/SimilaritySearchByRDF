package mclab.Experiments

import mclab.TestSettings
import mclab.deploy.LSHServer
import mclab.lsh.LSH
import mclab.lsh.vector.{DenseVector, SparseVector, Vectors}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import com.typesafe.config.ConfigFactory

/**
  * Test the average distance between the query's hash value and the ground truth top K objects' hash values.
  */
class AnalysisGroundTruthSuite extends FunSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    LSHServer.lshEngine = new LSH(TestSettings.testBaseConf)
    LSHServer.isUseDense=true
  }

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
    var i=0
    val allDenseVector = new ListBuffer[DenseVector]
    for (line <- Source.fromFile(getClass.getClassLoader.getResource(filename1).getFile).getLines()) {
      val tmp = Vectors.parseDense(line)
      val currentDenseVector = new DenseVector(tmp._1,tmp._2)
      allDenseVector += currentDenseVector
      i = i+1
      if(i%100000==0){
        println(i)
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
    * Get the corresponding hash value of similar objects, the save in the file.
    *
    * @param allDenseVector the feature data
    * @param resultsArray    the ground truth
    * @param k               top k ground truth to be compared, default as 10
    * @param hashFamilyID    the hash family functions to be used, default as 1
    * @return the average similarity degree
    */
  def analysisSimilarDegree(allDenseVector: ListBuffer[DenseVector], resultsArray: ListBuffer[Array[Int]],
                            k: Int = 10, hashFamilyID: Int = 1): Float = {
    val similarityDegree: ListBuffer[Int] =
      for (eachGT <- resultsArray) yield {
        val query = eachGT(0)
        var tmpDegree = 0
        for (o <- eachGT) {
          tmpDegree += Integer.bitCount(LSHServer.lshEngine.calculateIndex(allDenseVector(query), hashFamilyID)(0) ^
            LSHServer.lshEngine.calculateIndex(allDenseVector(o), hashFamilyID)(0))
        }
        tmpDegree / k
      }
    val averageSimilarityDegree = similarityDegree.map(x => x / similarityDegree.size.toFloat).sum
    println("Finish calculate the averageSimilarityDegree of hash Family ID = " + hashFamilyID + "------" +averageSimilarityDegree)
    averageSimilarityDegree
  }


  test("similar degree analysis for different number of tables") {
    val hashFamilyNum = 10
    val maxk=11
//    val (allDenseVector, resultsArray) = loadVectorFile("glove.twitter.27B/glove.twitter.27B.100d.20k.DenseVector.txt",
//      "glove.twitter.27B/glove.twitter.27B.100d.20k.groundtruth", 10)
//    val (allDenseVector, resultsArray) = loadVectorFile("partition/glove.twitter.27B.100d.DenseVector",
//      "partition/glove.twitter.27B.100d.2000queryGT.top100", 100)
//    val (allDenseVector, resultsArray) = loadVectorFile("ns/nytimes-256d-DenseVector.txt",
//        "ns/nytimes-256-top100-2000queryGT", 100)
//    val (allDenseVector, resultsArray) = loadVectorFile("ns/fashion-mnist-784d-DenseVector.txt",
//      "ns/fashion-mnist-784d-top100-2000queryGT", 100)
    val (allDenseVector, resultsArray) = loadVectorFile("CC_WEB_VIDEO/ccweb-256d-DenseVector-HSV_red.txt",
      "CC_WEB_VIDEO/new_Test_ES.rst", 10)
    for(k<- (2 until maxk).reverse){
      for(i <- resultsArray.indices){
        resultsArray(i) = resultsArray(i).slice(0,k)
      }
      val totalAverageSimilarityDegree = (0 until hashFamilyNum).map(tableID => analysisSimilarDegree(allDenseVector,
        resultsArray, k, tableID) / hashFamilyNum.toFloat).sum
      println("The total average similarity degree of " + hashFamilyNum + " different hash family functions(k = " + k + ") is "
        + totalAverageSimilarityDegree)
    }
  }


//  test("similar degree analysis for different number of tables ------>dataset:CC_WEB_VIDEO") {
//    //have to keep the vectorDim as same as the dimension of feature data, if slow, increase the heap size
//    LSHServer.lshEngine=new LSH(ConfigFactory.parseString
//      (s"""
//         |mclab.lsh.IsOrthogonal = false
//         |mclab.lsh.vectorDim = 2048
//       """.stripMargin).withFallback(TestSettings.testBaseConf)
//    )
//    val hashFamilyNum = 10
//    val k = 5
//    val (allSparseVector, resultsArray) = loadVectorFile("CC_WEB_VIDEO/allFileSortedNomalizedAllSparseVectors_HSV_blue",
//      "CC_WEB_VIDEO/new_Test_ES.rst", k)
//    val totalAverageSimilarityDegree = (0 until hashFamilyNum).map(tableID => analysisSimilarDegree(allSparseVector,
//      resultsArray, k, tableID) / hashFamilyNum.toFloat).sum
//    println("The total average similarity degree of " + hashFamilyNum + " different hash family functions(k = " + k + ") is "
//      + totalAverageSimilarityDegree)
//
//  }


}
