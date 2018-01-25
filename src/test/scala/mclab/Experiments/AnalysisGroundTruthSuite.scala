package mclab.Experiments

import mclab.TestSettings
import mclab.deploy.LSHServer
import mclab.lsh.LSH
import mclab.lsh.vector.{SparseVector, Vectors}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Test the average distance between the query's hash value and the ground truth top K objects' hash values.
  */
class AnalysisGroundTruthSuite extends FunSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    LSHServer.lshEngine = new LSH(TestSettings.testBaseConf)
  }

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
    for (line <- Source.fromFile(getClass.getClassLoader.getResource(filename1).getFile).getLines()) {
      val tmp = Vectors.fromPythonString(line)
      val currentSparseVector = new SparseVector(tmp._1, tmp._2, tmp._3, tmp._4)
      allSparseVector += currentSparseVector
    }
    val resultsArray: ListBuffer[Array[Int]] = new ListBuffer[Array[Int]]
    for (line <- Source.fromFile(getClass.getClassLoader.getResource(filename2).getFile).getLines()) {
      resultsArray += Vectors.analysisKNN(line, k)
    }
    println("Finish load the feature data and ground truth.")
    (allSparseVector, resultsArray)
  }

  /**
    * Get the corresponding hash value of similar objects, the save in the file.
    *
    * @param allSparseVector the feature data
    * @param resultsArray    the ground truth
    * @param k               top k ground truth to be compared, default as 10
    * @param hashFamilyID    the hash family functions to be used, default as 1
    * @return the average similarity degree
    */
  def analysisSimilarDegree(allSparseVector: ListBuffer[SparseVector], resultsArray: ListBuffer[Array[Int]],
                            k: Int = 10, hashFamilyID: Int = 1): Float = {
    val similarityDegree: ListBuffer[Int] =
      for (eachGT <- resultsArray) yield {
        val query = eachGT(0)
        var tmpDegree = 0
        for (o <- eachGT) {
          tmpDegree += Integer.bitCount(LSHServer.lshEngine.calculateIndex(allSparseVector(query), hashFamilyID)(0) ^
            LSHServer.lshEngine.calculateIndex(allSparseVector(o), hashFamilyID)(0))
        }
        tmpDegree / k
      }
    val averageSimilarityDegree = similarityDegree.map(x => x / similarityDegree.size.toFloat).sum
    println("Finish calculate the averageSimilarityDegree of hash Family ID = " + hashFamilyID)
    averageSimilarityDegree
  }


  test("similar degree analysis for different number of tables ----->datatsets:Glove") {
    val hashFamilyNum = 10
    val k = 8
    val (allSparseVector, resultsArray) = loadVectorFile("glove.twitter.27B/glove120k100dReverse.txt",
      "glove.twitter.27B/glove100d120k.txtQueryAndTop10NNResult1200", k)
    val totalAverageSimilarityDegree = (0 until hashFamilyNum).map(tableID => analysisSimilarDegree(allSparseVector,
      resultsArray, k, tableID) / hashFamilyNum.toFloat).sum
    println("The total average similarity degree of " + hashFamilyNum + " different hash family functions(k = " + k + ") is "
      + totalAverageSimilarityDegree)
  }

  test("similar degree analysis for different number of tables ------>dataset:CC_WEB_VIDEO") {
    //have to keep the vectorDim as same as the dimension of feature data, if slow, increase the heap size
    LSHServer.lshEngine=new LSH(ConfigFactory.parseString
      (s"""
         |mclab.lsh.IsOrthogonal = false
         |mclab.lsh.vectorDim = 2048
       """.stripMargin).withFallback(TestSettings.testBaseConf)
    )
    val hashFamilyNum = 10
    val k = 5
    val (allSparseVector, resultsArray) = loadVectorFile("CC_WEB_VIDEO/allFileSortedNomalizedAllSparseVectors_HSV_blue",
      "CC_WEB_VIDEO/new_Test_ES.rst", k)
    val totalAverageSimilarityDegree = (0 until hashFamilyNum).map(tableID => analysisSimilarDegree(allSparseVector,
      resultsArray, k, tableID) / hashFamilyNum.toFloat).sum
    println("The total average similarity degree of " + hashFamilyNum + " different hash family functions(k = " + k + ") is "
      + totalAverageSimilarityDegree)

  }


}
