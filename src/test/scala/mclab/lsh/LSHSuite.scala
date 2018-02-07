package mclab.lsh

import com.typesafe.config.{Config, ConfigFactory}
import mclab.TestSettings
import mclab.lsh.vector.{SparseVector, Vectors}
import org.scalatest.{BeforeAndAfterAll, FunSuite}


class LSHSuite extends FunSuite with BeforeAndAfterAll {

  var config: Config = _

  override def beforeAll() {
    config = ConfigFactory.parseString(
      s"""
         |mclab.lsh.familySize = 10
         |mclab.lsh.tableNum = 10
         |mclab.lsh.vectorDim = 3
         |mclab.lshTable.chainLength = 2
         |mclab.lsh.generateMethod=default
      """.stripMargin).withFallback(TestSettings.testBaseConf)
  }

  test("LSH initialize Hash Family and Hash Chain correctly") {
    val lsh1 = new LSH(config)
    val testVector = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
    val keyArray1= lsh1.calculateIndex(testVector)

    assert(keyArray1.length === 10)
    println("lsh1 index hash values:")
    keyArray1.map(x=>println(x.toBinaryString))

//    val lsh2=new LSH(config,2)
//    val keyArray2 =lsh2.calculateIndex(testVector)
//    println("lsh2 index hash values")
//    keyArray2.map(x=>println(x.toBinaryString))




//    println("after sampling:")
//    val bitsArray=Sampling.samplingKeyArray(keyArray)
//    for(i <- bitsArray.indices){ println(bitsArray(i))}


//    val testP=lsh.calculateIndex(testVector,1)(0) >>>30
//    println(testP)
    for (i <- lsh1.tableIndexGenerators.indices){
      for( functions <- lsh1.tableIndexGenerators(i).chainedHashFunctions){
        println(functions.toString)
      }
      println("---")
    }

//    lsh1.outPutTheHashFunctionsIntoFile()


  }
}
