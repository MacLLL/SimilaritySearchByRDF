package mclab.lsh.hashFamilies

import mclab.lsh.vector.{SparseVector, Vectors}
import org.scalatest.FunSuite

import scala.util.Random

class AngleHashSuite extends FunSuite {

  test("AngleHashChain calculates the index correctly for single hash function") {
    val randomVector = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
    val hashParameterSet = new AngleParameterSet(randomVector)
    val hashChain = new AngleHashChain(1, List(hashParameterSet))
    val testVector = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).asInstanceOf[SparseVector]
    //-2147483648 is 10000000 00000000 00000000 00000000, the leftest bit is signal bit: 1 << 31
    assert(hashChain.compute(testVector) === -2147483648)
  }
  //plan use size = 100
  test("AngleHashChain calculates the index correctly for multiple hash function") {
    val randomVector1 = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
    val randomVector2 = Vectors.sparse(3, Seq((0, 1.5), (1, -1.0), (2, 0.0))).
      asInstanceOf[SparseVector]
    val hashParameterSet1 = new AngleParameterSet(randomVector1)
    val hashParameterSet2 = new AngleParameterSet(randomVector2)
    val hashChain = new AngleHashChain(2, List(hashParameterSet1, hashParameterSet2))
    val testVector = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
    val indexBytes = hashChain.compute(testVector)
    //11000000 00000000 00000000 00000000 = 3<<30=-1073741824
    assert(indexBytes === -1073741824)
  }

  test("Hash Family generates AngleHashChain correctly") {
    val hashFamily = new AngleHashFamily(familySize = 100, vectorDim = 100, chainLength = 10, permutationNum = 3)
    val hashTableNum = Random.nextInt(50)
    val generatedHashChain = hashFamily.pick(hashTableNum)
    for ( hashChain <- generatedHashChain ){
      println()
      for( hashFunction <- hashChain.chainedHashFunctions )
        println(hashFunction.oneFunction)
    }

    println("chainLength=" + generatedHashChain(0).chainLength)
    assert(generatedHashChain.size === hashTableNum*3)
    for (hashChain <- generatedHashChain) {
      assert(hashChain.chainLength === 10)
    }
  }

  test("Angle HashFamily generates AngleParameterSet from file correctly") {
    val hashFamily = new AngleHashFamily(familySize = 0, vectorDim = 3, chainLength = 3, 3)
    val hashChainForTables = hashFamily.generateTableChainFromFile(
      getClass.getClassLoader.getResource("hashFamily/angleHashFile").getFile, tableNum = 3)
    assert(hashChainForTables.size === 3)

    val firstChain = hashChainForTables(0)
    assert(firstChain.chainedHashFunctions.size === 3)
    for (para <- firstChain.chainedHashFunctions) {
      assert(para.oneFunction.toString === "(1,3,[0,1],[1.0,2.0])")
    }
    val secondChain = hashChainForTables(1)
    assert(secondChain.chainedHashFunctions.size === 3)
    for (para <- secondChain.chainedHashFunctions) {
      assert(para.oneFunction.toString === "(2,3,[0,1],[1.0,3.0])")
    }
    val thirdChain = hashChainForTables(2)
    assert(thirdChain.chainedHashFunctions.size === 3)
    for (para <- thirdChain.chainedHashFunctions) {
      assert(para.oneFunction.toString === "(3,3,[0,1],[1.0,4.0])")
    }
  }
}
