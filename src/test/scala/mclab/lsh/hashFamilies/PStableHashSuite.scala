package mclab.lsh.hashFamilies

import java.nio.ByteBuffer

import mclab.lsh.vector.{SparseVector, Vectors}
import org.scalatest.FunSuite
import mclab.storage.ByteArrayWrapper

import scala.util.Random


class PStableHashSuite extends FunSuite {
  
  test("PStableHashChain calculates the index correctly for single hash function") {
    val randomVector = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
//    println(randomVector)
    val hashParameterSet = new PStableParameterSet(randomVector, 3, 10)
//    println(hashParameterSet.toString)
    val hashChain = new PStableHashChain(1, List(hashParameterSet))
    val testVector = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
// ByteArrayWrapper(ByteBuffer.allocate(4).putInt(4).array()).hashCode ==923521
    assert(hashChain.compute(testVector) === 923521)
  }

  test("PStableHashChain calculates the index correctly for multiple hash functions") {
    val randomVector1 = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
    val randomVector2 = Vectors.sparse(3, Seq((0, 2.0), (1, 2.0), (2, 2.0))).
      asInstanceOf[SparseVector]
    val hashParameterSet1 = new PStableParameterSet(randomVector1, 3, 4)
    val hashParameterSet2 = new PStableParameterSet(randomVector2, 3, 4)
    val hashChain = new PStableHashChain(2, List(hashParameterSet1, hashParameterSet2))
    val testVector = Vectors.sparse(3, Seq((0, 1.0), (1, 1.0), (2, 1.0))).
      asInstanceOf[SparseVector]
    val indexBytes = hashChain.compute(testVector)
    //ByteArrayWrapper(ByteBuffer.allocate(4).putInt(1).array()
    // ++ ByteBuffer.allocate(4).putInt(2).array()).hashCode == -1806530940
    assert(indexBytes === -1806530940)
  }
  
  test("Hash Family generates PStableHashChain correctly") {
    val hashFamily = new PStableHashFamily(familySize = 100, vectorDim = 2048, pStableMu = 0,
      pStableSigma = 0.5, w = 3, chainLength = 2)
    val hashTableNum = Random.nextInt(100)
    val generatedHashChain = hashFamily.pick(hashTableNum)
    assert(generatedHashChain.size === hashTableNum)
    for (hashChain <- generatedHashChain) {
      assert(hashChain.chainLength === 2)
    }
    for ( singlePStableHashParameter <- generatedHashChain){
      for ( hashFunctions<- singlePStableHashParameter.chainedHashFunctions) {
        println(hashFunctions.toString)
      }
      println("------")
    }
  }

  test("pStableParameterSet generates string correctly") {
    val vectorA = Vectors.sparse(3, Array(0, 1), Array(1.0, 2.0)).asInstanceOf[SparseVector]
    val paraSet = new PStableParameterSet(vectorA, 0.1, 5)
    assert(paraSet.toString === s"$vectorA;0.1;5")
  }
  
  test("pStable HashFamily generates pStableParameterSet from file correctly") {
    val hashFamily = new PStableHashFamily(familySize = 0, vectorDim = 3, pStableMu = 0,
      pStableSigma = 0.5, w = 0, chainLength = 1)
    val hashChain = hashFamily.generateTableChainFromFile(
      getClass.getClassLoader.getResource("hashFamily/pStableHashFile").getFile, 3)
    assert(hashChain.size === 3)
    val firstChain = hashChain(0)
    assert(firstChain.chainedHashFunctions.size === 1)
    for (para <- firstChain.chainedHashFunctions) {
      assert(para.a.toString === "(1,3,[0,1],[1.0,2.0])")
      assert(para.b === 0.1)
      assert(para.w === 5)
    }
    val secondChain = hashChain(1)
    assert(secondChain.chainedHashFunctions.size === 1)
    for (para <- secondChain.chainedHashFunctions) {
      assert(para.a.toString === "(2,3,[0,1],[1.0,3.0])")
      assert(para.b === 0.2)
      assert(para.w === 6)
    }
    val thirdChain = hashChain(2)
    assert(thirdChain.chainedHashFunctions.size === 1)
    for (para <- thirdChain.chainedHashFunctions) {
      assert(para.a.toString === "(3,3,[0,1],[1.0,4.0])")
      assert(para.b === 0.3)
      assert(para.w === 7)
    }
      
  }
}
