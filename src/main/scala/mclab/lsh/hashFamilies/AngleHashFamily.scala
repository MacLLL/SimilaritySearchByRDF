package mclab.lsh.hashFamilies

import breeze.linalg._
import mclab.lsh.vector.{SimilarityCalculator, SparseVector, Vectors}

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random
import mclab.lsh.LSH

// generate the AngleParameterSet by SparseVector
private[lsh] case class AngleParameterSet(oneFunction: SparseVector) extends LSHFunctionParameterSet {

  override def toString: String = oneFunction.toString
}

/**
  *
  * @param familySize
  * @param vectorDim
  * @param chainLength
  * @param permutationNum
  */
private[lsh] class AngleHashFamily(
    familySize: Int,
    vectorDim: Int,
    chainLength: Int,
    permutationNum: Int) extends LSHHashFamily[AngleParameterSet] {

  /**
    * Generate a unit vector as SparseVector form, which is normal angle functions
    *
    * @return normal angle functions
    */
  //generate a unit vector and return as SparseVector form.
  private def getNewUnitVector: SparseVector = {
    val values = {
      //generate the vectorDim numbers of 0 to 1 Double
      val arr = (for (vectorDim <- 0 until vectorDim) yield Random.nextDouble()).toArray
      //uniformly make the value from -1 to 1.
      arr.map(value => if (Random.nextInt(2) > 0) value else -1 * value)
    }
    //return the indices that value !=0
    val indices = values.zipWithIndex.filter { case (value, index) => value != 0 }.map(_._2)
    //normailization:|a| = sqrt((ax * ax) + (ay * ay) + (az * az))
    // and x = ax/|a| y = ay/|a| z=az/|a|
    val sqrSum = math.sqrt(values.foldLeft(0.0) { case (currentSum, newNum) => currentSum + newNum * newNum })
    //after normailization, it become unitvector
    new SparseVector(Vectors.nextVectorID, indices.length, indices, values.map(_ / sqrSum))
  }

  /**
    * Randomly generate a group of hash functions with certain family size
    *
    * @return array of hash functions Family
    */
  private[lsh] def initHashFamily: Array[AngleParameterSet] = {
    val parameters = new ListBuffer[AngleParameterSet]
    for (i <- 0 until familySize) {
      parameters += AngleParameterSet(getNewUnitVector)
    }
    parameters.toArray
  }

  /**
    * make sure the unit vectors are orthogonal with each other
    * which not only improves the performance, but also generates the hash family faster
    * Condition: FamilySize need to be larger than vectorDim
    *
    * @return
    */
  private def initOrthogonalUnitVectorHashFamily: Array[AngleParameterSet] = {
    val parameters = new ListBuffer[AngleParameterSet]
    //generate a matrix
    val originalMatrix = DenseMatrix.rand[Double](familySize, vectorDim)
    //do QR factorization
    val orthogonalMatrix = qr.justQ(originalMatrix)
    for (i <- 0 until familySize) {
      val values = (for (j <- 0 until vectorDim) yield orthogonalMatrix.valueAt(i, j)).toArray
      val indices = values.zipWithIndex.filter { case (value, index) => value != 0 }.map(_._2)
      parameters += AngleParameterSet(new SparseVector(Vectors.nextVectorID, indices.length, indices, values))
    }
    parameters.toArray
  }

  //  /**
  //   * get a set of parameters of the lsh function; essentially the user calls this method to get a
  //   * hash function from the family
  //   * @return the list of LSHTableHashChain
  //   */
  /*
  override def pick(tableNum: Int): List[LSHTableHashChain[AngleParameterSet]] = {
    //can be changed
//    val hashFamily=initHashFamily
    val hashFamily = initOrthogonalUnitVectorHashFamily
//    println("hash family size: " + hashFamily.length)
    val generatedHashChains = new Array[LSHTableHashChain[AngleParameterSet]](tableNum)
    for (tableId <- 0 until tableNum) {
      val hashFunctionChain = (0 until chainLength).map(_ =>
        if (LSH.generateByPulling) {
//          println("picking from family")
          val nextID = Random.nextInt(familySize)
//          println(s"$nextID")
          hashFamily(nextID)
        } else {
          AngleParameterSet(getNewUnitVector)
        }).toList
      generatedHashChains(tableId) = new AngleHashChain(chainLength, hashFunctionChain)
    }
    generatedHashChains.toList
  }
  */

  /**
    * get a set of parameters of the lsh function; essentially the user calls this method to get a
    * hash function from the family
    *
    * @return the list of LSHTableHashChain
    */
  override def pick(tableNum: Int): List[LSHTableHashChain[AngleParameterSet]] = {
    val generatedHashChains = new Array[LSHTableHashChain[AngleParameterSet]](tableNum * permutationNum)
//    println(LSH.IsOrthogonal + " "+LSH.generateByPulling)

    val hashFamily = if (LSH.IsOrthogonal) {
      //pick orthogonal hash functions
      initOrthogonalUnitVectorHashFamily
    } else {
      //pick normal angle hash functions
      initHashFamily
    }
    for (tableId <- 0 until tableNum) {
      val hashFunctionChain = (0 until chainLength).map(_ =>
        if (LSH.generateByPulling) {
          val nextID = Random.nextInt(familySize)
          hashFamily(nextID)
        } else {
          //pick the normal angle function one by one, since orthogonal functions have to be generate by bunch
            AngleParameterSet(getNewUnitVector)
        }).toList
      //TODO: there is a better way to do permutation, every time after getting the hash values, we do permutation
      //TODO: directly on hash values, which don't need to generate new hash functions.
      for (permutationID <- 0 until permutationNum) {
        generatedHashChains(permutationNum * tableId + permutationID) = new
            AngleHashChain(chainLength, Random.shuffle(hashFunctionChain))
      }
    }
    generatedHashChains.toList
  }

  /**
    * generate a hash table chain from the file, for select the best performance hash functions
    *
    * @param filePath the path of the file storing the hash chain
    * @param tableNum the number of hash tables*
    * @return the list of LSHTableHashChain
    */
  override def generateTableChainFromFile(filePath: String, tableNum: Int):
  List[LSHTableHashChain[AngleParameterSet]] = {
    val paraSetList = new ListBuffer[AngleParameterSet]
    try {
      for (vectorString <- Source.fromFile(filePath).getLines()) {
        val unitVector = Vectors.fromString(vectorString)
        paraSetList += new AngleParameterSet(
          Vectors.sparse(unitVector._1, unitVector._2, unitVector._3, unitVector._4).
            asInstanceOf[SparseVector])
      }
      //grouped by chainLength
      val groupedParaSets = paraSetList.grouped(chainLength)
      groupedParaSets.map(paraSet => new AngleHashChain(chainLength, paraSet.toList)).toList
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }
  }
}

private[lsh] class AngleHashChain(chainSize: Int, chainedFunctions: List[AngleParameterSet])
  extends LSHTableHashChain[AngleParameterSet](chainSize, chainedFunctions) {

  //for the convenience to save the hash value as primitive type
  private def sign(input: Double): Int = if (input <= 0) 0 else 1

  /**
    * calculate the index of the vector in the hash table corresponding to the set of functions
    * defined in this class
    *
    * @param vector the vector to be indexed
    * @return the index of the vector
    */
  override def compute(vector: SparseVector): Int = {
    var result = 0
    for (hashFunctionId <- 0 until chainSize) {
      //calculate the inner product of two sparse vectors, then sign to get 1 or 0
      val signResult = sign(
        SimilarityCalculator.fastCalculateSimilarity(chainedFunctions(hashFunctionId).oneFunction,
          vector))
      //put the result in an int variable, from right to left
      result = result << 1 | signResult
      //or from left to right
      //result = signResult << (chainSize - hashFunctionId - 1) | result
    }
    // move the 1 to left ( 32 - chainSize) bits, since we design the index structure generate from left to right
    result << (32 - chainSize)

  }
}







