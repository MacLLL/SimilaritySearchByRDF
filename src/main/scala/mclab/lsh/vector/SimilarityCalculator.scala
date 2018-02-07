package mclab.lsh.vector

import java.util

import scala.collection.mutable

private[mclab] object SimilarityCalculator {

  def fastCalculateSimilarity(vector1: SparseVector, vector2: SparseVector): Double = {
//    println(vector1.toString + vector2.toString)
    require(vector1 != null  && vector2 != null,
      s"${if (vector1 == null) "vector 1" else "vector 2"} is null")
    require(vector1.size == vector2.size, s"vector1 size: ${vector1.size}, " +
      s"vector2 size: ${vector2.size}")
    var similarity = 0.0
    val validBits = vector1.bitVector.clone().asInstanceOf[util.BitSet]
    //use and calculate interaction to find the position which both aren't equal 0
    validBits.and(vector2.bitVector)
    var nextSetBit = validBits.nextSetBit(0)
    while (nextSetBit != -1) {
      //calculate overlap number and add vector1.position.value*vector2.position.value
      similarity += vector1.indexToMap(nextSetBit) * vector2.indexToMap(nextSetBit)
      //check the next bits from the nextSetBit + 1
      nextSetBit = validBits.nextSetBit(nextSetBit + 1)
    }
    similarity
  }

  def fastCalculateSimilarity(vector1: DenseVector, vector2: DenseVector): Double = {
    require(vector1 != null  && vector2 != null,
      s"${if (vector1 == null) "vector 1" else "vector 2"} is null")
    require(vector1.size == vector2.size, s"vector1 size: ${vector1.size}, " +
      s"vector2 size: ${vector2.size}")
    var similarity = 0.0
    val pair=vector1.values.zip(vector2.values)
    pair.foreach(x => similarity = similarity + x._1*x._2)
    similarity
  }

  def fastCalculateSimilarity(vector1: SparseVector, vector2: DenseVector): Double = {
    require(vector1 != null  && vector2 != null,
      s"${if (vector1 == null) "vector 1" else "vector 2"} is null")
    require(vector1.size == vector2.size, s"vector1 size: ${vector1.size}, " +
      s"vector2 size: ${vector2.size}")
    var similarity = 0.0
    val pair=vector1.values.zip(vector2.values)
    pair.foreach(x => similarity = similarity + x._1*x._2)
    similarity
  }


}
