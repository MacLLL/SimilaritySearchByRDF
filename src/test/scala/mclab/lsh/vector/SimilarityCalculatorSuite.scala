package mclab.lsh.vector

import org.scalatest.FunSuite

class SimilarityCalculatorSuite extends FunSuite {
  test("fast similarity calculation") {
    val vector1 = Vectors.sparse(10, Seq((0, 1.0), (2, 0.5), (3, 1.2),(9, 2.5))).asInstanceOf[SparseVector]
    val vector2 = Vectors.sparse(10, Seq((2, 0.2), (3, 1.5))).asInstanceOf[SparseVector]

    val sim = SimilarityCalculator.fastCalculateSimilarity(vector1, vector2)
    assert(sim === 1.8)
    val vector3 = Vectors.sparse(10, Seq((0, 1.0), (2, 1.0), (3, 1.0),(9, 1.0))).
      asInstanceOf[SparseVector]
    val vector4 = Vectors.sparse(10, Seq((5, 1.0), (6, 1.0))).asInstanceOf[SparseVector]
    val sim1 = SimilarityCalculator.fastCalculateSimilarity(vector3, vector4)
    assert(sim1 === 0.0)
  }

  test("fast similarity calculation when there is a mis-matched bit in the middle of the vector") {
    val vector3 = Vectors.sparse(10, Seq((0, 1.0), (2, 1.0), (3, 2.0),(9, 1.0))).
      asInstanceOf[SparseVector]
    val vector4 = Vectors.sparse(10, Seq((3, 1.3), (6, 1.0), (9, 1.0))).asInstanceOf[SparseVector]
    val sim1 = SimilarityCalculator.fastCalculateSimilarity(vector3, vector4)
    assert(sim1 === 3.6)
  }
  test("dense vector similarity calculation"){
    val vector0=Vectors.sparse(3,Seq((0, 1.1), (1, 1.2), (2, 1.3))).asInstanceOf[SparseVector]
    val vector1=Vectors.dense(Array(0.1,0.2,0.3)).asInstanceOf[DenseVector]
    val vector2=Vectors.dense(Array(0.2,0.3,0.4)).asInstanceOf[DenseVector]
    val sim=SimilarityCalculator.fastCalculateSimilarity(vector1,vector2)
    assert(sim === 0.2)
    val sim1=SimilarityCalculator.fastCalculateSimilarity(vector0,vector1)
    assert(sim1 === 0.74)
  }

}
