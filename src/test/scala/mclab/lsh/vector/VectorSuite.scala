package mclab.lsh.vector

import org.scalatest.FunSuite
import scala.collection.mutable.ListBuffer
import scala.io.Source

// take the vector from file
class VectorSuite extends FunSuite {
  test("fromString test") {
    val vectorFile = getClass.getClassLoader.getResource("VectorTest/sparsevectorfile").getFile
    val results = new ListBuffer[(Int, Int, Array[Int], Array[Double])]
    for (line <- Source.fromFile(vectorFile).getLines()) {
      results += Vectors.fromString(line)
    }

    // convert to sparse vector and compare
    val sparseVector1 = new SparseVector(results(0)._1, results(0)._2, results(0)._3, results(0)._4)
    val sparseVector2 = new SparseVector(results(1)._1, results(1)._2, results(1)._3, results(1)._4)
    val sparseVector3 = new SparseVector(results(2)._1, results(2)._2, results(2)._3, results(2)._4)
    println(sparseVector1)
    println(sparseVector2)
    println(sparseVector3)
    assert(sparseVector1.toString === "(3,3,[0,1,2],[1.0,2.0,3.0])")
    assert(sparseVector2.toString === "(4,3,[0,1,2],[4.0,5.0,6.0])")
    assert(sparseVector3.toString === "(5,2,[0,1],[1.0,2.0])")

    //test densevector
    val denseVectorFile = getClass.getClassLoader.getResource("VectorTest/densevectorfile").getFile
    val denseResults = new ListBuffer[Array[Double]]
    for (line <- Source.fromFile(denseVectorFile).getLines()) {
      denseResults += Vectors.fromStringDense(line)
    }
    val denseVectorFromFile = new DenseVector(1, denseResults(0))
    val l = denseResults.length
    println(l)
    println(denseVectorFromFile)
    assert(denseVectorFromFile.toString === "[0.3,0.2,0.9]")

    val denseVector1 = new DenseVector(1, Seq(0.0, 1.0, 2.0).toArray)
    assert(denseVector1.toString === "[0.0,1.0,2.0]")

    //parseTest
    println(Vectors.fromPythonString("[3,3,[0,1,2],[1.0,2.0,3.0]]"))
    println(Vectors.wholeNewGTFromPython("1 1_1_Y E"))
    println(Vectors.ESfromPython("287#16#7#[2, 11, 12, 14, 15, 21, 26, 28, 40, 48, 51, 88, 101, 124, 127, 155]#[29, 44, 52, 74, 230, 245, 280]"))
    println(java.util.Arrays.toString(Vectors.analysisKNN("[1,2,3,4,5,7,8, 8,91,10,12,23]", 12)))
    println(java.util.Arrays.toString(Vectors.KNNFromPython(5, "[0.2,93.2,9.3,0.3,0.53,0.1882,392]")))
    println(Vectors.parseNumeric("(3,3,[0,1,2],[1.0,2.0,3.0])"))
    println(Vectors.parseNumeric("1.0,2.0,3.0"))
    println(Vectors.parseNumeric(Array(1.0, 2.0, 3.0)))
    println(Vectors.parseNumeric(Seq(3.0, Array(1.0, 2.0, 3.0), Array(0.9, 0.3, 0.3))))

  }

}
