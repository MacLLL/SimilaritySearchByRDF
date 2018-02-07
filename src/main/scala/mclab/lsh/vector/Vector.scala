/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mclab.lsh.vector

import java.lang.{Double => JavaDouble, Integer => JavaInteger, Iterable => JavaIterable}
import java.util
import java.util.concurrent.atomic.AtomicInteger
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import scala.annotation.varargs
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

// added this file to eliminate the dependency to spark (causing sbt
// assembly extremely slow)

sealed trait Vector extends Serializable {

  /**
   * Size of the vector.
   */
  def size: Int

  /**
   * Converts the instance to a double array.
   */
  def toArray: Array[Double]

  /**
    * Compare two Vectors
    * @param other Compared Vector
    * @return
    */
  override def equals(other: Any): Boolean = {
    other match {
      case v: Vector =>
        util.Arrays.equals(this.toArray, v.toArray)
      case _ => false
    }
  }

  override def hashCode(): Int = util.Arrays.hashCode(this.toArray)

  /**
   * Makes a deep copy of this vector.
   */
  def copy: Vector = {
    throw new NotImplementedError(s"copy is not implemented for ${this.getClass.getName}.")
  }
}

object Vectors {
  //vectorId from 0 increment
  private val vectorId = new AtomicInteger(0)

  def nextVectorID: Int = vectorId.getAndIncrement

  /**
   * Creates a dense vector from its values.
   */
  @varargs
  def dense(firstValue: Double, otherValues: Double*): Vector =
    new DenseVector(nextVectorID, (firstValue +: otherValues).toArray)

  // A dummy implicit is used to avoid signature collision with the one generated by @varargs.
  /**
   * Creates a dense vector from a double array.
   */
  def dense(values: Array[Double]): Vector = new DenseVector(nextVectorID, values)

  /**
   * Creates a dense vector from a double array and its ID
    *
    * @param id the vector id
   * @param values the values in this dense vector
   * @return the newly created vector
   */
  def dense(id: Int, values: Array[Double]): Vector = new DenseVector(id, values)

  /**
   * Creates a sparse vector providing its index array and value array.
   *
   * @param size vector size.
   * @param indices index array, must be strictly increasing.
   * @param values value array, must have the same length as indices.
   */
  def sparse(size: Int, indices: Array[Int], values: Array[Double]): Vector =
    new SparseVector(Vectors.nextVectorID, size, indices, values)

  /**
   * Creates a sparse vector providing its id, index array and value array.
   *
   * @param id vector Id
   * @param size vector size.
   * @param indices index array, must be strictly increasing.
   * @param values value array, must have the same length as indices.
   */
  def sparse(id: Int, size: Int, indices: Array[Int], values: Array[Double]): Vector =
    new SparseVector(id, size, indices, values)

  /**
   * Creates a sparse vector using unordered (index, value) pairs.
   *
   * @param size vector size.
   * @param elements vector elements in (index, value) pairs.
   */
  def sparse(size: Int, elements: Seq[(Int, Double)]): Vector = {
    require(size > 0)
    val (indices, values) = elements.sortBy(_._1).unzip
    var prev = -1
    indices.foreach { i =>
      require(prev < i, s"Found duplicate indices: $i.")
      prev = i
    }
    require(prev < size)

    new SparseVector(Vectors.nextVectorID, size, indices.toArray, values.toArray)
  }

  /**
   * Creates a sparse vector using unordered (index, value) pairs in a Java friendly way.
   *
   * @param size vector size.
   * @param elements vector elements in (index, value) pairs.
   */
  def sparse(size: Int, elements: JavaIterable[(JavaInteger, JavaDouble)]): Vector = {
    sparse(size, elements.asScala.map { case (i, x) =>
      (i.intValue(), x.doubleValue())
    }.toSeq)
  }

  /**
   * Creates a dense vector of all zeros.
   *
   * @param size vector size
   * @return a zero vector
   */
  def zeros(size: Int): Vector = {
    new DenseVector(Vectors.nextVectorID, new Array[Double](size))
  }

  /**
    * parse the String like: "(3,3,[0,1,2],[1.0,2.0,3.0])" into (Int, Int, Array[Int], Array[Double])
    * @param inputString String
    * @return the tuple for generate SparseVector
    */
  private[mclab] def fromString(inputString: String): (Int, Int, Array[Int], Array[Double]) = {
    val stringArray = inputString.split(",\\[")
    if (stringArray.length != 3) {
      throw new Exception(s"cannot parse $inputString")
    }
    val Array(id, size) = stringArray(0).replace("(", "").split(",").map(_.toInt)
    val indicesStringArray = stringArray(1).replace("]", "").split(",").filter(_ != "")
    val indices = if (indicesStringArray.nonEmpty) indicesStringArray.map(_.toInt) else
      new Array[Int](0)
    val valueStringArray = stringArray(2).replace("])", "").split(",").filter(_ != "")
    val values = if (valueStringArray.nonEmpty) valueStringArray.map(_.toDouble) else
      new Array[Double](0)
    (id, size, indices, values)
  }

  /**
    * Parse the String like 1,2,3
    * @param inputString String
    * @return the values array
    */
  private[mclab] def fromStringDense(inputString:String):Array[Double]={
    val stringArray = inputString.split(",")
    val values = if (stringArray.nonEmpty) stringArray.map(_.toDouble) else new Array[Double](0)
    values
  }

  /**
    * Parse the String like [1, 3, [1, 2, 3], [1.0, 2.0, 3.0]] from python
    * Notes: Even though there is a space before the numbers
    * @param inputString String
    * @return the tuple for generate SparseVector
    */
  private[mclab] def fromPythonString(inputString:String):(Int,Int,Array[Int],Array[Double])={
    val newInputString = inputString.replace(" ","")
    val stringArray = newInputString.split(",\\[")
    if (stringArray.length != 3) {
      throw new Exception(s"cannot parse $inputString")
    }
    val Array(id, size) = stringArray(0).replace("[", "").split(",").map(_.toInt)
    val indicesStringArray = stringArray(1).replace("]", "").split(",").filter(_ != "")
    val indices = if (indicesStringArray.nonEmpty) indicesStringArray.map(_.toInt) else
      new Array[Int](0)
    val valueStringArray = stringArray(2).replace("]]", "").split(",").filter(_ != "")
    val values = if (valueStringArray.nonEmpty) valueStringArray.map(_.toDouble) else
      new Array[Double](0)
    (id, size, indices, values)
  }

  /**
    * Parse the String like [1,[0.1,0.2,0.4,0.9]] from python
    * @param inputString
    * @return denseVector ,like (vectorID, values)
    */
  private[mclab] def parseDense(inputString:String):(Int,Array[Double])={
    val newInputString=inputString.replace(" ","").replace("[","").replace("]","")
    val arraysOfData=newInputString.split(",")
    (arraysOfData(0).toInt,arraysOfData.slice(1,arraysOfData.length).map(_.toDouble))
  }


  /**
    * Parse the String like: 1 1_1_Y E into (Int, String)
    * used when read the wholeNewGT.rst
    * @param inputString String
    * @return (index, videoName)
    */
  private[mclab] def wholeNewGTFromPython(inputString:String): (Int, String)={
    val stringArray = inputString.split(" ")
    if (stringArray.length != 3) {
      throw new Exception(s"cannot parse $inputString")
    }
    val index=stringArray(0).toInt
    val videoName=stringArray(1)
    (index, videoName)
  }

  /**
    * Parse the String like: 287#16#7#[2, 11, 12, 14, 15, 21, 26, 28, 40, 48, 51, 88, 101, 124, 127, 155]#[29, 44, 52, 74, 230, 245, 280]
    * used when read the testES
    * @param inputString
    * @return
    */
  private[mclab] def ESfromPython(inputString:String):(Int, Int,Int, Array[Int],Array[Int])={
    val stringArray=inputString.split("#")
    if (stringArray.length!=5){
      throw new Exception(s"cannot parse $inputString")
    }
    val totalNum=stringArray(0).toInt
    val ENum=stringArray(1).toInt
    val SNum=stringArray(2).toInt
    val Epart=stringArray(3).replace(" ","").replace("[","").replace("]","").split(",").map(_.toInt)
    require(Epart.length == ENum,s"$Epart has errors")
    val Spart=stringArray(4).replace(" ","").replace("[","").replace("]","").split(",").map(_.toInt)
    require(Spart.length == SNum,s"$Spart has errors")
    (totalNum, ENum, SNum, Epart, Spart)
  }

  /**
    * Parse top K Nearest Neighbors' distance like [0,0.22,0.3,0.4,...,?]
    * used when evaluate the error ratio
    * @param K the top k you need
    * @param inputString String
    * @return array of distance
    */
  private[mclab] def KNNFromPython(K:Int,inputString:String):Array[Double]={
    val oneKNNDistance=new ListBuffer[Double]
    val stringArrayKNN=inputString.replace(" ","").split(",")
    val length=stringArrayKNN.length
    if(K > length){ throw new Exception(s"cannot parse $inputString")}
    for(i <- 0 until K){
      oneKNNDistance += stringArrayKNN(i).replace("[","").replace("]","").toDouble
    }
    oneKNNDistance.toArray
  }

  /**
    * Parse top K Nearest Neighbors' indices like [1,30,19,230,193293,...,?]
    * for analysis simialrity degree
    * @param inputString one string line from file
    * @param k top k NN
    * @return
    */
  private[mclab] def analysisKNN(inputString:String,k:Int):Array[Int]={
    val oneKNN=new ListBuffer[Int]
    val stringArrayKNN=inputString.replace(" ","").split(",")
    val length=stringArrayKNN.length
    if(k > length){ throw new Exception(s"cannot parse $inputString")}
    for(i <- 0 until k){
      oneKNN += stringArrayKNN(i).replace("[","").replace("]","").toInt
    }
    oneKNN.toArray
  }

  /**
    * Parse the number into vector
    * @param any any type
    * @return the vector
    */
  private[mclab] def parseNumeric(any: Any): Vector = {
    any match {
      case values: Array[Double] =>
        Vectors.dense(values)
      case Seq(size: Double, indices: Array[Double], values: Array[Double]) =>
        Vectors.sparse(size.toInt, indices.map(_.toInt), values)
      case vectorString: String =>
        //for sparseVector
        try{
          val parsedResult = fromString(vectorString)
          Vectors.sparse(parsedResult._1, parsedResult._2, parsedResult._3, parsedResult._4)
        }catch{
          case ex:Exception =>{
            //for denseVector
            val parseResult=fromStringDense(vectorString)
            Vectors.dense(parseResult)
          }
        }
      case other =>
        throw new Exception(s"Cannot parse $other.")
    }
  }

  /**
   * Creates a vector instance from a breeze vector.
   */
  private[mclab] def fromBreeze(breezeVector: BV[Double]): Vector = {
    breezeVector match {
      case v: BDV[Double] =>
        if (v.offset == 0 && v.stride == 1) {
          new DenseVector(Vectors.nextVectorID, v.data)
        } else {
          // Can't use underlying array directly, so make a new one
          new DenseVector(Vectors.nextVectorID, v.toArray)
        }
      case v: BSV[Double] =>
        if (v.index.length == v.used) {
          new SparseVector(Vectors.nextVectorID, v.length, v.index, v.data)
        } else {
          new SparseVector(Vectors.nextVectorID, v.length, v.index.slice(0, v.used),
            v.data.slice(0, v.used))
        }
      case v: BV[_] =>
        sys.error("Unsupported Breeze vector type: " + v.getClass.getName)
    }
  }
}

/**
  * DenseVector definition
  * @param vectorId
  * @param values
  */
class DenseVector(val vectorId: Int, val values: Array[Double]) extends Vector {

  override def size: Int = values.length

  override def toString: String = values.mkString("[", ",", "]")

  override def toArray: Array[Double] = values

  override def copy: DenseVector = {
    new DenseVector(vectorId, values.clone())
  }
}

/**
  * SparseVector definition:
  * @param vectorId the id of vector
  * @param size the size of vector
  * @param indices the index where the value is equal to 0.0
  * @param values the values which aren't equal to 0.0 in vector
  */
//SparseVector definition: Cause the vector is sparse, so we use bitVector to represent the vector
class SparseVector(
    val vectorId: Int,
    override val size: Int,
    val indices: Array[Int],
    val values: Array[Double]) extends Vector {
  //use Tuple initalize the parameters
  def this(paraTuple: (Int, Int, Array[Int], Array[Double])) =
    this(paraTuple._1, paraTuple._2, paraTuple._3, paraTuple._4)

  require(indices.length == values.length,
    s"indices length: ${indices.length}, values length: ${values.length}")

  //for fast compute the similarity, index -> value
  val indexToMap = new mutable.HashMap[Int, Double]()

  for (i <- 0 until indices.length) {
    indexToMap(indices(i)) = values(i)
  }
  //set indices as true,for example, if indices=[0,2,3] bitVector=1011
  //so when we want to find the non-zero value position in vector
  //we can simply use nextSetBit(0) which means next position which value isn't zero
  val bitVector: util.BitSet = {
    val bv = new util.BitSet()
    for (i <- indices) {
      bv.set(i)
    }
    bv
  }
  
  override def toString: String =
    "(%s,%s,%s,%s)".format(vectorId, size, indices.mkString("[", ",", "]"),
      values.mkString("[", ",", "]"))

  override def toArray: Array[Double] = {
    val data = new Array[Double](size)
    for(i <- 0 until indices.length)
      data(indices(i))=values(i)
    data
  }

  override def copy: SparseVector = {
    new SparseVector(vectorId, size, indices.clone(), values.clone())
  }
}
