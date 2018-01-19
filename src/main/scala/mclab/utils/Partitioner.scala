package mclab.utils

import com.typesafe.config.Config
import mclab.lsh.LSH
import mclab.lsh.vector.SparseVector
import mclab.mapdb.Partitioner

/**
  * Inherit the Partitioner<A> and implement the method getPartition, the default one is just % numPartitions
  * @param numPartitions
  * @tparam K
  */
class HashPartitioner[K](numPartitions: Int) extends Partitioner[K](numPartitions) {
  override def getPartition(value: K): Int = {
    value.hashCode() % numPartitions
  }
}

/**
  * Locality sensitive hashing partitioner
  * @param conf application configuration
  * @param tableId
  * @param partitionBits the number of bits which used to define the sub-index-ID, here numPartitions=1 << partitionBits
  * @tparam K
  */
class LocalitySensitivePartitioner[K](conf: Config, tableId: Int, partitionBits: Int)
  extends Partitioner[K](1 << partitionBits) {

  //LSH class for partitioner
  val localitySensitiveHashing = new LSH(conf)

  println("===initialized Locality Sensitive Partitioner =====")

  override def getPartition(hashCode: K): Int = {
    val hashValueInInteger = hashCode.asInstanceOf[Int].hashCode()
    //val partitionId = objHashValue >>> (32 - numBits)
    //partitionId
    //build vector
    val vector = new Array[Int](32)
    for (i <- 0 until 32) {
      //put the ith bit into Array(i)
      vector(i) = (hashValueInInteger & (1 << i)) >>> i
    }
    val index = vector.zipWithIndex.filter(_._1 != 0).map(_._2)
    val values = vector.filter(_ != 0).map(_.toDouble)
    val v = new SparseVector(0, 32, index, values)
    //re locality-sensitive hashing:generate the partitions, for example,
    // if it is partitionBits=2, and hash value is 10......,
    //then move 30 bits, it will become 10.
    //too solid!
    localitySensitiveHashing.calculateIndex(v, tableId)(0) >>> (32 - partitionBits)
  }
}
