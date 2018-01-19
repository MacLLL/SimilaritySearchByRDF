package mclab.lsh.hashFamilies

import scala.collection.mutable.ListBuffer
import scala.util._

class Sampling(seed:Int) {
  private val orderedIndex=new ListBuffer[Int]
  private val random=new Random(seed)
  for (i <- 0 until 32){ orderedIndex += i}

  val samplingIndex=random.shuffle(orderedIndex)

  def samplingKeyArray(keyArray: Array[Int]): Array[Int] = {
    val bitsArray = new ListBuffer[Int]
    for (i <- keyArray.indices) {
      //      var tmp=new ListBuffer[Int]
      var tmp: Int = 0
      for (j <- 0 until 32) {
        //for each index, get bit in index, and add together by moving bits
        tmp += ((keyArray(i) >>> this.samplingIndex(j)) & 1) << (31 - j)
      }
      bitsArray += tmp
    }
    bitsArray.toArray
  }

  /**
    * sampling the key to avoid the innner dulplicate part.
    * @param key: 32 bits int
    * @return sampled 32 bits int.
    */
  def samplingOneKey(key:Int):Int={
    var tmp:Int=0
    for (j <- 0 until 32) {
      //for each index, get bit in index, and add together by moving bits
      tmp += ((key >>> this.samplingIndex(j)) & 1) << (31 - j)
    }
    tmp
  }

}
