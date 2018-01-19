package mclab.lsh.hashFamilies

import breeze.linalg.DenseVector

object significantBits {
  /**
    * count the continue ones for last 28 bits in Int. Then reconstruct the Int.
    * @param key
    * @return newKey
    */
  def continueBitsCount(key:Int,numOfBits:Array[Int]):Int={
//    println("the hash value is " + key.toBinaryString)
    val firstFourBits= key >>> 28
    //save
    val bits = scala.collection.mutable.BitSet.empty
    // 0,0,0,0 coresponding to 7,5,3,1 times of continue bits
    val newIndexArray:Array[Int]=Array(0,0,0,0)

    val mask = 1
    var count=0
    for (i <- 0 until 28){
      if (((key >>> i) & mask) == 1){
        count += 1
        if (i == 27){
          if (count >= numOfBits(0)){
            newIndexArray(0) += 1
            newIndexArray(1) += 1
            newIndexArray(2) += 1
            newIndexArray(3) += 1
          }else if(count >= numOfBits(1)){
            newIndexArray(1) += 1
            newIndexArray(2) += 1
            newIndexArray(3) += 1
          }else if(count >= numOfBits(2)){
            newIndexArray(2) += 1
            newIndexArray(3) += 1
          }else if(count >= numOfBits(3)){
            newIndexArray(3) += 1
          }
          count = 0
        }
      }else if (((key >>> i) & mask) == 0){
        if (count >= numOfBits(0)){
          newIndexArray(0) += 1
          newIndexArray(1) += 1
          newIndexArray(2) += 1
          newIndexArray(3) += 1
        }else if(count >= numOfBits(1)){
          newIndexArray(1) += 1
          newIndexArray(2) += 1
          newIndexArray(3) += 1
        }else if(count >= numOfBits(2)){
          newIndexArray(2) += 1
          newIndexArray(3) += 1
        }else if(count >= numOfBits(3)){
          newIndexArray(3) += 1
        }
        count = 0
      }
    }
    var tmp:Int=0
    for (i <- newIndexArray.reverse.indices){
      tmp += newIndexArray.reverse(i) << ((3 - i)*7)
    }
    tmp += firstFourBits << 28
    tmp
  }

  /**
    * count the changes of 0 to 1 in keys
    * @param key
    * @return new key
    */
  def countTheChanges(key:Int):Int={
    1
  }

  def combineValue(key:Int):Int={
    val firstFourBits= key >>> 28
    val base=Array(1,1,1,1,1,1,1)
    val combineVector=Array(0,0,0,0,0,0,0)
    var mask=0x7f
    var mask1=1
    for(i <- 0 until 4){
      var tmp=(key&(mask << i*7)) >>> i*7
      for(j<- 0 until 7){
        combineVector(6-j) += (tmp >>> j)&mask1
      }
    }
    for(i<- combineVector.indices) println(combineVector(i))
    val v1=DenseVector(combineVector)
    val baseVector=DenseVector(base)
    val Lx=math.sqrt(baseVector.dot(baseVector))
    val Ly=math.sqrt(v1.dot(v1))
    val cos_angle=v1.dot(baseVector)/(Lx*Ly)
    val angle=math.acos(cos_angle)
    println(angle*360/2/math.Pi)
    1
  }
  def angleDistance(key:Int):Double={
    val keyVector=new Array[Int](28)
    val base=new Array[Int](28)
    for(i<- 0 until 28){
      keyVector(27-i) = (key >>> i)&1
      base(i)=1
    }
    val v1=DenseVector(keyVector)
    val baseVector=DenseVector(base)
    val angle=math.acos(v1.dot(baseVector)/(math.sqrt(baseVector.dot(baseVector))*math.sqrt(v1.dot(v1))))
//    print(angle*360/2/math.Pi)
    angle*360/2/math.Pi
  }
  def newMethod(key:Int):Int={
    val measureMetric:Array[Double]=Array(16.0,25.0,33.0,39.0,46.0,52.0,58.0,66.0,72.0)
    var index=0
    while(index < 9&&(angleDistance(key)>measureMetric(index))){
      index += 1
    }
    val mask=0x7f
    val firstLabel=index // first 7 bits
    val first4bits=(key >>> 28)&mask
    val first7Bits=(key >>> 21)&mask
    val two7Bits=(key >>> 14)&mask
    val three7Bits=(key >>> 7)&mask
    val last7Bits=key&mask
    last7Bits + (three7Bits << 7) + (firstLabel << 14) + (first7Bits << 21) + (first4bits << 28)
  }
  //different bits in each layer
  def variableBits(key:Int):Int={
    val mask7=0x7f
    val mask4=0xf
    val first4Bits=(key >>> 28)& mask7
    val first7Bits=(key >>> 24)& mask4
    val second7Bits= (key >>> 17) & mask7
    val three7Bits=(key >>> 10) & mask7
    val last7Bits = (key >>> 3) & mask7
    last7Bits + (three7Bits << 7) + (second7Bits << 14) + (first7Bits << 21) + (first4Bits << 28)
  }


}
