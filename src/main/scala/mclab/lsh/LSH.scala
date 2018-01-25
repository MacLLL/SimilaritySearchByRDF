package mclab.lsh

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import hashFamilies._
import hashFamilies.Sampling
import mclab.lsh.vector.SparseVector
import com.typesafe.config.{Config, ConfigFactory}

/**
 * Implementation class of LSH
 * This class wraps one or more LSHTableHashChains
 * By passing different lshFamilyName, we can implement different lsh schema
 */
private[mclab] class LSH(conf: Config,seedNumber:Int=1) extends Serializable {
  //indicate what type of hash functions to use
  private val lshFamilyName: String = conf.getString("mclab.lsh.name")
  private val sampling=new Sampling(conf.getInt(s"mclab.lsh.seed$seedNumber"))
  private val typeOfIndex=conf.getString("mclab.lsh.typeOfIndex")
  LSH.generateByPulling = conf.getBoolean("mclab.lsh.generateByPulling")
  LSH.IsOrthogonal = conf.getBoolean("mclab.lsh.IsOrthogonal")
  //TODO: to implement two-level partition mechanism in PLSH, we have to expose this variable to
  //TODO: external side; we can actually fix it with Dependency Injection, etc.?
  private[mclab] val tableIndexGenerators: List[LSHTableHashChain[_]] = initHashChains()
  
  private def initHashChains[T <: LSHFunctionParameterSet](): List[LSHTableHashChain[_]] = {
    val familySize = conf.getInt("mclab.lsh.familySize")
    val vectorDim = conf.getInt("mclab.lsh.vectorDim")
    val chainLength = conf.getInt("mclab.lshTable.chainLength")
    val permutationNum=conf.getInt("mclab.lsh.permutationNum")
    val initializedChains = lshFamilyName match {
      case "angle" =>
        val family = Some(new AngleHashFamily(familySize = familySize, vectorDim = vectorDim,
          chainLength = chainLength, permutationNum=permutationNum))
        pickUpHashChains(family)
      case "pStable" =>
        val mu = conf.getDouble("mclab.lsh.family.pstable.mu")
        val sigma = conf.getDouble("mclab.lsh.family.pstable.sigma")
        val w = conf.getInt("mclab.lsh.family.pstable.w")
        val family = Some(new PStableHashFamily(familySize = familySize, vectorDim = vectorDim,
          chainLength = chainLength, pStableMu = mu, pStableSigma = sigma, w = w))
        pickUpHashChains(family)
      case x => None
    }
    if (initializedChains.isDefined) {
      initializedChains.get
    } else {
      List()
    }
  }

  /**
    * pick the lsh family, can be from file or random generate
    *
    * @param lshFamily
    * @tparam T
    * @return
    */
  private def pickUpHashChains[T <: LSHFunctionParameterSet](lshFamily: Option[LSHHashFamily[T]]):
    Option[List[LSHTableHashChain[T]]] = {
    require(lshFamily.isDefined, s"${lshFamilyName} is not a valid family name")
    val tableNum = conf.getInt("mclab.lsh.tableNum")
    val generateMethodOfHashFamily = conf.getString("mclab.lsh.generateMethod")
    lshFamily.map(lshHashFamily => {
      if (generateMethodOfHashFamily == "default") {
        lshHashFamily.pick(tableNum)
      } else if (generateMethodOfHashFamily == "fromfile"){
        lshHashFamily.generateTableChainFromFile(conf.getString("mclab.lsh.familyFilePath"),
          tableNum) 
      } else {
        null
      }
    })
  }

  /**
   * calculate the index of the vector in tables, the index in each table is represented as a 
   * byte array
    *
    * @param vector the vector to be indexed
   * @param tableId the id of the table
   * @return the index of the vector in tables, the order corresponds to the validTableIDs parameter
   */
  //The difference between doing the LSH and doing the LSH-based partition
  def calculateIndex(vector: SparseVector, tableId: Int = -1): Array[Int] = {
    if (tableId < 0) {
      (for (i <- tableIndexGenerators.indices)
        yield tableIndexGenerators(i).compute(vector)).toArray
    } else {
//      try{
//        if(Array.fill(1)(tableIndexGenerators(tableId).compute(vector))==null){
//          println("it's null")
//        }
//        Array.fill(1)(tableIndexGenerators(tableId).compute(vector))
//      }catch{
//        case e:NullPointerException =>{
//          println("yes")
//        }
//      }
      //TODO:sample the hashvalue, it's the more efficient way to do permutation.
      //ToDo: just remember to keep the random seed consistent when query the index
      val oneValue= typeOfIndex match {
        case "original" =>
          tableIndexGenerators(tableId).compute(vector)
        case "sampling" =>
          sampling.samplingOneKey(tableIndexGenerators(tableId).compute(vector))
        case "continueBitsCount" =>
          significantBits.continueBitsCount(tableIndexGenerators(tableId).compute(vector),Array(6,4,2,1))
        case "angleNewMethod" =>
          significantBits.newMethod(tableIndexGenerators(tableId).compute(vector))

      }
      //todo variable bits for deeper layer
      Array.fill(1)(oneValue)
    }
  }

  /**
    * Output the best hash functions if you find the performance is good.
    *
    * @param hashFunctionsID default as 0, put the all hash familes, otherwise only put the certain set hash functions
    */
  def outPutTheHashFunctionsIntoFile(hashFunctionsID:Int = -1): Unit = {
    if (hashFunctionsID == -1) {
      val writer = new FileWriter(new File(s"src/test/resources/hashFamily/bestHashFamily-${conf.getString("mclab.lsh.name")}"), true)
      lshFamilyName match {
        case "angle" =>
          tableIndexGenerators.map(x => x.chainedHashFunctions.map(x => writer.append(x.toString + "\r\n")))
        case "pStable" =>
          tableIndexGenerators.map(x => x.chainedHashFunctions.map(y => writer.append(y.toString + "\r\n")))
        case x => None
      }
      writer.close()
    } else {
      val writer = new FileWriter(new File(s"src/test/resources/hashFamily/theBestHashFamilyForPartition-${conf.getString("mclab.lsh.name")}"), true)
      lshFamilyName match {
        case "angle" =>
          tableIndexGenerators(hashFunctionsID).chainedHashFunctions.foreach(x => writer.write(x.toString + "\r\n"))
        case "pStable" =>
          tableIndexGenerators(hashFunctionsID).chainedHashFunctions.foreach(x => writer.write(x.toString + "\r\n"))
        case x => None
      }
    }
  }
}

private[lsh] object LSH {

  var generateByPulling = true
  var IsOrthogonal = true

  private[lsh] def generateParameterSetString(config: Config): String = {
    val sb = new StringBuffer()
    val lsh = new LSH(config)
    lsh.tableIndexGenerators.foreach(hashChain =>
      hashChain.chainedHashFunctions.foreach(hashParameterSet =>
        sb.append(hashParameterSet.toString + "\n"))
    )
    sb.toString
  }
  
  // output the generated parameters
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("usage: program configFilePath")
      sys.exit(1)
    }
    val config = ConfigFactory.parseString(
      """
        |mclab.lsh.generateMethod=default
      """.stripMargin).withFallback(ConfigFactory.parseFile(new File(args(0))))
    val str = generateParameterSetString(config)
    Files.write(Paths.get("file.txt"), str.getBytes(StandardCharsets.UTF_8))
  }
}
