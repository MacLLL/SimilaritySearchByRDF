package mclab.deploy

import java.util
import java.util.concurrent.{ExecutorService, Executors}

import com.typesafe.config.{Config, ConfigFactory}
import mclab.lsh.vector.{SparseVector, Vectors}
import mclab.mapdb.RandomDrawTreeMap
import mclab.utils.{HashPartitioner, LocalitySensitivePartitioner, Serializers}
import mclab.lsh.LSH

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source

/**
  * initialize the (dataTable, lshTable) for single feature
  * implement the put get method
  */
private[mclab] object SingleRDFInit {
  private var createFlag: Boolean = false
  private var tableNum = 0
  private var permutationNum = 0
  //lshTable
  var vectorDatabase: Array[RandomDrawTreeMap[Int, Boolean]] = null
  //dataTable
  @volatile var vectorIdToVector: RandomDrawTreeMap[Int, SparseVector] = null

  /**
    * initialize definition for HashTable
    *
    * @param tableName    tablename can be lshTable and dataTable
    * @param confInstance the configurtion
    * @param table
    */
  private def setupTable(tableName: String, confInstance: Config,
                         table: RandomDrawTreeMap[_, _]): Unit = {
    table.BUCKET_OVERFLOW = confInstance.getInt(s"mclab.$tableName.bufferOverflow")
    table.updateBucketLength(confInstance.getInt(s"mclab.$tableName.bucketBits"))
    table.updateDirectoryNodeSize(confInstance.getInt(s"mclab.$tableName.dirNodeSize"),
      confInstance.getInt(s"mclab.$tableName.chainLength"))
  }

  /**
    * initialize RDFHashMap
    *
    * @param conf configuration
    */
  def initializeRDFHashMap(conf: Config): Unit = {
    createFlag = true
    val tableNum = conf.getInt("mclab.lsh.tableNum")
    val workingDirRoot = conf.getString("mclab.lsh.workingDirRoot")
    val ramThreshold = conf.getInt("mclab.lsh.ramThreshold")
    val numPartitions = conf.getInt("mclab.dataTable.numPartitions")
    val partitionBits = conf.getInt("mclab.lsh.partitionBits")
    val permutationNum = conf.getInt("mclab.lsh.permutationNum")
    this.permutationNum = permutationNum
    this.tableNum = tableNum
    val confForPartitioner = ConfigFactory.parseString(
      s"""
         |mclab.lsh.vectorDim=32
         |mclab.lsh.chainLength=$partitionBits
      """.stripMargin).withFallback(conf)
    def initializeVectorDatabase(tableId: Int): RandomDrawTreeMap[Int, Boolean] = {
      new RandomDrawTreeMap[Int, Boolean](
        tableId,
        "lsh",
        workingDirRoot + "-" + tableId,
        "partitionedTree-" + tableId,
        new LocalitySensitivePartitioner[Int](confForPartitioner, tableId, partitionBits),
        true,
        1,
        Serializers.scalaIntSerializer,
        null,
        null,
        Executors.newCachedThreadPool(),
        true,
        ramThreshold)
    }
    def initializeIdToVectorMap(): RandomDrawTreeMap[Int, SparseVector] = {
      new RandomDrawTreeMap[Int, SparseVector](
        tableNum,
        "default",
        workingDirRoot + "-vector",
        "vectorIdToVector",
        new HashPartitioner[Int](numPartitions),
        true,
        1,
        Serializers.scalaIntSerializer,
        Serializers.vectorSerializer,
        null,
        Executors.newCachedThreadPool(),
        true,
        ramThreshold)
    }
    vectorDatabase = new Array[RandomDrawTreeMap[Int, Boolean]](tableNum * permutationNum)
    for (tableId <- 0 until tableNum * permutationNum) {
      vectorDatabase(tableId) = initializeVectorDatabase(tableId)
      setupTable("lshTable", conf, vectorDatabase(tableId))
    }
    vectorIdToVector = initializeIdToVectorMap()
    setupTable("dataTable", conf, vectorIdToVector)
    for (tableId <- 0 until tableNum * permutationNum) {
      vectorDatabase(tableId).initStructureLocks()
    }
    vectorIdToVector.initStructureLocks()
  }

  /**
    * Non-multithread methods to fit feature data into index.
    *
    * @param fileName the fileurl in resources
    * @param conf configuration
    * @return the array of dataset
    */
  def newFastFit(fileName: String, conf: Config): Array[Array[Double]] = {
    if (LSHServer.lshEngine == null) {
      LSHServer.lshEngine = new LSH(conf)
    }
    SingleRDFInit.initializeRDFHashMap(conf)
    println("finish initialize the hash tree.")
    val AllSparseVectorsFile = getClass.getClassLoader.getResource(fileName).getFile
    val allDenseVectors = new ListBuffer[Array[Double]]
    var count = 0
    for (line <- Source.fromFile(AllSparseVectorsFile).getLines()) {
      val tmp = Vectors.fromPythonString(line)
      val currentSparseVector = new SparseVector(tmp._1, tmp._2, tmp._3, tmp._4)
      allDenseVectors += tmp._4
      SingleRDFInit.vectorIdToVector.put(count, currentSparseVector)
      for (tableID <- 0 until this.tableNum * permutationNum) {
        SingleRDFInit.vectorDatabase(tableID).put(count, true)
      }
      count += 1
      if (count % 10000 == 0) {
        println(count + " objects loaded")
      }
    }
    println("finish load, totally " + count + " objects.")
    allDenseVectors.toArray
  }


  /**
    * new Multiple threads fit, to create the indexing faster.
    *
    * @param fileName
    * @param conf
    * @return
    */
  def newMultiThreadFit(fileName: String, conf: Config): Array[Array[Double]] = {
    if (LSHServer.lshEngine == null) LSHServer.lshEngine = new LSH(conf)
    val threadNum = conf.getInt("mclab.insertThreadNum")
    SingleRDFInit.initializeRDFHashMap(conf)
    val insertThreadPool: ExecutorService = Executors.newFixedThreadPool(threadNum)
    val AllSparseVectorsFile = getClass.getClassLoader.getResource(fileName).getFile
    val allDenseVectors = new ListBuffer[Array[Double]]
    var vectorId = 0
    val a = System.currentTimeMillis()
    try {
      for (line <- Source.fromFile(AllSparseVectorsFile).getLines()) {
        val tmp = Vectors.fromPythonString(line)
        val currentSparseVector = new SparseVector(tmp._1, tmp._2, tmp._3, tmp._4)
        allDenseVectors += tmp._4
        this.vectorIdToVector.put(vectorId, currentSparseVector)
        try {
          for (i <- 0 until threadNum) {
            insertThreadPool.execute(new threadFit(vectorId,
              currentSparseVector,
              i * this.tableNum * this.permutationNum / threadNum,
              (i + 1) * this.tableNum * this.permutationNum / threadNum))
          }
        }
        vectorId += 1
        if (vectorId % 10000 == 0) {
          println(vectorId + " objects loaded")
        }
      }
    } finally {
      insertThreadPool.shutdown()
    }
    val b = System.currentTimeMillis()
    print("time is " + (b - a) / 1000 + "s")
    allDenseVectors.toArray
  }

  /**
    * the fit thread
    *
    * @param vectorId the vectorID
    * @param vector the real sparseVector data
    * @param start the startTable for this thread to put
    * @param end the endTable for this thread to put
    */
  private class threadFit(vectorId: Int, vector: SparseVector, start: Int, end: Int) extends Runnable {
    override def run(): Unit = {
      InsertTask.insert(vectorId, vector, start, end)
    }
  }

  /**
    * the insert task
    */
  private object InsertTask {
    def insert(id: Int, vector: SparseVector, startTable: Int, endTable: Int): Unit = {
      for (tableID <- startTable until endTable)
        SingleRDFInit.vectorDatabase(tableID).put(id, true)
    }
  }

  /**
    * search the queryKey in index
    * @param queryKey
    * @return the similar objects key set in database
    */
  def querySingleKey(queryKey:Int):Set[AnyRef] = {
    //search through all LSHTables
    var finalResultsSet = Set.empty[AnyRef]
    try {
      for (i <- SingleRDFInit.vectorDatabase.indices) {
        val SingleLSHTableResults = SingleRDFInit.vectorDatabase(i).getSimilar(queryKey).toArray().toSet
        finalResultsSet = finalResultsSet.union(SingleLSHTableResults)
      }
      finalResultsSet
    } catch {
      case ex: NullPointerException => println("need to fit the data first")
        null
    }
  }


  /**
    * Each query multi thread search over all hashTable,
    * means one thread take responsibility to certain number of hashTable
    * @param queryArray the keys array
    * @param queryThreadNum numebr of query thread
    * @return the similar objects for each key
    */
  def NewMultiThreadQueryBatch(queryArray:Array[Int],queryThreadNum:Int=5):Array[Set[AnyRef]]={
    val resultsArray:ArrayBuffer[Set[AnyRef]]=ArrayBuffer.empty[Set[AnyRef]]
    val queryThreadPool: ExecutorService = Executors.newFixedThreadPool(queryThreadNum)
    try{
      queryThreadPool.execute(new thr)
    }

    //Todo 多线程操作,然后记得对unionresult的操作加上synchronized...


  }

  private class threadQuery() extends Runnable{
    override def run(): Unit = {
      InsertTask.insert(vectorId, vector, start, end)
    }
  }

  private 


//  def queryBatch(queryArray:Array[Int]):Array[Set[AnyRef]]={
//    val resultsArray:ArrayList[Set[AnyRef]]=ArrayBuffer.empty[Set[AnyRef]]
//    for( i<- queryArray.indices){
//      resultsArray += querySingleSet(queryArray(i))
//    }
//    resultsArray.toArray
//  }









}
