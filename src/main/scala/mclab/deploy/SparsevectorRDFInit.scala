package mclab.deploy


import breeze.linalg._
import java.util.concurrent.{ExecutorService, Executors}

import breeze.linalg.{DenseMatrix, DenseVector}
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
private[mclab] object SparsevectorRDFInit {
  private var createFlag: Boolean = false
  private var tableNum = 0
  private var permutationNum = 0
  private var dataTableNumOfSubIndex = 0
  private var hashTableNumOfSubIndex = 0
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
    val featureDataType = conf.getString("mclab.lsh.featureDataFormat")
    this.permutationNum = permutationNum
    this.tableNum = tableNum
    this.dataTableNumOfSubIndex = numPartitions
    this.hashTableNumOfSubIndex = math.pow(2,partitionBits).toInt
    val confForPartitioner = ConfigFactory.parseString(
      s"""
         |mclab.confType=partition
         |mclab.lsh.vectorDim=32
         |mclab.lsh.tableNum = 1
         |mclab.lshTable.chainLength=$partitionBits
         |mclab.lsh.generateMethod=default
      """.stripMargin).withFallback(conf)
    def initializeVectorDatabase(tableId: Int): RandomDrawTreeMap[Int, Boolean] = {
      new RandomDrawTreeMap[Int, Boolean](
        tableId,
        "lsh",
        workingDirRoot + "-" + tableId,
        "partitionedTree-" + tableId,
        new LocalitySensitivePartitioner[Int](confForPartitioner, 0, partitionBits),
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
    * @param conf     configuration
    * @return the array of all denseVector in dataTable
    */
  def newFastFit(fileName: String, conf: Config): Array[Array[Double]] = {
    if (LSHServer.lshEngine == null) {
      LSHServer.lshEngine = new LSH(conf)
    }
    SparsevectorRDFInit.initializeRDFHashMap(conf)
    println("Finish initialize the RDF.")
    val AllSparseVectorsFile = getClass.getClassLoader.getResource(fileName).getFile
    val allDenseVectors = new ListBuffer[Array[Double]]
    var count = 0
    for (line <- Source.fromFile(AllSparseVectorsFile).getLines()) {
      val tmp = Vectors.fromPythonString(line)
      val currentSparseVector = new SparseVector(tmp._1, tmp._2, tmp._3, tmp._4)
      allDenseVectors += tmp._4
      SparsevectorRDFInit.vectorIdToVector.put(count, currentSparseVector)
      for (tableID <- 0 until this.tableNum * permutationNum) {
        SparsevectorRDFInit.vectorDatabase(tableID).put(count, true)
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
    * @param fileName the feature file name
    * @param conf     configuration
    * @return the Array of all densVectors in dataTable
    */
  def newMultiThreadFit(fileName: String, conf: Config): Array[Array[Double]] = {
    if (LSHServer.lshEngine == null) LSHServer.lshEngine = new LSH(conf)
    val threadNum = conf.getInt("mclab.insertThreadNum")
    SparsevectorRDFInit.initializeRDFHashMap(conf)
    println("Finish initialize the RDF.")
    val insertThreadPool: ExecutorService = Executors.newFixedThreadPool(threadNum)
    val AllSparseVectorsFile = getClass.getClassLoader.getResource(fileName).getFile
    val allDenseVectors = new ListBuffer[Array[Double]]
    var vectorId = 0
    var flag = true
    try {
      for (line <- Source.fromFile(AllSparseVectorsFile).getLines()) {
        val tmp = Vectors.fromPythonString(line)
        val currentSparseVector = new SparseVector(tmp._1, tmp._2, tmp._3, tmp._4)
        //Todo :fix the bug that tmp._4 is not include the 0, so it should change, also in transfer,
        //Todo: make a transfer method in Vector
        allDenseVectors += tmp._4
//        allDenseVectors += currentSparseVector.toArray
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
    }
    insertThreadPool.shutdown()
    while (flag) {
      if (insertThreadPool.isTerminated) {
        flag = false
      }
      Thread.sleep(5)
    }
    println("finish load, dataTable totally has " + vectorIdToVector.size() + " objects.")
    allDenseVectors.toArray
  }

  /**
    * new Multiple threads fit, to create the indexing faster. Used when the the query parameter is sparseVector array
    *
    * @param querySVArray query sparseVector array
    * @param threadNum    numebr of thread to put the query sparseVector into index first
    * @return the new added DenseVectors in dataTable; the query Array
    */
  private def newMultiThreadFit(querySVArray: Array[SparseVector], threadNum: Int = 5): (Array[Array[Double]], Array[Int]) = {
    //    if (LSHServer.lshEngine == null) LSHServer.lshEngine = new LSH(conf)
    //    val threadNum = conf.getInt("mclab.insertThreadNum")
    val insertThreadPool: ExecutorService = Executors.newFixedThreadPool(threadNum)
    val newDenseVectors = new ListBuffer[Array[Double]]
    val queryArray = new ArrayBuffer[Int]
    var vectorId = vectorIdToVector.size()
    var flag = true
    try {
      for (currentSparseVector <- querySVArray) {
        newDenseVectors += currentSparseVector.values
        this.vectorIdToVector.put(vectorId, currentSparseVector)
        try {
          for (i <- 0 until threadNum) {
            insertThreadPool.execute(new threadFit(vectorId,
              currentSparseVector,
              i * this.tableNum * this.permutationNum / threadNum,
              (i + 1) * this.tableNum * this.permutationNum / threadNum))
          }
        }
        queryArray += vectorId
        vectorId += 1
        if (vectorId % 10000 == 0) {
          println(vectorId + " objects loaded")
        }
      }
    }
    insertThreadPool.shutdown()
    while (flag) {
      if (insertThreadPool.isTerminated) {
        flag = false
      }
      Thread.sleep(5)
    }
    println("finish load, dataTable totally has " + vectorIdToVector.size() + " objects.")
    (newDenseVectors.toArray, queryArray.toArray)
  }

  /**
    * the fit thread
    *
    * @param vectorId the vectorID
    * @param vector   the real sparseVector data
    * @param start    the startTable for this thread to put
    * @param end      the endTable for this thread to put
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
        SparsevectorRDFInit.vectorDatabase(tableID).put(id, true)
    }
  }

  /**
    * Search the queryKey in index(Non-multiThread version)
    *
    * @param queryKey
    * @param steps steps in multi parition search
    * @return the similar objects key set in database
    */
  def querySingleKey(queryKey: Int,sparseVector: SparseVector, steps: Int = 0,L:Int): Set[AnyRef] = {
    //search through all LSHTables
    var finalResultsSet = Set.empty[AnyRef]
    try {
//      val timeA=System.nanoTime()
      SparsevectorRDFInit.vectorDatabase.slice(0,L).map(x => finalResultsSet=finalResultsSet.union(x.getSimilarWithStepWiseFaster(queryKey,sparseVector,steps).toArray().toSet))
//      val timeB=System.nanoTime()
//      println("Query in vectorDatabase, time is " + (timeB-timeA)/1000000.0 +"ms")
      finalResultsSet
    } catch {
      case ex: NullPointerException => println("need to fit the data first")
        null
    }
  }

  /**
    * Search the queryArray's similar objects in index(Non-multiThread version)
    *
    * @param queryArray
    * @param steps steps in multi parition search
    * @return
    */
  def queryBatch(queryArray: Array[Int], sparseVectorArray: Array[SparseVector], steps: Int,L:Int=tableNum*permutationNum): Array[Set[AnyRef]] = {
    val resultsArray: ArrayBuffer[Set[AnyRef]] = ArrayBuffer.empty[Set[AnyRef]]
    for (i <- queryArray.indices) {
      resultsArray += querySingleKey(queryArray(i), sparseVectorArray(i),steps,L)
    }
    resultsArray.toArray
  }


  /**
    * resultsArray to save the all similar objects of keys
    */
  //  @volatile private val resultsArray:ArrayBuffer[Set[AnyRef]]=ArrayBuffer.empty[Set[AnyRef]]
  @volatile private var resultsArray: Array[Set[AnyRef]] = null

  /**
    * Each query multi thread search over all hashTable,
    * means one thread take responsibility to certain number of hashTable
    *
    * @param queryArray     the keys array
    * @param queryThreadNum numebr of query thread
    * @param steps          steps in multi parition search
    * @return the similar objects (is the keys array, which save in dataTable)
    */
  def NewMultiThreadQueryBatch(queryArray: Array[Int], steps: Int = 0, queryThreadNum: Int = 5): Array[Set[AnyRef]] = {
    var flagQuery = true
    this.resultsArray = new Array[Set[AnyRef]](queryArray.length)
    for (i <- resultsArray.indices) {
      resultsArray(i) = Set()
    }
    val queryThreadPool: ExecutorService = Executors.newFixedThreadPool(queryThreadNum)
    //Todo multiThread, remember to add synchronized on union result operation...
    try {
      for (i <- 0 until queryThreadNum) {
        queryThreadPool.execute(new threadQuery(queryArray,
          i * this.tableNum * this.permutationNum / queryThreadNum,
          (i + 1) * this.tableNum * this.permutationNum / queryThreadNum,
          steps))
      }
    }
    queryThreadPool.shutdown()
    while (flagQuery) {
      if (queryThreadPool.isTerminated) {
        flagQuery = false
      }
      Thread.sleep(5)
    }
    resultsArray
  }

  /**
    * Each query multi thread search over all hashTable,
    * means one thread take responsibility to certain number of hashTable
    * For system use
    *
    * @param querySVArray   the query sparseVector array
    * @param steps          steps in multi parition search
    * @param queryThreadNum numebr of query thread
    * @return the similar objects (is the keys array, which save in dataTable)
    */
  def NewMultiThreadQueryBatch(querySVArray: Array[SparseVector], steps: Int, queryThreadNum: Int): Array[Set[AnyRef]] = {
    //fit the new SparseVectors first, and get the keys array
    val (_, queryArray) = this.newMultiThreadFit(querySVArray, queryThreadNum)
    var flagQuery = true
    this.resultsArray = new Array[Set[AnyRef]](queryArray.length)
    for (i <- resultsArray.indices) {
      resultsArray(i) = Set()
    }
    val queryThreadPool: ExecutorService = Executors.newFixedThreadPool(queryThreadNum)
    //Todo multiThread, remember to add synchronized on union result operation...
    try {
      for (i <- 0 until queryThreadNum) {
        queryThreadPool.execute(new threadQuery(queryArray,
          i * this.tableNum * this.permutationNum / queryThreadNum,
          (i + 1) * this.tableNum * this.permutationNum / queryThreadNum,
          steps))
      }
    }
    queryThreadPool.shutdown()
    while (flagQuery) {
      if (queryThreadPool.isTerminated) {
        flagQuery = false
      }
      Thread.sleep(5)
    }
    resultsArray
  }


  private class threadQuery(queryArray: Array[Int], startTable: Int, endTable: Int, steps: Int = 0)
    extends Runnable {
    override def run(): Unit = {
      QueryTask.query(queryArray, startTable, endTable, steps)
    }
  }

  /**
    * the query task
    * Steps: 1.search the tables from startTable to endTable to get the resultset for each query key
    * 2.if the result set is alreay exit in resultArray, do union operation, otherwise append a new one.
    */
  private object QueryTask {
    def query(queryArray: Array[Int], startTable: Int, endTable: Int, steps: Int = 0): Unit = {
      for (i <- queryArray.indices) {
        var oneResultsSet = Set.empty[AnyRef]
        try {
          for (tableId <- startTable until endTable) {
            oneResultsSet = oneResultsSet.union(SparsevectorRDFInit.vectorDatabase(tableId).getSimilarWithStepWise(queryArray(i), steps).toArray().toSet)
          }
        } catch {
          case ex: NullPointerException => println("need to fit the data first")
        }
        //better way to achieve multi-Thread
        this.synchronized {
          if (oneResultsSet != null)
            resultsArray(i) = resultsArray(i).union(oneResultsSet)
        }
      }
    }
  }

  /**
    * read the top k ground truth from file
    *
    * @param filename
    * @return each ground truth is a set[Int]
    */
  def getTopKGroundTruth(filename: String, K: Int): Array[Set[Int]] = {
    val gtFile = getClass.getClassLoader.getResource(filename).getFile
    val resultsArray: ListBuffer[Set[Int]] = new ListBuffer[Set[Int]]
    for (line <- Source.fromFile(gtFile).getLines()) {
      resultsArray += Vectors.analysisKNN(line, K).toSet
    }
    resultsArray.toArray
  }


  /**
    * Clear the index, delete all objects. And close the engines
    */
  def clearAndClose(): Unit = {
    SparsevectorRDFInit.vectorIdToVector.clear()
    SparsevectorRDFInit.vectorDatabase.foreach(x => x.clear())
    SparsevectorRDFInit.vectorIdToVector.close()
    SparsevectorRDFInit.vectorDatabase.foreach(x => x.close())
  }


  //ToDO calculate the top k from similar object, by using matrix transfer
  /**
    * get the topK similar objects and calculate the precision based on ground truth
    *
    * @param allDenseVectors the feature Data
    * @param groundTruth     ground truth data
    * @param conf            configuration
    * @param steps           steps in multi parition search
    * @return (topK,precision)
    */
  def topKAndPrecisionScore(allDenseVectors: Array[Array[Double]], groundTruth: Array[Set[Int]], conf: Config, steps: Int = 0): (Array[Array[Int]], Double,Long) = {
    val queryArray = groundTruth.indices.toArray
    var averageScore = 0.0
    val allQueryedTopK = new ArrayBuffer[Array[Int]]
    val queryNum=groundTruth.length
    val sv:Array[SparseVector]=new Array(queryNum)
    val dim=allDenseVectors(0).length
//    val indices=new Array[Int]()
    for(i <- 0 until queryNum){sv(i)=Vectors.sparse(i,dim,Array.range(0,dim),allDenseVectors(i)).asInstanceOf[SparseVector]}
//    val resultsSet = this.NewMultiThreadQueryBatch(queryArray, steps, conf.getInt("mclab.queryThreadNum"))
    val timeA = System.currentTimeMillis()
    val resultsSet = this.query(queryArray, sv, steps, conf.getInt("mclab.queryThreadNum"))
    val timeB = System.currentTimeMillis()
    for (i <- resultsSet.indices) {
      var score = 0.0
      if (resultsSet(i) != Set.empty[AnyRef]) {
        val resultSetForOneQuery = resultsSet(i).toArray
        val queryDenseVector: Array[Double] = allDenseVectors(i)
        val dataSetMatirx: ArrayBuffer[Array[Double]] = new ArrayBuffer[Array[Double]]
        //        println("size=" + resultSetForOneQuery.length)
        for (j <- resultSetForOneQuery.indices) {
          dataSetMatirx += allDenseVectors(resultSetForOneQuery(j).toString.toInt)
        }
        val dv1 = DenseVector(queryDenseVector: _*)
        val dv2 = DenseMatrix(dataSetMatirx: _*)
        val a = System.currentTimeMillis()
        val indexList = argsort(dv2 * dv1).reverse.slice(0, conf.getInt("mclab.lsh.topK"))
        val b = System.currentTimeMillis()
        //        println("For query " + (i) + ", the results are: ")
        val tmpOneQueryedTopK = new ArrayBuffer[Int]
        for (w <- indexList.indices) {
          //          print(resultSetForOneQuery(indexList(w)) + ",")
          tmpOneQueryedTopK += resultSetForOneQuery(indexList(w)).toString.toInt
          if (groundTruth(i).contains(resultSetForOneQuery(indexList(w)).toString.toInt)) {
            score += 1
          }
        }
        allQueryedTopK += tmpOneQueryedTopK.toArray
        //        println("####score=" + score + " distanceCal time is " + (b - a))
        averageScore += score / queryArray.length
      }
    }
    (allQueryedTopK.toArray, averageScore / conf.getInt("mclab.lsh.topK"),timeB-timeA)
  }


  /**
    * get the dataTable and hashTable average distribution of number of objects in different sub-index
 *
    * @return
    */
  def getDtAndHtNumDistribution(): (Array[Double], Array[Double]) = {
    val dtDistribution:Array[Double]= new Array(dataTableNumOfSubIndex)
    var htDistribution:Array[Double]= new Array(hashTableNumOfSubIndex)
    if (this.vectorIdToVector != null){
      val tmp=this.vectorIdToVector.allSubIndexObjectsNumberDistribution()
      for(i <- 0 until tmp.size()){dtDistribution(i) += tmp.get(i)}
    }
    if(this.vectorDatabase!=null){
      for(oneHt <- this.vectorDatabase){
        val tmp=oneHt.allSubIndexObjectsNumberDistribution()
        for(i <- 0 until tmp.size()){htDistribution(i) += tmp.get(i)}
      }
      htDistribution=htDistribution.map(x => x/this.tableNum)
    }
    (dtDistribution, htDistribution)
  }

  /**
    *
    */
  def query(queryKeyArray: Array[Int], querySVArray:Array[SparseVector], steps: Int = 0, queryThreadNum: Int = 10): Array[Set[AnyRef]] = {
    var flagQuery = true
    this.resultsArray = new Array[Set[AnyRef]](queryKeyArray.length)
    for (i <- resultsArray.indices) {
      resultsArray(i) = Set()
    }
    val queryThreadPool: ExecutorService = Executors.newFixedThreadPool(queryThreadNum)
    //Todo multiThread, remember to add synchronized on union result operation...
    try {
      for (i <- 0 until queryThreadNum) {
        queryThreadPool.execute(new threadQueryNew(queryKeyArray, querySVArray,
          i * this.tableNum * this.permutationNum / queryThreadNum,
          (i + 1) * this.tableNum * this.permutationNum / queryThreadNum,
          steps))
      }
    }
    queryThreadPool.shutdown()
    while (flagQuery) {
      if (queryThreadPool.isTerminated) {
        flagQuery = false
      }
      Thread.sleep(5)
    }
    resultsArray
  }


  private class threadQueryNew(queryArray: Array[Int], querySVArray:Array[SparseVector], startTable: Int, endTable: Int, steps: Int = 0)
    extends Runnable {
    override def run(): Unit = {
      QueryTaskNew.query(queryArray, querySVArray,startTable, endTable, steps)
    }
  }

  /**
    * the query task
    * Steps: 1.search the tables from startTable to endTable to get the resultset for each query key
    * 2.if the result set is alreay exit in resultArray, do union operation, otherwise append a new one.
    */
  private object QueryTaskNew {
    def query(queryArray: Array[Int], querySVArray:Array[SparseVector],startTable: Int, endTable: Int, steps: Int = 0): Unit = {
      for (i <- queryArray.indices) {
        var oneResultsSet = Set.empty[AnyRef]
        try {
          for (tableId <- startTable until endTable) {
            oneResultsSet = oneResultsSet.union(SparsevectorRDFInit.vectorDatabase(tableId).getSimilarWithStepWiseFaster(queryArray(i), querySVArray(i),steps).toArray().toSet)
          }
        } catch {
          case ex: NullPointerException => println("need to fit the data first")
        }
        //better way to achieve multi-Thread
        this.synchronized {
          if (oneResultsSet != null)
            resultsArray(i) = resultsArray(i).union(oneResultsSet)
        }
      }
    }
  }






}
