package mclab.deploy

import java.util.concurrent.{ExecutorService, Executors}

import akka.actor._
import com.typesafe.config.{Config, ConfigFactory}
import mclab.mapdb.RandomDrawTreeMap
import mclab.lsh.vector.{SparseVector, Vectors}
import mclab.lsh.LSH
import mclab.utils.{HashPartitioner, LocalitySensitivePartitioner, Serializers}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source
import scala.language.postfixOps

/**
  * initialize the Tables(mainTable,LSHTable)
  */
private[mclab] object HashTableInit {
  var createFlag:Boolean=false
  private var tableNum=0
  private var permutationNum=0
  private def setTableNum(num:Int):Unit={
    tableNum=num
  }
  private def setPermutationNum(num:Int):Unit={
    permutationNum=num
  }
  //inital definination for HashTable
  private def setupTable(tableName: String, confInstance: Config,
                         table: RandomDrawTreeMap[_, _]): Unit = {
    table.BUCKET_OVERFLOW = confInstance.getInt(s"cpslab.$tableName.bufferOverflow")
    table.updateBucketLength(confInstance.getInt(s"cpslab.$tableName.bucketBits"))
    table.updateDirectoryNodeSize(confInstance.getInt(s"cpslab.$tableName.dirNodeSize"),
      confInstance.getInt(s"cpslab.$tableName.chainLength"))
  }

//  def initializeActorBasedHashTree(conf: Config): Unit = {
//    val tableNum = conf.getInt("cpslab.lsh.tableNum")
//    setTableNum(tableNum)
//    val numPartitions = conf.getInt("cpslab.mainTable.numPartitions")
//    val workingDirRoot = conf.getString("cpslab.lsh.workingDirRoot")
//    val ramThreshold = conf.getInt("cpslab.lsh.ramThreshold")
//    val partitionBits = conf.getInt("cpslab.lsh.partitionBits")
//    val permutationNum=conf.getInt("cpslab.lsh.permutationNum")
//    setPermutationNum(permutationNum)
//    val confForPartitioner = ConfigFactory.parseString(
//      s"""
//         |cpslab.lsh.vectorDim=32
//         |cpslab.lshTable.chainLength=$partitionBits
//      """.stripMargin).withFallback(conf)
//    def initializeVectorDatabase(tableId: Int): RandomDrawTreeMap[Int, Boolean] = {
//      new ActorBasedPartitionedHTreeMap[Int, Boolean](
//        conf,
//        tableId,
//        "lsh",
//        workingDirRoot + "-" + tableId,
//        "partitionedTree-" + tableId,
////        new HashPartitioner[Int](numPartitions),
//        new LocalitySensitivePartitioner[Int](confForPartitioner, tableId, partitionBits),
//        true,
//        1,
//        Serializers.scalaIntSerializer,
//        null,
//        null,
//        Executors.newCachedThreadPool(),
//        true,
//        ramThreshold)
//    }
//    def initializeIdToVectorMap(conf: Config): PartitionedHTreeMap[Int, SparseVector] = {
//      new ActorBasedPartitionedHTreeMap[Int, SparseVector](
//        conf,
//        tableNum,
//        "default",
//        workingDirRoot + "-vector",
//        "vectorIdToVector",
//        new HashPartitioner[Int](numPartitions),
//        true,
//        1,
//        Serializers.scalaIntSerializer,
//        Serializers.vectorSerializer,
//        null,
//        Executors.newCachedThreadPool(),
//        true,
//        ramThreshold)
//    }
//    ActorBasedPartitionedHTreeMap.actorSystem = ActorSystem("AK", conf)
//    vectorDatabase = new Array[PartitionedHTreeMap[Int, Boolean]](tableNum)
//    for (tableId <- 0 until tableNum) {
//      vectorDatabase(tableId) = initializeVectorDatabase(tableId)
//      setupTable("lshTable", conf, vectorDatabase(tableId))
//      vectorDatabase(tableId).initStructureLocks()
//    }
//    vectorIdToVector = initializeIdToVectorMap(conf)
//    setupTable("mainTable", conf, vectorIdToVector)
//    vectorIdToVector.initStructureLocks()
//  }

  def initializeMapDBHashMap(conf: Config): Unit = {
    this.createFlag=true
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    setTableNum(tableNum)
    val workingDirRoot = conf.getString("cpslab.lsh.workingDirRoot")
    val ramThreshold = conf.getInt("cpslab.lsh.ramThreshold")
    val numPartitions = conf.getInt("cpslab.mainTable.numPartitions")
    val partitionBits = conf.getInt("cpslab.lsh.partitionBits")
    val permutationNum=conf.getInt("cpslab.lsh.permutationNum")
    setPermutationNum(permutationNum)
    val confForPartitioner = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.vectorDim=32
         |cpslab.lsh.chainLength=$partitionBits
      """.stripMargin).withFallback(conf)
    def initializeVectorDatabase(tableId: Int): RandomDrawTreeMap[Int, Boolean] = {
      val newTree = new RandomDrawTreeMap[Int, Boolean](
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
      newTree
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
    vectorDatabase= new Array[RandomDrawTreeMap[Int, Boolean]](tableNum*permutationNum)
    for (tableId <- 0 until tableNum*permutationNum) {
      vectorDatabase(tableId) = initializeVectorDatabase(tableId)
      setupTable("lshTable", conf, vectorDatabase(tableId))
    }
    vectorIdToVector = initializeIdToVectorMap()
    setupTable("mainTable", conf, vectorIdToVector)
    for (tableId <- 0 until tableNum*permutationNum) {
      vectorDatabase(tableId).initStructureLocks()
    }
    vectorIdToVector.initStructureLocks()
  }


//  def initializePartitionedHashMap(conf: Config): Unit = {
//    val tableNum = conf.getInt("cpslab.lsh.tableNum")
//    setTableNum(tableNum)
//    val workingDirRoot = conf.getString("cpslab.lsh.workingDirRoot")
//    val ramThreshold = conf.getInt("cpslab.lsh.ramThreshold")
//    val numPartitions = conf.getInt("cpslab.mainTable.numPartitions")//partition for mainTable
//    // LSHTable configurations
//    val partitionBits = conf.getInt("cpslab.lsh.partitionBits")//partition number for lshTable, 2bits for 4 partitions
//    val ifFromFile = conf.getString("cpslab.lsh.partitionBitsGenerateMethod")
//    val permutationNum=conf.getInt("cpslab.lsh.permutationNum")
//    setPermutationNum(permutationNum)
//    //default lsh hash value dimension is 32, and we need lshTable.chainLength is partitionBits
//    val confForPartitioner = ConfigFactory.parseString(
//      s"""
//         |cpslab.lsh.vectorDim=32
//         |cpslab.lsh.generateMethod=$ifFromFile
//         |cpslab.lsh.familyFilePath=partitionFunc.txt
//         |cpslab.lshTable.chainLength=$partitionBits
//      """.stripMargin).withFallback(conf)
//    def initializeVectorDatabase(tableId: Int): RandomDrawTreeMap[Int, Boolean] = {
//      new ActorPartitionedHTreeBasic[Int, Boolean](
//        tableId,
//        "lsh",
//        workingDirRoot + "-" + tableId,
//        "partitionedTree-" + tableId,
//        new LocalitySensitivePartitioner[Int](confForPartitioner, tableId, partitionBits),
//        true,
//        1,
//        Serializers.scalaIntSerializer,
//        null,
//        null,
//        Executors.newCachedThreadPool(),
//        true,
//        ramThreshold)
//    }
//    def initializeIdToVectorMap(): PartitionedHTreeMap[Int, SparseVector] = {
//      new ActorPartitionedHTreeBasic[Int, SparseVector](
//        tableNum,
//        "default",
//        workingDirRoot + "-vector",
//        "vectorIdToVector",
//        new HashPartitioner[Int](numPartitions),
//        true,
//        1,
//        Serializers.scalaIntSerializer,
//        Serializers.vectorSerializer,
//        null,
//        Executors.newCachedThreadPool(),
//        true,
//        ramThreshold)
//    }
//    vectorDatabase = new Array[PartitionedHTreeMap[Int, Boolean]](permutationNum*tableNum)
//    for (tableId <- 0 until permutationNum*tableNum) {
//      vectorDatabase(tableId) = initializeVectorDatabase(tableId)
//      setupTable("lshTable", conf, vectorDatabase(tableId))
//    }
//    vectorIdToVector = initializeIdToVectorMap()
//    setupTable("mainTable", conf, vectorIdToVector)
//
//    for (tableId <- 0 until permutationNum*tableNum) {
//      vectorDatabase(tableId).initStructureLocks()
//    }
//    vectorIdToVector.initStructureLocks()
//  }



  //many hash trees
  var vectorDatabase: Array[RandomDrawTreeMap[Int, Boolean]] = null
//  MainTable
  var vectorIdToVector: RandomDrawTreeMap[Int, SparseVector] = null

  def initializeMapDBHashMultiple(conf: Config): Unit = {
    this.createFlag=true
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    setTableNum(tableNum)
    val workingDirRoot = conf.getString("cpslab.lsh.workingDirRoot")
    val ramThreshold = conf.getInt("cpslab.lsh.ramThreshold")
    val numPartitions = conf.getInt("cpslab.mainTable.numPartitions")
    val partitionBits = conf.getInt("cpslab.lsh.partitionBits")
    val permutationNum=conf.getInt("cpslab.lsh.permutationNum")
    setPermutationNum(permutationNum)
    val confForPartitioner = ConfigFactory.parseString(
      s"""
         |cpslab.lsh.vectorDim=32
         |cpslab.lsh.chainLength=$partitionBits
      """.stripMargin).withFallback(conf)
    def initializeVectorDatabase(tableId: Int): RandomDrawTreeMap[Int, Boolean] = {
      val newTree = new RandomDrawTreeMap[Int, Boolean](
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
      newTree
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
    vectorDatabase_blue= new Array[RandomDrawTreeMap[Int, Boolean]](tableNum*permutationNum)
    for (tableId <- 0 until tableNum*permutationNum) {
      vectorDatabase_blue(tableId) = initializeVectorDatabase(tableId)
      setupTable("lshTable", conf, vectorDatabase_blue(tableId))
    }
    vectorIdToVector_blue = initializeIdToVectorMap()
    setupTable("mainTable", conf, vectorIdToVector_blue)
    for (tableId <- 0 until tableNum*permutationNum) {
      vectorDatabase_blue(tableId).initStructureLocks()
    }
    vectorIdToVector_blue.initStructureLocks()
    vectorDatabase_green= new Array[RandomDrawTreeMap[Int, Boolean]](tableNum*permutationNum)
    for (tableId <- 0 until tableNum*permutationNum) {
      vectorDatabase_green(tableId) = initializeVectorDatabase(tableId)
      setupTable("lshTable", conf, vectorDatabase_green(tableId))
    }
    vectorIdToVector_green = initializeIdToVectorMap()
    setupTable("mainTable", conf, vectorIdToVector_green)
    for (tableId <- 0 until tableNum*permutationNum) {
      vectorDatabase_green(tableId).initStructureLocks()
    }
    vectorIdToVector_green.initStructureLocks()
    vectorDatabase_red= new Array[RandomDrawTreeMap[Int, Boolean]](tableNum*permutationNum)
    for (tableId <- 0 until tableNum*permutationNum) {
      vectorDatabase_red(tableId) = initializeVectorDatabase(tableId)
      setupTable("lshTable", conf, vectorDatabase_red(tableId))
    }
    vectorIdToVector_red = initializeIdToVectorMap()
    setupTable("mainTable", conf, vectorIdToVector_red)
    for (tableId <- 0 until tableNum*permutationNum) {
      vectorDatabase_red(tableId).initStructureLocks()
    }
    vectorIdToVector_red.initStructureLocks()
  }





  var vectorDatabase_blue:Array[RandomDrawTreeMap[Int,Boolean]] =null
  var vectorDatabase_green:Array[RandomDrawTreeMap[Int,Boolean]] =null
  var vectorDatabase_red:Array[RandomDrawTreeMap[Int,Boolean]] =null

  var vectorIdToVector_blue: RandomDrawTreeMap[Int, SparseVector] = null
  var vectorIdToVector_green: RandomDrawTreeMap[Int, SparseVector] = null
  var vectorIdToVector_red: RandomDrawTreeMap[Int, SparseVector] = null




  def getLSHTableNum(conf:Config):Int=conf.getInt("cpslab.lsh.tableNum")
  def getLSHTableEachPartitionNum():Unit={
    for(i<- 0 until this.vectorDatabase.length){
      this.vectorDatabase(i).getPartitionNums()
    }

  }
  val threadPool:ExecutorService=Executors.newFixedThreadPool(10)
//  //每个线程搜索
//  def newQuerySingleSet(queryKey:Int)={
//
//  }


  def querySingleSet(queryKey:Int):Set[AnyRef]={
    //search through all LSHTables
    var finalResultsSet=Set.empty[AnyRef]
    try{
      for (i <- 0 until HashTableInit.vectorDatabase.length){
        val SingleLSHTableResults=HashTableInit.vectorDatabase(i).getSimilar(queryKey).toArray().toSet
        finalResultsSet=finalResultsSet.union(SingleLSHTableResults)
      }
//      println(finalResultsSet)
//      println(finalResultsSet.size)
      finalResultsSet
    }catch{
      case ex:NullPointerException => println("need to fit the data first")
        null
    }

//    val results=this.
  }
  def queryBatch(queryArray:Array[Int]):Array[Set[AnyRef]]={
    val resultsArray:ArrayBuffer[Set[AnyRef]]=ArrayBuffer.empty[Set[AnyRef]]
    for( i<- queryArray.indices){
      resultsArray += querySingleSet(queryArray(i))
    }
    resultsArray.toArray
  }


//  def mutiThreadQuery(): Unit ={
//    val threadPool  :ExecutorService=Executors.newFixedThreadPool(10)
//    try{
//      val future=new Future[Set[AnyRef]](new Callable[String] {
//        override def call():Set[AnyRef]={
//          val oneTableResult=HashTableInit.vectorDatabase()
//
//        }
//      })
//    }
//  }

    def multiFeatureSingleQuery(queryKey:Int):Set[AnyRef]={
      var finalResultsSet1=Set.empty[AnyRef]
      var finalResultsSet2=Set.empty[AnyRef]
      var finalResultsSet3=Set.empty[AnyRef]
      try{
        for (i <- 0 until HashTableInit.vectorDatabase_blue.length){
          val SingleLSHTableResults=HashTableInit.vectorDatabase_blue(i).getSimilar(queryKey,1).toArray().toSet
          finalResultsSet1=finalResultsSet1.union(SingleLSHTableResults)
        }
        for (i <- 0 until HashTableInit.vectorDatabase_green.length){
          val SingleLSHTableResults=HashTableInit.vectorDatabase_green(i).getSimilar(queryKey,2).toArray().toSet
          finalResultsSet2=finalResultsSet2.union(SingleLSHTableResults)
        }
        for (i <- 0 until HashTableInit.vectorDatabase_red.length){
          val SingleLSHTableResults=HashTableInit.vectorDatabase_red(i).getSimilar(queryKey,3).toArray().toSet
          finalResultsSet3=finalResultsSet3.union(SingleLSHTableResults)
        }

        finalResultsSet1.union(finalResultsSet2).union(finalResultsSet3)
//        println(finalR)
      }catch{
        case ex:NullPointerException => println("need to fit the data first")
          null
      }
    }

  def queryMultiFeatureBatch(queryArray:Array[Int]):Array[Set[AnyRef]]={
    val resultsArray:ArrayBuffer[Set[AnyRef]]=ArrayBuffer.empty[Set[AnyRef]]
    for( i<- queryArray.indices){
      resultsArray += multiFeatureSingleQuery(queryArray(i))
    }
    resultsArray.toArray
  }



  /**
    * fit one SparseVector into indexing structure
    * @param vector
    */
  private def fit(vector:SparseVector):Unit={
    HashTableInit.vectorIdToVector.put(HashTableInit.vectorDatabase.size,vector)
    for ( i <-0 until this.tableNum){
      HashTableInit.vectorDatabase(i).put(HashTableInit.vectorDatabase(i).size(),true)
    }
  }
//  /**
//    * fit the vector into the Indexing structure
//    * @param filename
//    * @param conf
//    */
//  def fit(filename:String,conf:Config):Unit={
//    if(LSHServer.lshEngine == null){ LSHServer.lshEngine = new LSH(conf)}
//    val (vectorArray,numbers) =getSparseVectorsFromFile(filename)
//    HashTableInit.initializeActorBasedHashTree(conf)
//    //generate dataTable
//    for ( i <- 0 until numbers){
//      HashTableInit.vectorIdToVector.put(i,vectorArray(i))
//    }
////    vectorArray.map( x => HashTableInit.vectorIdToVector.put(x.vectorId,x))
//    //get the LSHTable number
////    val tableNum = conf.getInt("cpslab.lsh.tableNum")
//    //put the data into hashTables
//    for ( i <- 0 until this.tableNum){
//      for ( j <- vectorArray.indices){
//        HashTableInit.vectorDatabase(i).put(j,true)
//      }
//    }
//  }
  /**
    * Get the sparse vector from datafile,
    * @param fileName: the filename
    * @return Array[SparseVector] and the number of sparse vectors
    */
  private def getSparseVectorsFromFile(fileName: String): (Array[SparseVector],Int) = {
    val AllSparseVectorsFile = getClass.getClassLoader.getResource(fileName).getFile
    val AllSparseVectors = new ListBuffer[(Int, Int, Array[Int], Array[Double])]
    for (line <- Source.fromFile(AllSparseVectorsFile).getLines()) {
      AllSparseVectors += Vectors.fromPythonString(line)
    }
    val length=AllSparseVectors.length
    val videoVectors: Array[SparseVector] = new Array[SparseVector](length)
    for (i <- 0 until length) {
      val SingleVideoVector = new SparseVector(AllSparseVectors(i)._1, AllSparseVectors(i)._2,
        AllSparseVectors(i)._3, AllSparseVectors(i)._4)
      videoVectors(i) = SingleVideoVector
      if (i %10000==0){ println(i + " objects loaded")}
    }
    println("Finish read all the line in file:" + fileName)
    println("Totally " + length)
    (videoVectors,length)
  }
  def newFastFit(fileName:String,conf:Config):Array[Array[Double]]={
    if(LSHServer.lshEngine == null){ LSHServer.lshEngine = new LSH(conf)}
    HashTableInit.initializeMapDBHashMap(conf)
    println("finish initialize the hash tree.")
    val AllSparseVectorsFile = getClass.getClassLoader.getResource(fileName).getFile
    val allDenseVectors = new ListBuffer[Array[Double]]
    var count=0
    for(line <- Source.fromFile(AllSparseVectorsFile).getLines()){
      val tmp=Vectors.fromPythonString(line)
      val currentSparseVector=new SparseVector(tmp._1,tmp._2,tmp._3,tmp._4)
      allDenseVectors += tmp._4
      HashTableInit.vectorIdToVector.put(count,currentSparseVector)
//      println(HashTableInit.vectorIdToVector.get(count))
      for(tableID <- 0 until this.tableNum*permutationNum){
//        println(HashTableInit.vectorDatabase(tableID).hasher)
//        println("nothing happen?")
        HashTableInit.vectorDatabase(tableID).put(count,true)
      }
      count += 1
      if (count % 100 == 0){ println(count + " objects loaded")}
    }
    println("finish load, totally " + count + " objects.")
    allDenseVectors.toArray
  }
  def newMultiFastFit(fileName1:String,fileName2:String,fileName3:String,
                      conf:Config):Unit={
    if(LSHServer.lshEngine == null){ LSHServer.lshEngine = new LSH(conf)}
    HashTableInit.initializeMapDBHashMultiple(conf)
    println("finish initialize the hash tree.")
    val AllSparseVectorsFile1 = getClass.getClassLoader.getResource(fileName1).getFile
    var count=0
    for(line <- Source.fromFile(AllSparseVectorsFile1).getLines()){
      val tmp=Vectors.fromPythonString(line)
      val currentSparseVector=new SparseVector(tmp._1,tmp._2,tmp._3,tmp._4)
//      allDenseVectors += tmp._4
      HashTableInit.vectorIdToVector_blue.put(count,currentSparseVector)
      for(tableID <- 0 until this.tableNum*permutationNum){
        HashTableInit.vectorDatabase_blue(tableID).put(count,true,1)
      }
      count += 1
      if (count % 100 == 0){ println(count + " objects loaded")}
    }
    println("finish blue load, totally " + count + " objects.")
    count=0
    val AllSparseVectorsFile2 = getClass.getClassLoader.getResource(fileName2).getFile
    for(line <- Source.fromFile(AllSparseVectorsFile2).getLines()){
      val tmp=Vectors.fromPythonString(line)
      val currentSparseVector=new SparseVector(tmp._1,tmp._2,tmp._3,tmp._4)
      //      allDenseVectors += tmp._4
      HashTableInit.vectorIdToVector_green.put(count,currentSparseVector)
      for(tableID <- 0 until this.tableNum*permutationNum){
        HashTableInit.vectorDatabase_green(tableID).put(count,true,2)
      }
      count += 1
      if (count % 100 == 0){ println(count + " objects loaded")}
    }
    println("finish green load, totally " + count + " objects.")
    count=0
    val AllSparseVectorsFile3 = getClass.getClassLoader.getResource(fileName3).getFile
    for(line <- Source.fromFile(AllSparseVectorsFile3).getLines()){
      val tmp=Vectors.fromPythonString(line)
      val currentSparseVector=new SparseVector(tmp._1,tmp._2,tmp._3,tmp._4)
      //      allDenseVectors += tmp._4
      HashTableInit.vectorIdToVector_red.put(count,currentSparseVector)
      for(tableID <- 0 until this.tableNum*permutationNum){
        HashTableInit.vectorDatabase_red(tableID).put(count,true,3)
      }
      count += 1
      if (count % 100 == 0){ println(count + " objects loaded")}
    }
    println("finish green load, totally " + count + " objects.")
    println("finish load all features")
  }

  /**
    * new Multiple threads fit, to create the indexing faster.
    * @param fileName
    * @param conf
    * @param threadNum
    * @return
    */
  def newMultiThreadFit(fileName:String,conf:Config,threadNum:Int=5):Array[Array[Double]]={
    if(LSHServer.lshEngine==null) LSHServer.lshEngine=new LSH(conf)
    HashTableInit.initializeMapDBHashMap(conf)
    val threadPool:ExecutorService=Executors.newFixedThreadPool(threadNum)
    val AllSparseVectorsFile = getClass.getClassLoader.getResource(fileName).getFile
    val allDenseVectors = new ListBuffer[Array[Double]]
    var count=0
    var terminateFlag=false
    val a = System.currentTimeMillis()
    for (line <- Source.fromFile(AllSparseVectorsFile).getLines()){
      val tmp=Vectors.fromPythonString(line)
      val currentSparseVector=new SparseVector(tmp._1,tmp._2,tmp._3,tmp._4)
      allDenseVectors += tmp._4
      this.vectorIdToVector.put(count,currentSparseVector)
      try{
        for(i <- 0 until threadNum){
          threadPool.execute(new threadFit(count,currentSparseVector,i*this.tableNum*this.permutationNum/threadNum,(i + 1)*this.tableNum*this.permutationNum/threadNum))
        }
      }
      count += 1
      if (count % 10000 == 0){ println(count + " objects loaded")}
    }
    val b = System.currentTimeMillis()
    if(!threadPool.isShutdown) {
      threadPool.shutdown()
      terminateFlag=true
    }

    print("time is " + (b-a))
    allDenseVectors.toArray
  }
  private class threadFit(count:Int,vector:SparseVector,start:Int,end:Int) extends Runnable {
    override def run(): Unit = {
      for(tableID <- start until end){
        HashTableInit.vectorDatabase(tableID).put(count,true)
      }
    }
  }

  class ThreadDemo(arr:Array[Int],start:Int,end:Int) extends Runnable{
    override def run(): Unit = {
      var result=0
      for(i <- start until end){
        result += arr(i)
      }
      println(result)
    }
  }

  /**
    * 对每次要put的值，我put两个值，但是这两个值的原有的hashvalue改变了
    */
  def newSamplingPut():Unit={

  }


}





