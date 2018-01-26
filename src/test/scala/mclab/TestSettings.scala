package mclab

import com.typesafe.config.ConfigFactory

private[mclab] object TestSettings {
  private val appConf = ConfigFactory.parseString(
    s"""
       |mclab.lsh.name = angle
       |mclab.lsh.generateByPulling = true
       |mclab.lsh.IsOrthogonal = true
       |mclab.lsh.generateMethod = default
       |mclab.lsh.familyFilePath = "hashFamily/bestHashFamily-angle"
       |mclab.lsh.family.pstable.mu = 0.0
       |mclab.lsh.family.pstable.sigma = 1.0
       |mclab.lsh.family.pstable.w = 4
       |
       |mclab.lsh.familySize = 100
       |mclab.lsh.vectorDim = 100
       |mclab.lsh.tableNum = 10
       |mclab.lsh.permutationNum = 1
       |mclab.lsh.seed1 = 31258
       |mclab.lsh.seed2 = 24872
       |mclab.lsh.seed3 = 83752
       |mclab.lsh.typeOfIndex = original
       |
       |mclab.lshTable.bufferOverflow=500
       |mclab.dataTable.bufferOverflow=500
       |mclab.lshTable.bucketBits=28
       |mclab.dataTable.bucketBits=28
       |mclab.lshTable.dirNodeSize=32
       |mclab.dataTable.dirNodeSize=32
       |mclab.lshTable.chainLength = 32
       |mclab.dataTable.chainLength = 32
       |mclab.dataTable.numPartitions=1
       |mclab.lsh.partitionBits=4
       |mclab.lsh.partitionBitsGenerateMethod="default"
       |
       |mclab.lsh.ramThreshold=2147483647
       |mclab.lsh.workingDirRoot="PersistIndex"
       |mclab.insertThreadNum=10
       |mclab.queryThreadNum=5
       |mclab.lsh.topK = 10
       |
       |
       |
       |
       |cpslab.lsh.plsh.benchmark.expDuration=0
       |cpslab.lsh.benchmark.replica=1
       |cpslab.lsh.benchmark.expDuration=30000
       |cpslab.lsh.benchmark.offset=0
       |cpslab.lsh.benchmark.cap=1000000
       |cpslab.vectorDatabase.memoryModel=offheap

       |cpslab.lsh.similarityThreshold = 0.0

       |
       |cpslab.lsh.plsh.updateWindowSize = 10
       |cpslab.lsh.plsh.partitionSwitch=false
       |cpslab.lsh.plsh.maxNumberOfVector=1000000

       |cpslab.lsh.initVectorNumber=0
       |cpslab.lsh.sharding.initParallism=1
       |cpslab.lsh.plsh.maxWorkerNum=5
       |cpslab.lsh.concurrentCollectionType=Doraemon

       |cpslab.lsh.inputFilePath=""

       |cpslab.lsh.deploy.client = "/user/client"
       |
       |cpslab.lsh.nodeID = 0

       |cpslab.lsh.deploy.maxNodeNum=100
       |
       """.stripMargin)

  private val akkaConf = ConfigFactory.parseString(
    """
      |akka.loglevel = "INFO"
      |akka.remote.netty.tcp.port = 0
      |akka.remote.netty.tcp.hostname = "127.0.0.1"
      |akka.cluster.roles = [compute]
      |akka.actor.provider = "akka.cluster.ClusterActorRefProvider"
      |akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
      |akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot-store.local"
    """.stripMargin)

  private val shardingConf = ConfigFactory.parseString(
    """
      |cpslab.lsh.distributedSchema = SHARDING
      |cpslab.lsh.sharding.maxShardNumPerTable = 100
      |cpslab.lsh.sharding.maxShardDatabaseWorkerNum = 1
      |cpslab.lsh.sharding.loadBatchingDuration = 0
      |cpslab.lsh.sharding.maxDatabaseNodeNum = 1
      |cpslab.lsh.writerActorNum = 10
      |cpslab.lsh.sharding.loadBatchingDuration = 0
      |cpslab.lsh.clientAddress="akka.tcp://FlatShardingSystem@127.0.0.1:2553/user/client"
      |cpslab.lsh.nodeID = 0
      |cpslab.vectorDatabase.asyncDelay=0
      |cpslab.vectorDatabase.asyncQueueSize=0
    """.stripMargin)

  private val clientConf = ConfigFactory.parseString(
    """
      |cpslab.lsh.plsh.benchmark.inputSource=""
      |cpslab.lsh.plsh.benchmark.remoteProxyList=["akka.tcp://LSH@127.0.0.1:3000/user/clientRequestHandler"]
      |cpslab.lsh.plsh.workerList=["akka.tcp://LSH@127.0.0.1:3000/user/PLSHWorker", "akka.tcp://LSH@127.0.0.1:3001/user/PLSHWorker"]
      |cpslab.lsh.plsh.benchmark.messageInterval=200
    """.stripMargin)

  val testBaseConf = appConf.withFallback(akkaConf)
  val testShardingConf = appConf.withFallback(akkaConf).withFallback(shardingConf)
  val testClientConf = appConf.withFallback(akkaConf).withFallback(clientConf)
}
