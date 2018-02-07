package mclab

import com.typesafe.config.ConfigFactory

private[mclab] object TestSettings {
  private val appConf = ConfigFactory.parseString(
    s"""
       |mclab.confType=lsh
       |mclab.lsh.name = angle
       |mclab.lsh.generateByPulling = true
       |mclab.lsh.IsOrthogonal = true
       |mclab.lsh.generateMethod = default
       |mclab.lsh.familyFilePath = "hashFamily/lsh-bestHashFamily-angle-TableNum-10"
       |mclab.lsh.partitionFamilyFilePath="hashFamily/partition-bestHashFamily-angle"
       |mclab.lsh.family.pstable.mu = 0.0
       |mclab.lsh.family.pstable.sigma = 1.0
       |mclab.lsh.family.pstable.w = 4
       |
       |mclab.lsh.familySize = 100
       |mclab.lsh.vectorDim = 100
       |mclab.lsh.tableNum = 20
       |mclab.lsh.permutationNum = 1
       |mclab.lsh.seed1 = 31258
       |mclab.lsh.seed2 = 24872
       |mclab.lsh.seed3 = 83752
       |mclab.lsh.typeOfIndex = original
       |mclab.lsh.featureDataFormat = sparse
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
       |mclab.lsh.partitionBits=1
       |mclab.lsh.partitionBitsGenerateMethod="default"
       |
       |mclab.lsh.ramThreshold=2147483647
       |mclab.lsh.workingDirRoot="PersistIndex"
       |mclab.insertThreadNum=20
       |mclab.queryThreadNum=20
       |mclab.lsh.topK = 10
       |
       |
       |mclab.lsh.plsh.benchmark.expDuration=0
       |mclab.lsh.benchmark.replica=1
       |mclab.lsh.benchmark.expDuration=30000
       |mclab.lsh.benchmark.offset=0
       |mclab.lsh.benchmark.cap=1000000
       |mclab.vectorDatabase.memoryModel=offheap

       |mclab.lsh.similarityThreshold = 0.0
       |mclab.lsh.plsh.maxNumberOfVector=1000000
       |mc.lsh.inputFilePath=""
       |mclab.lsh.nodeID = 0
       |mclab.lsh.deploy.maxNodeNum=100
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

  val testBaseConf = appConf.withFallback(akkaConf)
}
