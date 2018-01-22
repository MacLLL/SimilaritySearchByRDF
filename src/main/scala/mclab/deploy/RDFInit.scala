package mclab.deploy

import com.typesafe.config.Config
import mclab.mapdb.RandomDrawTreeMap

/**
  * initialize the (dataTable, hashTable)
  * implement the put get method
  */
private[mclab] object RDFInit {
  private var createFlag:Boolean=false

  //inital definination for HashTable
  private def setupTable(tableName: String, confInstance: Config,
                         table: RandomDrawTreeMap[_, _]): Unit = {
    table.BUCKET_OVERFLOW = confInstance.getInt(s"mclab.$tableName.bufferOverflow")
    table.updateBucketLength(confInstance.getInt(s"mclab.$tableName.bucketBits"))
    table.updateDirectoryNodeSize(confInstance.getInt(s"cpslab.$tableName.dirNodeSize"),
      confInstance.getInt(s"cpslab.$tableName.chainLength"))
  }

}
