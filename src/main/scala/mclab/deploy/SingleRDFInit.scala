package mclab.deploy

import com.typesafe.config.Config
import mclab.mapdb.RandomDrawTreeMap

/**
  * initialize the (dataTable, lshTable)
  * implement the put get method
  */
private[mclab] object SingleRDFInit {
  private var createFlag:Boolean=false

  /**
    * initialize definition for HashTable
    *
    * @param tableName tablename can be lshTable and dataTable
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
    * @param conf configuration
    */
  def initializeRDFHashMap(conf: Config): Unit = {
    createFlag=true
    val tableNum = conf.getInt("mclab.lsh.tableNum")
  }





}
