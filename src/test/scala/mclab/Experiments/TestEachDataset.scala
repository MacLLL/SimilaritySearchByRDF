package mclab.Experiments

import mclab.TestSettings
import mclab.deploy.{DenseTestInit, LSHServer}
import mclab.lsh.LSH
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import mclab.lsh.vector.{DenseVector, SparseVector, Vectors}

/**
  * Created by eternity on 2/21/18.
  */
class TestEachDataset extends FunSuite with BeforeAndAfterAll {

  test("test sift"){
    LSHServer.lshEngine = new LSH(TestSettings.testBaseConf)
    LSHServer.isUseDense= true
    val allDenseVectors = DenseTestInit.newMultiThreadFit("glove.twitter.27B/glove.twitter.27B.100d.20k.DenseVector.txt",
          TestSettings.testBaseConf)
    val step=0
    val dv=allDenseVectors.slice(0,2000)
    //    val timea=System.nanoTime()
        DenseTestInit.querySingleKey(0,dv,step)
    //    val timeb=System.nanoTime()
    //    println("Step= " + step + ", Single query time " + (timeb-timea)/1000000.0 + "ms")


  }

}
