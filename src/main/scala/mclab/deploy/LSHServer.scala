package mclab.deploy

import mclab.lsh.LSH

private[mclab] object LSHServer {

  var lshEngine: LSH = null

  var isUseDense:Boolean = false

  def getLSHEngine = lshEngine

  def getisUseDense = isUseDense





}

