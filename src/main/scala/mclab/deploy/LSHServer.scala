package mclab.deploy

import mclab.lsh.LSH

private[mclab] object LSHServer {

  var lshEngine: LSH = null

  def getLSHEngine = lshEngine
}

