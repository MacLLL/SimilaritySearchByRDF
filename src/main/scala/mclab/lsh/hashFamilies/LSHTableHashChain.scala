package mclab.lsh.hashFamilies

import mclab.lsh.vector.{DenseVector, SparseVector}


/**
 * the class implementing the functions chaining in one of the hash tables
 * @param chainLength the number of hash functions
 * @param chainedHashFunctions the parameter setup for one set of the functions
 * @tparam T type of parameter set
 */
private[lsh] abstract class LSHTableHashChain[+T <: LSHFunctionParameterSet](
    private[lsh] val chainLength: Int,
    private[lsh] val chainedHashFunctions: List[T]) extends Serializable {

  /**
   * calculate the index of the vector in the hash table corresponding to the set of functions 
   * defined in this class
   * @param vector the vector to be indexed
   * @return the index of the vector
   */
  def compute(vector: SparseVector): Int

  def compute(vector: DenseVector) :Int
  
}
