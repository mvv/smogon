package com.github.mvv.smogon

class SmogonException(message: String, cause: Throwable)
      extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(cause.getMessage, cause)
}
final case class DuplicateKeyException(indexName: String)
                 extends SmogonException(
                           "Duplicate key for unique index '" + indexName + "'")
final case class UntranslatableQueryException[R <: Documents](filter: Filter[R])
                 extends SmogonException("Failed to translate query " + filter)
