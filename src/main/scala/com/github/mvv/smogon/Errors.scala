package com.github.mvv.smogon

class SmogonException(message: String, cause: Throwable)
             extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
  def this(cause: Throwable) = this(cause.getMessage, cause)
}
final class DuplicateKeyException extends SmogonException("Duplicate key")
