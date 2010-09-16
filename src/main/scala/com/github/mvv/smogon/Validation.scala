package com.github.mvv.smogon

import scala.util.matching.Regex
import java.net.URI

object Validation {
  sealed trait Result {
    def ++(other: Result): Result
    def errors: Seq[String]
  }
  object Valid extends Result {
    def ++(other: Result) = other
    def errors = Vector.empty
  }
  final case class Invalid(customErrors: String*) extends Result {
    def ++(other: Result) = other match {
      case Valid => this
      case other: Invalid => Invalid((customErrors ++ other.customErrors): _*)
    }
    def errors = if (customErrors.isEmpty) Vector("Illegal value")
                 else customErrors
  }
}

trait Validator[-T] extends (T => Validation.Result) {
  import Validator._

  final def &&[T1 <: T](other: Validator[T1]) = And[T1](this, other)
  final def ||[T1 <: T](other: Validator[T1]) = Or[T1](this, other)
}

object Validator {
  import Validation._

  final case class And[-T](first: Validator[T], second: Validator[T])
                   extends Validator[T] {
    def apply(value: T) = first(value) ++ second(value)
  }
  final case class Or[-T](first: Validator[T], second: Validator[T])
                   extends Validator[T] {
    def apply(value: T) = first(value) match {
      case Valid => Valid
      case _ => second(value)
    }
  }

  object Ok extends Validator[Any] {
    def apply(value: Any) = Valid
  }
  final case class Min[T : Ordering](bound: T) extends Validator[T] {
    def apply(value: T) = if (implicitly[Ordering[T]].gteq(value, bound))
                            Valid
                          else
                            Invalid("Less than " + bound)
  }
  final case class Max[T : Ordering](bound: T) extends Validator[T] {
    def apply(value: T) = if (implicitly[Ordering[T]].lteq(value, bound))
                            Valid
                          else
                            Invalid("Greater than " + bound)
  }
  final case class In[T : Ordering](min: T, max: T) extends Validator[T] {
    def apply(value: T) = {
      val ord = implicitly[Ordering[T]]
      if (ord.gteq(value, min) && ord.lteq(value, max))
        Valid
      else
        Invalid("Not in [" + min + ", " + max + "]")
    }
  }
  final case class Length(min: Int, max: Int) extends Validator[String] {
    require(min >= 0)
    require(max >= 0)
    require(min <= max)

    def apply(value: String) = {
      val len = value.length
      if (len < min)
        Invalid("Length is less than " + min)
      else if (len > max)
        Invalid("Length is greater than " + max)
      else
        Valid
    }
  }
  def MinLength(min: Int) = Length(min, Int.MaxValue)
  def MaxLength(max: Int) = Length(0, max)
  val NotEmpty = MinLength(1)
  final case class Matches(regex: Regex) extends Validator[String] {
    def apply(value: String) = if (regex.pattern.matcher(value).matches)
                                 Valid
                               else
                                 Invalid("Doesn't match regex " + regex)
  }
  object NotBlank extends Validator[String] {
    def apply(value: String) = if (value.forall(_.isSpaceChar))
                                 Invalid("Is blank")
                               else
                                 Valid
  }
  object Trimmed extends Validator[String] {
    def apply(value: String) = {
      val len = value.length
      if (len > 0) {
        if (value(0).isSpaceChar)
          Invalid("Begins with a space character")
        else if (value(len - 1).isSpaceChar)
          Invalid("Ends with a space character")
        else
          Valid
      } else
        Valid
    }
  }
  sealed case class IsURI(schemas: String*) extends Validator[String] {
    def apply(value: String) =
      try {
        val uri = new URI(value)
        if (schemas.isEmpty || schemas.contains(uri.getScheme))
          Valid
        else
          Invalid("Scheme is not one of (" + schemas.mkString(", ") + ")")
      } catch {
        case e: Exception =>
          Invalid("Not a URI: " + e.getMessage)
      }
  }

  implicit def functionToValidator[T](fn: T => Result) = new Validator[T] {
    def apply(value: T) = fn(value)
  }
  implicit def boolFunctionToValidator[T](fn: T => Boolean) = new Validator[T] {
    def apply(value: T) = if(fn(value)) Valid else Invalid()
  }
}
