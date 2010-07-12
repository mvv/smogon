/*
 * Copyright (C) 2010 Mikhail Vorozhtsov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.mvv.smogon

import java.util.Date
import org.bson.types.ObjectId
import com.mongodb.DBObject

sealed trait BsonType {
  def toBson: AnyRef
}

object BsonType {
  import scala.collection.JavaConversions._

  def default[T <: BsonType : ClassManifest](): T = (classManifest[T].erasure match {
    case c if c == classOf[BsonBool] => BsonBool.False
    case c if c == classOf[BsonInt] => BsonInt.Zero
    case c if c == classOf[BsonLong] => BsonLong.Zero
    case c if c == classOf[BsonDouble] => BsonDouble.Zero
    case c if c == classOf[BsonString] => BsonString.Empty
    case c if c == classOf[BsonDate] => BsonDate(new Date)
    case c if c == classOf[BsonId] => BsonId(null)
    case c if c == classOf[BsonArray] => BsonArray.Empty
    case c if c == classOf[BsonObject] => BsonObject.Empty
    case _ => BsonNull
  }).asInstanceOf[T]

  def fromRaw(value: AnyRef): BsonType = value match {
    case null => BsonNull
    case x: java.lang.Boolean => BsonBool(x.booleanValue)
    case x: java.lang.Integer => BsonInt(x.intValue)
    case x: java.lang.Long => BsonLong(x.longValue)
    case x: java.lang.Double => BsonDouble(x.doubleValue)
    case x: String => BsonString(x)
    case x: Date => BsonDate(x)
    case x: ObjectId => BsonId(x)
    case x: java.lang.Iterable[_] =>
      BsonArray(x.iterator.map(e => fromRaw(e.asInstanceOf[AnyRef])).toSeq)
    case x: DBObject =>
      BsonObject(x.keySet.view.map { k =>
                   val v = x.get(k)
                   (k, fromRaw(v))
                 } toMap)
  }
}

sealed trait OptBsonType extends BsonType
sealed trait MandatoryBsonType extends BsonType

sealed trait BasicBsonType extends BsonType {
  val value: Any
  def toBson = value.asInstanceOf[AnyRef]
}
sealed trait OptBsonBool extends BasicBsonType with OptBsonType
object OptBsonBool {
  implicit def booleanToOptBsonBool(x: java.lang.Boolean) =
    if (x == null) BsonNull else BsonBool(x.booleanValue)
  implicit def optBsonBoolToBoolean(x: OptBsonBool): java.lang.Boolean = x match {
    case BsonNull => null
    case BsonBool(x) => new java.lang.Boolean(x)
  }
}
final class BsonBool private(val value: Boolean) extends OptBsonBool
                                                    with MandatoryBsonType
object BsonBool {
  val True = new BsonBool(true)
  val False = new BsonBool(false)
  def apply(value: Boolean) = if (value) True else False
  def unapply(x: Any): Option[Boolean] = x match {
    case bson: BsonBool => Some(bson.value)
    case _ => None
  }
  implicit def booleanToBsonBool(x: Boolean) = BsonBool(x)
  implicit def bsonBoolToBoolean(x: BsonBool) = x.value
}
sealed trait IntegralBsonType extends BasicBsonType
sealed trait OptBsonInt extends IntegralBsonType with OptBsonType
object OptBsonInt {
  implicit def byteToOptBsonInt(x: java.lang.Byte): OptBsonInt =
    BsonInt(x.intValue)
  implicit def optBsonIntToByte(x: OptBsonInt) = x match {
    case BsonNull => null
    case BsonInt(x) => new java.lang.Byte(x.byteValue)
  }
  implicit def shortToOptBsonInt(x: java.lang.Short): OptBsonInt =
    BsonInt(x.intValue)
  implicit def optBsonIntToShort(x: OptBsonInt) = x match {
    case BsonNull => null
    case BsonInt(x) => new java.lang.Short(x.shortValue)
  }
  implicit def intToOptBsonInt(x: java.lang.Integer): OptBsonInt =
    BsonInt(x.intValue)
  implicit def optBsonIntToInt(x: OptBsonInt) = x match {
    case BsonNull => null
    case BsonInt(x) => new java.lang.Integer(x)
  }
}
final case class BsonInt(value: Int) extends OptBsonInt
                                        with MandatoryBsonType
object BsonInt {
  val Zero = BsonInt(0)
  implicit def byteToBsonInt(x: Byte) = BsonInt(x)
  implicit def bsonIntToByte(x: BsonInt) = x.value.byteValue
  implicit def shortToBsonInt(x: Short) = BsonInt(x)
  implicit def bsonIntToShort(x: BsonInt) = x.value.shortValue
  implicit def intToBsonInt(x: Int) = BsonInt(x)
  implicit def bsonIntToInt(x: BsonInt) = x.value
}
sealed trait OptBsonLong extends IntegralBsonType with OptBsonType
object OptBsonLong {
  implicit def byteToOptBsonLong(x: java.lang.Byte): OptBsonLong =
    BsonLong(x.longValue)
  implicit def optBsonLongToByte(x: OptBsonLong) = x match {
    case BsonNull => null
    case BsonLong(x) => new java.lang.Byte(x.byteValue)
  }
  implicit def shortToOptBsonLong(x: java.lang.Short): OptBsonLong =
    BsonLong(x.longValue)
  implicit def optBsonLongToShort(x: OptBsonLong) = x match {
    case BsonNull => null
    case BsonLong(x) => new java.lang.Short(x.shortValue)
  }
  implicit def intToOptBsonLong(x: java.lang.Integer): OptBsonLong =
    BsonLong(x.longValue)
  implicit def optBsonLongToInt(x: OptBsonLong) = x match {
    case BsonNull => null
    case BsonLong(x) => new java.lang.Integer(x.intValue)
  }
  implicit def longToOptBsonLong(x: java.lang.Long): OptBsonLong =
    BsonLong(x.longValue)
  implicit def optBsonLongToLong(x: OptBsonLong) = x match {
    case BsonNull => null
    case BsonLong(x) => new java.lang.Long(x)
  }
}
final case class BsonLong(value: Long) extends OptBsonLong
                                          with MandatoryBsonType
object BsonLong {
  val Zero = BsonLong(0)
  implicit def byteToBsonLong(x: Byte) = BsonLong(x)
  implicit def bsonLongToByte(x: BsonLong) = x.value.byteValue
  implicit def shortToBsonLong(x: Short) = BsonLong(x)
  implicit def bsonLongToShort(x: BsonLong) = x.value.shortValue
  implicit def intToBsonLong(x: Int) = BsonLong(x)
  implicit def bsonLongToInt(x: BsonLong) = x.value.intValue
  implicit def intToBsonLong(x: Long) = BsonLong(x)
  implicit def bsonLongToLong(x: BsonLong) = x.value
}
sealed trait OptBsonDouble extends BasicBsonType with OptBsonType
object OptBsonDouble {
  implicit def floatToOptBsonDouble(x: java.lang.Float): OptBsonDouble =
    BsonDouble(x.doubleValue)
  implicit def optBsonDoubleToFloat(x: OptBsonDouble) = x match {
    case BsonNull => null
    case BsonDouble(x) => new java.lang.Float(x.floatValue)
  }
  implicit def doubleToOptBsonDouble(x: java.lang.Double): OptBsonDouble =
    BsonDouble(x.doubleValue)
  implicit def optBsonDoubleToDouble(x: OptBsonDouble) = x match {
    case BsonNull => null
    case BsonDouble(x) => new java.lang.Double(x)
  }
}
final case class BsonDouble(value: Double) extends OptBsonDouble
                                              with MandatoryBsonType
object BsonDouble {
  val Zero = BsonDouble(0.0)
  implicit def floatToBsonDouble(x: Float) = BsonDouble(x)
  implicit def bsonDoubleToFloat(x: BsonDouble) = x.value.floatValue
  implicit def doubleToBsonDouble(x: Double) = BsonDouble(x)
  implicit def bsonDoubleToDouble(x: BsonDouble) = x.value
}
sealed trait OptBsonString extends BasicBsonType
object OptBsonString {
  implicit def stringToOptBsonString(x: String) =
    if (x == null) BsonNull else BsonString(x)
  implicit def optBsonStringToString(x: OptBsonString) = x match {
    case BsonNull => null
    case BsonString(x) => x
  }
}
final case class BsonString(value: String) extends OptBsonString
                                              with MandatoryBsonType
object BsonString {
  val Empty = BsonString("")
  implicit def stringToBsonString(x: String) = BsonString(x)
  implicit def bsonStringToString(x: BsonString) = x.value
}
sealed trait OptBsonDate extends BasicBsonType with OptBsonType
object OptBsonDate {
  implicit def dateToOptBsonDate(x: Date) =
    if (x == null) BsonNull else BsonDate(x)
  implicit def optBsonDateToDate(x: OptBsonDate) = x match {
    case BsonNull => null
    case BsonDate(x) => x
  }
}
final case class BsonDate(value: Date) extends OptBsonDate
                                          with MandatoryBsonType
object BsonDate {
  implicit def dateToBsonDate(x: Date) = BsonDate(x)
}
sealed trait OptBsonId extends BasicBsonType with OptBsonType
object OptBsonId {
  implicit def idToOptBsonId(x: ObjectId) =
    if (x == null) BsonNull else BsonId(x)
  implicit def optBsonIdToId(x: OptBsonId) = x match {
    case BsonNull => null
    case BsonId(x) => x
  }
}
final case class BsonId(value: ObjectId) extends OptBsonId
                                            with MandatoryBsonType
object BsonId {
  implicit def idToBsonId(x: ObjectId) = BsonId(x)
  implicit def bsonIdToId(x: BsonId) = x.value
}
sealed trait OptBsonArray extends BsonType
object OptBsonArray {
  implicit def seqToOptBsonArray(x: Seq[BsonType]) =
    if (x == null) BsonNull else BsonArray(x)
  implicit def optBsonArrayToSeq(x: OptBsonArray) = x match {
    case BsonNull => null
    case BsonArray(x) => x
  }
}
final case class BsonArray(value: Seq[BsonType])
                   extends OptBsonArray with MandatoryBsonType {
  import scala.collection.JavaConversions._
  def toBson = asList(value.map(_.toBson))
}
object BsonArray {
  val Empty = BsonArray(Seq())
  implicit def seqToBsonArray(x: Seq[BsonType]) = BsonArray(x)
  implicit def bsonArrayToSeq(x: BsonArray) = x.value
}
sealed trait OptBsonObject extends BsonType
object OptBsonObject {
  implicit def mapToOptBsonObject(x: Map[String, BsonType]) =
    if (x == null) BsonNull else BsonObject(x)
  implicit def optBsonObjectToMap(x: OptBsonObject) = x match {
    case BsonNull => null
    case BsonObject(x) => x
  }
}
final case class BsonObject(value: Map[String, BsonType])
                   extends OptBsonObject with MandatoryBsonType {
  import scala.collection.JavaConversions._
  def toBson = asMap(value.map { case (k, v) => (k, v.toBson) })
}
object BsonObject {
  val Empty = BsonObject(Map())
  implicit def mapToBsonObject(x: Map[String, BsonType]) = BsonObject(x)
  implicit def bsonObjectToMap(x: BsonObject) = x.value
}

object BsonNull extends BsonType
                    with OptBsonBool
                    with OptBsonInt
                    with OptBsonLong
                    with OptBsonDouble
                    with OptBsonString
                    with OptBsonDate
                    with OptBsonId
                    with OptBsonArray
                    with OptBsonObject {
  val value = null
}
