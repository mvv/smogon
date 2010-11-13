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
import java.util.regex.Pattern
import scala.util.matching.Regex
import scala.collection.JavaConversions._
import org.bson.types.ObjectId
import org.bson.BSONObject
import com.mongodb.DBObject
import com.github.mvv.layson
import layson.bson._
import layson.json._

final class StaticDocumentBsonObject[D <: StaticDocument](
              val docDef: D, docRepr: D#DocRepr) extends BsonObject {
  private val doc = docRepr.asInstanceOf[docDef.DocRepr]
  def get(key: String) = docDef.fieldsMap.get(key).map { field =>
    field.fieldBson(field.get(doc))
  }
  def iterator = docDef.staticFields.iterator.map { field =>
    field.fieldName -> field.fieldBson(field.get(doc))
  }
  def members = docDef.staticFields.view.map { field =>
    field.fieldName -> field.fieldBson(field.get(doc))
  }
  def membersMap = docDef.fieldsMap.mapValues { field =>
    field.fieldBson(field.get(doc))
  }
}

final class DynamicDocumentBsonObject[D <: DynamicDocument](
              val docDef: D, docRepr: D#DocRepr) extends BsonObject {
  private val doc = docRepr.asInstanceOf[docDef.DocRepr]
  def get(key: String) = docDef.stringToName(key).flatMap { name =>
    docDef.get(doc, name).map(v => docDef.field.fieldBson(v))
  }
  def iterator = docDef.fields(doc).map { case (n, v) =>
    docDef.nameToString(n) -> docDef.field.fieldBson(v)
  }
  def members = docDef.fieldsSeq(doc).view.map { case (n, v) =>
    docDef.nameToString(n) -> docDef.field.fieldBson(v)
  }
  def membersMap = {
    val submap = docDef.fieldsMap(doc).mapValues { v =>
      docDef.field.fieldBson(v)
    }
    new SuperMap[String, docDef.FieldName, BsonValue](
          submap, docDef.nameToString(_), docDef.stringToName(_))
  }
}

class BsonDBObject(obj: BsonObject) extends DBObject {
  def get(key: String): AnyRef = obj.get(key) match {
    case Some(v) => Bson.toRaw(v)
    case None =>
      if (key == "_transientFields") Nil
      else throw new NoSuchElementException(key)
  }
  def put(key: String, value: AnyRef) =
    throw new UnsupportedOperationException
  def putAll(map: java.util.Map[_, _]) =
    throw new UnsupportedOperationException
  def putAll(dbo: BSONObject) =
    throw new UnsupportedOperationException
  def removeField(key: String) =
    throw new UnsupportedOperationException
  def keySet = obj.membersMap.keySet
  def containsKey(key: String) = obj.get(key).isDefined
  def containsField(key: String) = obj.get(key).isDefined
  def toMap = obj.membersMap.map { case (k, v) => (k, Bson.toRaw(v)) }
  def isPartialObject = false
  def markAsPartialObject() {}
  override def toString = com.mongodb.util.JSON.serialize(this)
}

class DBBsonObject(dbo: DBObject)
      extends MapBsonObject(
                dbo.toMap.asInstanceOf[java.util.Map[String, AnyRef]].toMap.
                  mapValues(Bson.fromRaw(_)))

object Bson {
  def default[T <: BsonValue : ClassManifest](): T = (classManifest[T].erasure match {
    case c if c == classOf[BsonBool] => BsonBool.False
    case c if c == classOf[BsonInt] => BsonInt.Zero
    case c if c == classOf[BsonLong] => BsonLong.Zero
    case c if c == classOf[BsonDouble] => BsonDouble.Zero
    case c if classOf[BsonStr].isAssignableFrom(c) => BsonStr.Empty
    case c if c == classOf[BsonDate] => BsonDate.Min
    case c if c == classOf[BsonId] => BsonId.Zero
    case c if c == classOf[BsonRegex] => BsonRegex.Any
    case c if classOf[BsonArray].isAssignableFrom(c) => BsonArray.Empty
    case c if classOf[BsonObject].isAssignableFrom(c) => BsonObject.Empty
    case _ => BsonNull
  }).asInstanceOf[T]

  def toDBObject(obj: BsonObject): DBObject = new BsonDBObject(obj)
  def fromDBObject(dbo: DBObject): BsonObject =
    new MapBsonObject(
          dbo.toMap.asInstanceOf[java.util.Map[String, AnyRef]].toMap.
            mapValues(fromRaw(_)))
  def fromRaw(value: AnyRef): BsonValue = value match {
    case null => BsonNull
    case x: java.lang.Boolean => BsonBool(x.booleanValue)
    case x: java.lang.Integer => BsonInt(x.intValue)
    case x: java.lang.Long => BsonLong(x.longValue)
    case x: java.lang.Double => BsonDouble(x.doubleValue)
    case x: String => BsonStr(x)
    case x: Date => BsonDate(x)
    case x: ObjectId => BsonId(x._time, x._machine, x._inc)
    case x: Pattern => BsonRegex(new Regex(x.toString))
    case x: java.lang.Iterable[_] =>
      BsonArray(x.view.map(e => fromRaw(e.asInstanceOf[AnyRef])).toSeq: _*)
    case x: DBObject => fromDBObject(x)
  }
  def toRaw(value: BsonValue): AnyRef = value match {
    case BsonNull => null
    case BsonBool(x) => java.lang.Boolean.valueOf(x)
    case BsonInt(x) => java.lang.Integer.valueOf(x)
    case BsonLong(x) => java.lang.Long.valueOf(x)
    case BsonDouble(x) => java.lang.Double.valueOf(x)
    case BsonStr(x) => x
    case BsonDate(x) => x
    case BsonId(time, machine, increment) =>
      new ObjectId(time, machine, increment)
    case BsonRegex(x) => x.pattern
    case BsonArray(x) => asJavaIterable(x.map(toRaw)) 
    case x: BsonObject => toDBObject(x)
    case _ => null
  }

  def fromJson[B <: BsonValue](
        value: JsonValue, bsonClass: Class[B]): B = ((value, bsonClass) match {
    case (JsonNum(x), c) if c.isAssignableFrom(classOf[BsonInt]) =>
      if (x.scale != 0)
        throw new IllegalArgumentException
      BsonInt(x.intValue)
    case (JsonNum(x), c) if c.isAssignableFrom(classOf[BsonLong]) =>
      if (x.scale != 0)
        throw new IllegalArgumentException
      BsonLong(x.longValue)
    case (JsonNum(x), c) if c.isAssignableFrom(classOf[BsonDouble]) =>
      BsonDouble(x.doubleValue)
    case (JsonStr(BsonIdStr(x)), c) if c.isAssignableFrom(classOf[BsonId]) =>
      x
    case (value, c) =>
      val bson = value.toBson
      if (!c.isAssignableFrom(bson.getClass))
        throw new IllegalArgumentException
      bson
  }).asInstanceOf[B]

  def toJson(value: BsonValue): JsonValue = value match {
    case BsonBool(x) => JsonBool(x)
    case BsonInt(x) => JsonNum(x)
    case BsonLong(x) => JsonNum(x)
    case BsonDouble(x) => JsonNum(x)
    case BsonStr(x) => JsonStr(x)
    case BsonDate(x) => JsonNum(x.getTime)
    case x: BsonId => JsonStr(x.toString)
    case BsonRegex(x) => JsonStr(x.toString)
    case x: BsonArray => JsonArray(x.iterator.map(toJson(_)))
    case x: BsonObject =>
      JsonObject(x.iterator.map { case (k, v) => k -> toJson(v) })
    case _ => JsonNull
  }
}
