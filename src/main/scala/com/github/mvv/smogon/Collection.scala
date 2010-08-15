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

import scala.collection.generic.{
         SeqFactory, MapFactory, GenericTraversableTemplate}
import scala.collection.MapLike
import scala.util.matching.Regex
import com.github.mvv.layson
import layson.bson._
import layson.json._
import org.bson.BSONObject
import org.bson.types.ObjectId
import com.mongodb.{
         DBObject, BasicDBObject, DBCollection, WriteConcern, WriteResult,
         MongoException}

trait ReprBsonValue {
  type ValueRepr
  type Bson <: BsonValue

  def fromBson(bson: Bson): ValueRepr
  def toBson(repr: ValueRepr): Bson
}

sealed trait Documents extends Document {
  final type Root = this.type
}

sealed trait HasName {
  val fieldName: String
  val fieldRootName: String
  val fieldFullName: String
}

sealed trait Projection[C <: Collection] {
  def projectedFields: Set[(F, F#Doc#Coll =:= C) forSome {
                             type D <: Document
                             type F <: D#FieldBase
                           }]
  def projectionBson(include: Boolean): DBObject = {
    val value = if (include) 1 else 0
    val bson = new BasicDBObject
    projectedFields.foreach { case (f, _) =>
      bson.put(f.fieldFullName, value)
    }
    bson
  }
  def *(proj: Projection[C]): Projection[C] =
    new Projection.Many[C](projectedFields ++ proj.projectedFields)
}

object Projection {
  final class Many[C <: Collection](
                val projectedFields: Set[(F, F#Doc#Coll =:= C) forSome {
                                           type D <: Document
                                           type F <: D#FieldBase
                                         }]) extends Projection[C]
}

sealed trait Sort[DS <: Documents] {
  def sortByFields: Seq[(F, Boolean, F#Doc#Root =:= DS) forSome {
                          type D <: Document
                          type F <: D#FieldBase
                        }]
  def sortBson: DBObject = {
    val bson = new BasicDBObject
    sortByFields.foreach { case (f, asc, _) =>
      bson.put(f.fieldRootName, if (asc) 1 else -1)
    }
    bson
  }
  def &(sort: Sort[DS]): Sort[DS] =
    new Sort.Many[DS](sortByFields ++ sort.sortByFields)
}

object Sort {
  final class Many[DS <: Documents](
                val sortByFields: Seq[(F, Boolean, F#Doc#Root =:= DS) forSome {
                                        type D <: Document
                                        type F <: D#FieldBase
                                      }]) extends Sort[DS]
}

object Document {
  val objectNamePat = ".*\\$([a-zA-Z_][a-zA-Z0-9_]*)\\$".r.pattern
}

object Field {
  def unapply[D <: Document](x: D#FieldBase): Option[D#FieldBase] =
    Some(x)
}
object BasicField {
  def unapply[D <: Document](x: D#BasicFieldBase): Option[D#BasicFieldBase] =
    Some(x)
}
object EmbeddingField {
  def unapply[D <: Document](
        x: D#EmbeddingFieldBase): Option[D#EmbeddingFieldBase] =
    Some(x)
}
object OptEmbeddingField {
  def unapply[D <: Document](
        x: D#OptEmbeddingFieldBase): Option[D#OptEmbeddingFieldBase] =
    Some(x)
}
object DocumentsArrayField {
  def unapply[D <: Document](
        x: D#DocumentsArrayFieldBase): Option[D#DocumentsArrayFieldBase] =
    Some(x)
}

trait Document { document =>
  import Document._

  type Coll <: Collection
  type Root <: Documents
  type DocRepr

  sealed trait FieldBase extends HasName
                            with Projection[Coll]
                            with Sort[Root]
                            with JsonSpec.DocumentField[document.type] { field =>
    final type Doc = document.type
    type Repr

    val fieldIndex: Int
    def fieldDocument: Doc = document
    def fieldBson(value: Repr): BsonValue

    def default: Repr

    def get(repr: DocRepr): Repr
    def set(repr: DocRepr, value: Repr): DocRepr

    def |(field: FieldBase) = this

    def projectedFields = {
      val e = (this, implicitly[Doc#Coll =:= Coll])
      Set(e)
    }

    def sortByFields = {
      val e = (this, true, implicitly[Doc#Root =:= Root])
      Seq(e)
    }
    def asc = this
    def desc = new Sort[Root] {
      def sortByFields = field.sortByFields.map {
        case (f, _, w) => (f, false, w)
      }
    }

    def ===(value: Repr) = Filter.Eq[Doc, this.type](this, value)
    def !==(value: Repr) = !(this === value)
    def in(values: Set[Repr]): Filter.In[Doc, this.type] =
      Filter.In[Doc, this.type](this, values)
    def in(values: Repr*): Filter.In[Doc, this.type] = in(values.toSet)
    def notIn(values: Set[Repr]) = !(this in values)
    def notIn(values: Repr*) = !(this in values.toSet)

    def =#(value: Repr) = Update.SetTo[Doc, this.type](this, value)

    final def as(name: String) = JsonSpec.Renamed[Doc, this.type](name, this)
    final def toOpt(conv: Repr => Option[JsonValue]) =
      JsonSpec.ConvTo[Doc, this.type](fieldName, this, conv)
    final def to(conv: Repr => JsonValue) = toOpt(r => Some(conv(r)))
    final def from(conv: PartialFunction[JsonValue, Repr]) =
      JsonSpec.ConvFrom[Doc, this.type](fieldName, this, conv)
    final def const(value: Repr) =
      new JsonSpec.Const[Doc, this.type](this, value)
    final def eval(expr: => Repr) =
      new JsonSpec.Eval[Doc, this.type](this, expr)
  }

  private val fieldByName: scala.collection.mutable.Map[String, FieldBase] =
    new scala.collection.mutable.HashMap()
  private var fieldByIndex: Seq[FieldBase] = Vector.empty
  
  def field(name: String): Option[FieldBase] = fieldByName.get(name)
  def fields: Seq[FieldBase] = fieldByIndex

  sealed abstract class AbstractField(name: String) extends FieldBase {
    final val fieldName = name match {
      case null =>
        val m = objectNamePat.matcher(getClass.getSimpleName)
        val n = if (m.matches)
                  m.group(1)
                else
                  throw new IllegalArgumentException
        if (n == "id" && document.isInstanceOf[Collection]) "_id" else n
      case fn => fn
    }
    final val fieldRootName = document match {
      case _: Documents => fieldName
      case hn: HasName => hn.fieldRootName + '.' + fieldName
    }
    final val fieldFullName = document match {
      case _: Collection => fieldName 
      case hn: HasName => hn.fieldFullName + '.' + fieldName
    }
    final val fieldIndex = {
      if (fieldByName.contains(fieldName))
        throw new IllegalArgumentException(
                    "Field with name '" + fieldName + "' already exists")
      fieldByName += (fieldName -> this)
      fieldByIndex :+= this
      fieldByName.size - 1
    }
  }

  sealed trait BasicFieldBase extends FieldBase with ReprBsonValue {
    final type ValueRepr = Repr

    val bsonClass: Class[Bson]

    def fieldBson(value: Repr): Bson = toBson(value)

    def +=(value: Repr)(implicit witness: Bson <:< OptNumericBsonValue) =
      Update.Increment[Doc, this.type](this, value)
    def -=(value: Repr)(implicit witness: Bson <:< OptNumericBsonValue) =
      Update.Decrement[Doc, this.type](this, value)
  }

  object BasicFieldBase {
    final class BasicFieldOps[F <: BasicFieldBase] private[BasicFieldBase](
                  field: F)(implicit witness: F#Bson <:< OptSimpleBsonValue) {
      def <(value: F#Repr) = Filter.Less[F#Doc, F](field, value)
      def <=(value: F#Repr) = Filter.LessOrEq[F#Doc, F](field, value)
      def >(value: F#Repr) = !(this <= value)
      def >=(value: F#Repr) = !(this < value)
    }

    implicit def basicFieldOps[F <: BasicFieldBase](
                   field: F)(
                   implicit witness: F#Bson <:< OptSimpleBsonValue) =
      new BasicFieldOps(field)

    final class ModResultFilter[F <: BasicFieldBase] private[BasicFieldBase](
                  field: F, devisor: F#Repr)(
                  implicit witness: F#Bson <:< OptIntegralBsonValue) {
      def ===(result: F#Repr) = Filter.Mod[F#Doc, F](field, devisor, result)
      def !==(result: F#Repr) = !(this === result)
    }

    final class IntegralFieldOps[F <: BasicFieldBase] private[BasicFieldBase](
                  field: F)(
                  implicit witness: F#Bson <:< OptIntegralBsonValue) {
      def %(devisor: F#Repr) = new ModResultFilter(field, devisor)
    }

    implicit def integralFieldOps[F <: BasicFieldBase](
                   field: F)(
                   implicit witness: F#Bson <:< OptIntegralBsonValue) =
      new IntegralFieldOps(field)

    final class StringFieldOps[F <: BasicFieldBase] private[BasicFieldBase](
                  field: F)(implicit witness: F#Bson <:< OptBsonStr) {
      def =~(regex: Regex) = Filter.Match[F#Doc, F](field, regex)
      def !~(regex: Regex) = !(this =~ regex)
    }

    implicit def stringFieldOps[F <: BasicFieldBase](
                   field: F)(
                   implicit witness: F#Bson <:< OptBsonStr) =
      new StringFieldOps(field)
  }

  sealed trait EmbeddingFieldBase extends FieldBase with Document {
    final type Coll = document.Coll
    final type Root = document.Root
    final type DocRepr = Repr

    def default = create
    final def fieldBson(value: Repr): BsonObject =
      BsonObject(fields.map(f => f.fieldName -> f.fieldBson(f.get(value))).toMap)

    def enter[IO <: Direction](
          spec: this.type => JsonSpec[d.type, IO]
                               forSome { val d: this.type }) =
      new JsonSpec.InEmbedding[Doc, this.type, IO](
            jsonMember, this, spec(this).asInstanceOf[JsonSpec[this.type, IO]])
    def open[IO <: Direction](
          spec: this.type =>
                JsonSpec.NoDefault[d.type, IO]
                  forSome { val d: this.type }): JsonSpec.NoDefault[Doc, IO] =
      spec(this) match {
        case JsonSpec.Many(ss) =>
          JsonSpec.Many(
            ss.map { s =>
              s match {
                case JsonSpec.OptMember(s, alt) =>
                  val castedAlt = alt.asInstanceOf[Repr => Repr]
                  JsonSpec.OptMember[Doc](
                    JsonSpec.Lifted[Doc, this.type, In](
                      this, s.asInstanceOf[JsonSpec.Single[this.type, In]]),
                    dr => set(dr, castedAlt(get(dr)))).
                      asInstanceOf[JsonSpec.Single[Doc, IO]]
                case s =>
                  JsonSpec.Lifted[Doc, this.type, IO](
                    this, s.asInstanceOf[JsonSpec.Single[this.type, IO]])
              }
            })
        case JsonSpec.OptMember(s, alt) =>
          val castedAlt = alt.asInstanceOf[Repr => Repr]
          JsonSpec.OptMember[Doc](
            JsonSpec.Lifted[Doc, this.type, In](
              this, s.asInstanceOf[JsonSpec.Single[this.type, In]]),
            dr => set(dr, castedAlt(get(dr))))
        case s =>
          new JsonSpec.Lifted[Doc, this.type, IO](
                this, s.asInstanceOf[JsonSpec.Single[this.type, IO]])
      }
    }

  sealed trait OptEmbeddingFieldBase extends EmbeddingFieldBase {
    protected def nullDocRepr: DocRepr
    def isNull(repr: DocRepr): Boolean
    final override def default = nullDocRepr
  }

  sealed trait ArrayFieldBase extends FieldBase {
    type ElemRepr

    def newArrayRepr(): Repr
    def append(repr: Repr, value: ElemRepr): Repr
    def iterator(repr: Repr): Iterator[ElemRepr]

    def default = newArrayRepr
    def elementBson(elem: ElemRepr): BsonValue
    final def fieldBson(value: Repr) =
      BsonArray(iterator(value).map(elementBson(_)).toSeq: _*)

    final def size(n: Long) = Filter.Size[Doc, this.type](this, n)

    final def pushBack(elems: ElemRepr*) =
      Update.Push[Doc, this.type](this, elems)
    final def pull(elems: Set[ElemRepr]): Update.Pull[Doc, this.type] =
      Update.Pull[Doc, this.type](this, elems)
    final def pull(elems: ElemRepr*): Update.Pull[Doc, this.type] =
      pull(elems.toSet)
    final def popFront() = Update.Pop[Doc, this.type](this, true)
    final def popBack() = Update.Pop[Doc, this.type](this, false)
  }

  trait SeqArrayField[S[X] <: Seq[X] with GenericTraversableTemplate[X, S]]
          extends AbstractField with ArrayFieldBase {
    type Repr >: S[ElemRepr] <: Seq[ElemRepr]

    protected def seqFactory: SeqFactory[S]

    def newArrayRepr(): Repr = seqFactory.empty[ElemRepr]
    def append(repr: Repr, value: ElemRepr): Repr =
      seqFactory.concat(repr :+ value)
    def iterator(repr: Repr): Iterator[ElemRepr] = repr.iterator
  }

  trait MapArrayField[M[K, V] <: Map[K, V] with MapLike[K, V, M[K, V]]]
          extends AbstractField with ArrayFieldBase {
    type Key
    type Repr >: M[Key, ElemRepr] <: Map[Key, ElemRepr]

    protected def mapFactory: MapFactory[M]
    def elemKey(repr: ElemRepr): Key

    def newArrayRepr(): Repr = mapFactory.empty[Key, ElemRepr]
    def append(repr: Repr, value: ElemRepr): Repr =
      ((mapFactory.newBuilder[Key, ElemRepr] ++= repr) +=
       (elemKey(value) -> value)).result
    def iterator(repr: Repr): Iterator[ElemRepr] = repr.valuesIterator
  }

  sealed trait ElementsArrayFieldBase extends ArrayFieldBase with ReprBsonValue {
    final type ElemRepr = ValueRepr

    final def elementBson(elem: ElemRepr) = toBson(elem)

    final def contains(filter: ValueFilterBuilder[this.type] =>
                               ValueFilter[this.type]) =
      Filter.ContainsElem[Doc, this.type](
        this, filter(new ValueFilterBuilder[this.type]))
  }

  trait DocumentsArrayFieldBase extends ArrayFieldBase with Documents {
    final type Coll = document.Coll
    final type ElemRepr = DocRepr

    final def elementBson(elem: ElemRepr) = toBson(elem)

    final def contains(filter: this.type =>
                               Filter[this.type]): Filter[Doc#Root] =
      Filter.Contains[Doc, this.type](this, filter(this))

    final def foreach[IO <: Direction](
                spec: this.type => JsonSpec[d.type, IO]
                                     forSome { val d: this.type }) =
      JsonSpec.InDocuments[Doc, this.type, IO](
        jsonMember, this, spec(this).asInstanceOf[JsonSpec[this.type, IO]],
        identity)
    final def transform(
                trans: Iterator[DocRepr] => Iterator[DocRepr]) =
      new JsonSpec.Transformed[Doc, this.type](jsonMember, this, trans)
  }

  protected def newDocRepr(): DocRepr

  final def create = fields.foldLeft(newDocRepr){ (doc, field) =>
    field.set(doc, field.default)
  }

  final class DefaultDocRepr private[smogon]() {
    private val fields: Array[Any] = new Array[Any](fieldByName.size)

    def get[F <: FieldBase](field: F): F#Repr =
      fields(field.fieldIndex).asInstanceOf[F#Repr]
    def set[F <: FieldBase](field: F, value: F#Repr) = {
      fields(field.fieldIndex) = value
      this.asInstanceOf[DocRepr]
    }
  }

  sealed abstract class Field[R](
                          getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
                          name: String)
                        extends AbstractField(name) {
    type Repr = R

    final def get(doc: DocRepr) = getter(doc)
    final def set(doc: DocRepr, value: R) = setter(doc, value)
  }

  sealed abstract class FieldM[R](
                          getter: DocRepr => R, setter: (DocRepr, R) => Unit,
                          name: String)
                        extends Field[R](getter, (d, r) => { setter(d, r); d },
                                         name)

  sealed abstract class FieldD(
                          name: String)(
                          implicit witness: DocRepr =:= DefaultDocRepr)
                        extends AbstractField(name) { field =>
    final def get(doc: DocRepr) = witness(doc).get[field.type](field)
    final def set(doc: DocRepr, value: Repr) =
      witness(doc).set[field.type](field, value)
  }

  abstract class AbstractBasicField(name: String)
                   extends AbstractField(name)
                      with BasicFieldBase

  abstract class BasicField[B <: BsonValue, R] private[Document](
                   getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
                   name: String = null)(
                   implicit fromRepr: R => B, toRepr: B => R,
                            bsonManifest: ClassManifest[B])
                 extends Field(getter, setter, name) with BasicFieldBase {
    final type Bson = B

    final val bsonClass = bsonManifest.erasure.asInstanceOf[Class[B]]

    final def fromBson(bson: B): R = toRepr(bson)
    final def toBson(repr: R): B = fromRepr(repr)

    def default = fromBson(Bson.default[B])
  }

  abstract class BasicFieldM[B <: BsonValue, R] private[Document](
                   getter: DocRepr => R, setter: (DocRepr, R) => Unit,
                   name: String = null)(
                   implicit fromRepr: R => B, toRepr: B => R,
                            bsonManifest: ClassManifest[B])
                 extends BasicField[B, R](getter, (d, r) => { setter(d, r); d },
                                          name)

  abstract class BasicFieldD[B <: BsonValue, R](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr,
                            fromRepr: R => B, toRepr: B => R,
                            bsonManifest: ClassManifest[B])
                 extends FieldD(name) with BasicFieldBase {
    final type Repr = R
    final type Bson = B

    final val bsonClass = bsonManifest.erasure.asInstanceOf[Class[B]]

    final def fromBson(bson: B): R = toRepr(bson)
    final def toBson(repr: R): B = fromRepr(repr)

    def default = fromBson(Bson.default[B])
  }

  class BoolField[R](
          getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
          name: String = null)(
          implicit fromRepr: R => BsonBool, toRepr: BsonBool => R)
        extends BasicField[BsonBool, R](getter, setter, name)

  class BoolFieldM[R](
          getter: DocRepr => R, setter: (DocRepr, R) => Unit,
          name: String = null)(
          implicit fromRepr: R => BsonBool, toRepr: BsonBool => R)
        extends BasicFieldM[BsonBool, R](getter, setter, name)

  class BoolFieldD[R](
          name: String = null)(
          implicit witness: DocRepr =:= DefaultDocRepr,
                   fromRepr: R => BsonBool, toRepr: BsonBool => R)
        extends BasicFieldD[BsonBool, R](name)

  class OptBoolField[R](
          getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
          name: String = null)(
          implicit fromRepr: R => OptBsonBool, toRepr: OptBsonBool => R)
        extends BasicField[OptBsonBool, R](getter, setter, name)

  class OptBoolFieldM[R](
          getter: DocRepr => R, setter: (DocRepr, R) => Unit,
          name: String = null)(
          implicit fromRepr: R => OptBsonBool, toRepr: OptBsonBool => R)
        extends BasicFieldM[OptBsonBool, R](getter, setter, name)

  class OptBoolFieldD[R](
          name: String = null)(
          implicit witness: DocRepr =:= DefaultDocRepr,
                   fromRepr: R => OptBsonBool, toRepr: OptBsonBool => R)
        extends BasicFieldD[OptBsonBool, R](name)

  class IntField[R](
          getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
          name: String = null)(
          implicit fromRepr: R => BsonInt, toRepr: BsonInt => R)
        extends BasicField[BsonInt, R](getter, setter, name)

  class IntFieldM[R](
          getter: DocRepr => R, setter: (DocRepr, R) => Unit,
          name: String = null)(
          implicit fromRepr: R => BsonInt, toRepr: BsonInt => R)
        extends BasicFieldM[BsonInt, R](getter, setter, name)

  class IntFieldD[R](name: String = null)(
          implicit witness: DocRepr =:= DefaultDocRepr,
                   fromRepr: R => BsonInt, toRepr: BsonInt => R)
        extends BasicFieldD[BsonInt, R](name)

  class LongField[R](
          getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
          name: String = null)(
          implicit fromRepr: R => BsonLong, toRepr: BsonLong => R)
        extends BasicField[BsonLong, R](getter, setter, name)

  class LongFieldM[R](
          getter: DocRepr => R, setter: (DocRepr, R) => Unit,
          name: String = null)(
          implicit fromRepr: R => BsonLong, toRepr: BsonLong => R)
        extends BasicFieldM[BsonLong, R](getter, setter, name)

  class LongFieldD[R](name: String = null)(
          implicit witness: DocRepr =:= DefaultDocRepr,
                   fromRepr: R => BsonLong, toRepr: BsonLong => R)
        extends BasicFieldD[BsonLong, R](name)

  class DoubleField[R](
          getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
          name: String = null)(
          implicit fromRepr: R => BsonDouble, toRepr: BsonDouble => R)
        extends BasicField[BsonDouble, R](getter, setter, name)

  class DoubleFieldM[R](
          getter: DocRepr => R, setter: (DocRepr, R) => Unit,
          name: String = null)(
          implicit fromRepr: R => BsonDouble, toRepr: BsonDouble => R)
        extends BasicFieldM[BsonDouble, R](getter, setter, name)

  class DoubleFieldD[R](name: String = null)(
          implicit witness: DocRepr =:= DefaultDocRepr,
                   fromRepr: R => BsonDouble, toRepr: BsonDouble => R)
        extends BasicFieldD[BsonDouble, R](name)

  class StringField[R](
          getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
          name: String = null)(
          implicit fromRepr: R => BsonStr, toRepr: BsonStr => R)
        extends BasicField[BsonStr, R](getter, setter, name)

  class StringFieldM[R](
          getter: DocRepr => R, setter: (DocRepr, R) => Unit,
          name: String = null)(
          implicit fromRepr: R => BsonStr, toRepr: BsonStr => R)
        extends BasicFieldM[BsonStr, R](getter, setter, name)

  class StringFieldD[R](name: String = null)(
          implicit witness: DocRepr =:= DefaultDocRepr,
                   fromRepr: R => BsonStr, toRepr: BsonStr => R)
        extends BasicFieldD[BsonStr, R](name)

  class OptStringField[R](
          getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
          name: String = null)(
          implicit fromRepr: R => OptBsonStr, toRepr: OptBsonStr => R)
        extends BasicField[OptBsonStr, R](getter, setter, name)

  class OptStringFieldM[R](
          getter: DocRepr => R, setter: (DocRepr, R) => Unit,
          name: String = null)(
          implicit fromRepr: R => OptBsonStr, toRepr: OptBsonStr => R)
        extends BasicFieldM[OptBsonStr, R](getter, setter, name)

  class OptStringFieldD[R](name: String = null)(
          implicit witness: DocRepr =:= DefaultDocRepr,
                   fromRepr: R => OptBsonStr, toRepr: OptBsonStr => R)
        extends BasicFieldD[OptBsonStr, R](name)

  class DateField[R](
          getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
          name: String = null)(
          implicit fromRepr: R => BsonDate, toRepr: BsonDate => R)
        extends BasicField[BsonDate, R](getter, setter, name)

  class DateFieldM[R](
          getter: DocRepr => R, setter: (DocRepr, R) => Unit,
          name: String = null)(
          implicit fromRepr: R => BsonDate, toRepr: BsonDate => R)
        extends BasicFieldM[BsonDate, R](getter, setter, name)

  class DateFieldD[R](name: String = null)(
          implicit witness: DocRepr =:= DefaultDocRepr,
                   fromRepr: R => BsonDate, toRepr: BsonDate => R)
        extends BasicFieldD[BsonDate, R](name)

  class OptDateField[R](
          getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
          name: String = null)(
          implicit fromRepr: R => OptBsonDate, toRepr: OptBsonDate => R)
        extends BasicField[OptBsonDate, R](getter, setter, name)

  class OptDateFieldM[R](
          getter: DocRepr => R, setter: (DocRepr, R) => Unit,
          name: String = null)(
          implicit fromRepr: R => OptBsonDate, toRepr: OptBsonDate => R)
        extends BasicFieldM[OptBsonDate, R](getter, setter, name)

  class OptDateFieldD[R](name: String = null)(
          implicit witness: DocRepr =:= DefaultDocRepr,
                   fromRepr: R => OptBsonDate, toRepr: OptBsonDate => R)
        extends BasicFieldD[OptBsonDate, R](name)

  class IdField[R](
          getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
          name: String = null)(
        implicit fromRepr: R => BsonId, toRepr: BsonId => R)
          extends BasicField[BsonId, R](getter, setter, name)

  class IdFieldM[R](
          getter: DocRepr => R, setter: (DocRepr, R) => Unit,
          name: String = null)(
        implicit fromRepr: R => BsonId, toRepr: BsonId => R)
          extends BasicFieldM[BsonId, R](getter, setter, name)

  class IdFieldD[R](name: String = null)(
          implicit witness: DocRepr =:= DefaultDocRepr,
                   fromRepr: R => BsonId, toRepr: BsonId => R)
        extends BasicFieldD[BsonId, R](name)

  class OptIdField[R](
          getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
          name: String = null)(
        implicit fromRepr: R => OptBsonId, toRepr: OptBsonId => R)
          extends BasicField[OptBsonId, R](getter, setter, name)

  class OptIdFieldM[R](
          getter: DocRepr => R, setter: (DocRepr, R) => Unit,
          name: String = null)(
        implicit fromRepr: R => OptBsonId, toRepr: OptBsonId => R)
          extends BasicFieldM[OptBsonId, R](getter, setter, name)

  class OptIdFieldD[R](name: String = null)(
          implicit witness: DocRepr =:= DefaultDocRepr,
                   fromRepr: R => OptBsonId, toRepr: OptBsonId => R)
        extends BasicFieldD[OptBsonId, R](name)

  abstract class AbstractEmbeddingField(name: String)
                   extends AbstractField(name)
                      with EmbeddingFieldBase

  abstract class EmbeddingField[R](
                   getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
                   name: String = null)
                 extends Field(getter, setter, name)
                    with EmbeddingFieldBase

  abstract class EmbeddingFieldM[R](
                   getter: DocRepr => R, setter: (DocRepr, R) => Unit,
                   name: String = null)
                 extends FieldM(getter, setter, name)
                    with EmbeddingFieldBase

  abstract class EmbeddingFieldD[R](name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with EmbeddingFieldBase {
    final type Repr = R
  }

  abstract class EmbeddingFieldDD(
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with EmbeddingFieldBase {
    final type Repr = DefaultDocRepr
    final protected def newDocRepr() = new DefaultDocRepr
  }

  abstract class OptEmbeddingField[R](
                   getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
                   name: String = null)
                 extends Field(getter, setter, name)
                    with OptEmbeddingFieldBase

  abstract class OptEmbeddingFieldM[R](
                   getter: DocRepr => R, setter: (DocRepr, R) => Unit,
                   name: String = null)
                 extends FieldM(getter, setter, name)
                    with OptEmbeddingFieldBase

  abstract class OptEmbeddingFieldD[R](name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with OptEmbeddingFieldBase {
    final type Repr = R
  }

  abstract class OptEmbeddingFieldDD(
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with OptEmbeddingFieldBase {
    final type Repr = DefaultDocRepr
    final protected def newDocRepr() = new DefaultDocRepr
    protected final def nullDocRepr = null
  }

  abstract class AbstractElementsArrayField(fieldName: String = null)
                   extends AbstractField(fieldName)
                      with ElementsArrayFieldBase

  abstract class ElementsArrayField[B <: BsonValue, R, C[X]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => DocRepr,
                   name: String = null)(
                   implicit fromRepr: R => B, toRepr: B => R)
                 extends Field(getter, setter, name)
                    with ElementsArrayFieldBase {
    final type ValueRepr = R
    final type Bson = B

    final def fromBson(bson: B): R = toRepr(bson)
    final def toBson(repr: R): B = fromRepr(repr)
  }

  abstract class ElementsArrayFieldM[B <: BsonValue, R, C[X]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => Unit,
                   name: String = null)(
                   implicit fromRepr: R => B, toRepr: B => R)
                 extends ElementsArrayField[B, R, C](
                           getter, (doc, c) => { setter(doc, c); doc }, name)

  abstract class ElementsArrayFieldD[B <: BsonValue, R, C[_]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr,
                            fromRepr: R => B, toRepr: B => R)
                 extends FieldD(name) with ElementsArrayFieldBase {
    final type Repr = C[R]
    final type ValueRepr = R
    final type Bson = B

    final def fromBson(bson: B): R = toRepr(bson)
    final def toBson(repr: R): B = fromRepr(repr)
  }

  abstract class BoolArrayField[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => DocRepr,
                   name: String = null)(
                   implicit fromRepr: R => BsonBool, toRepr: BsonBool => R)
                 extends ElementsArrayField[BsonBool, R, C](
                           getter, setter, name)

  abstract class BoolArrayFieldM[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => Unit,
                   name: String = null)(
                   implicit fromRepr: R => BsonBool, toRepr: BsonBool => R)
                 extends ElementsArrayFieldM[BsonBool, R, C](
                           getter, setter, name)

  abstract class BoolArrayFieldD[R, C[_]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr,
                            fromRepr: R => BsonBool, toRepr: BsonBool => R)
                 extends ElementsArrayFieldD[BsonBool, R, C](name)

  abstract class IntArrayField[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => DocRepr,
                   name: String = null)(
                   implicit fromRepr: R => BsonInt, toRepr: BsonInt => R)
                 extends ElementsArrayField[BsonInt, R, C](getter, setter, name)

  abstract class IntArrayFieldM[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => Unit,
                   name: String = null)(
                   implicit fromRepr: R => BsonInt, toRepr: BsonInt => R)
                 extends ElementsArrayFieldM[BsonInt, R, C](
                           getter, setter, name)

  abstract class IntArrayFieldD[R, C[_]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr,
                            fromRepr: R => BsonInt, toRepr: BsonInt => R)
                 extends ElementsArrayFieldD[BsonInt, R, C](name)

  abstract class LongArrayField[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => DocRepr,
                   name: String = null)(
                   implicit fromRepr: R => BsonLong, toRepr: BsonLong => R)
                 extends ElementsArrayField[BsonLong, R, C](
                           getter, setter, name)

  abstract class LongArrayFieldM[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => Unit,
                   name: String = null)(
                   implicit fromRepr: R => BsonLong, toRepr: BsonLong => R)
                 extends ElementsArrayFieldM[BsonLong, R, C](
                           getter, setter, name)

  abstract class LongArrayFieldD[R, C[_]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr,
                            fromRepr: R => BsonLong, toRepr: BsonLong => R)
                 extends ElementsArrayFieldD[BsonLong, R, C](name)

  abstract class DoubleArrayField[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => DocRepr,
                   name: String = null)(
                   implicit fromRepr: R => BsonDouble, toRepr: BsonDouble => R)
                 extends ElementsArrayField[BsonDouble, R, C](
                           getter, setter, name)

  abstract class DoubleArrayFieldM[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => Unit,
                   name: String = null)(
                   implicit fromRepr: R => BsonDouble, toRepr: BsonDouble => R)
                 extends ElementsArrayFieldM[BsonDouble, R, C](
                           getter, setter, name)

  abstract class DoubleArrayFieldD[R, C[_]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr,
                            fromRepr: R => BsonDouble, toRepr: BsonDouble => R)
                 extends ElementsArrayFieldD[BsonDouble, R, C](name)

  abstract class StringArrayField[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => DocRepr,
                   name: String = null)(
                   implicit fromRepr: R => BsonStr, toRepr: BsonStr => R)
                 extends ElementsArrayField[BsonStr, R, C](
                           getter, setter, name)

  abstract class StringArrayFieldM[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => Unit,
                   name: String = null)(
                   implicit fromRepr: R => BsonStr, toRepr: BsonStr => R)
                 extends ElementsArrayFieldM[BsonStr, R, C](
                           getter, setter, name)

  abstract class StringArrayFieldD[R, C[_]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr,
                            fromRepr: R => BsonStr, toRepr: BsonStr => R)
                 extends ElementsArrayFieldD[BsonStr, R, C](name)

  abstract class DateArrayField[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => DocRepr,
                   name: String = null)(
                   implicit fromRepr: R => BsonDate, toRepr: BsonDate => R)
                 extends ElementsArrayField[BsonDate, R, C](
                           getter, setter, name)

  abstract class DateArrayFieldM[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => Unit,
                   name: String = null)(
                   implicit fromRepr: R => BsonDate, toRepr: BsonDate => R)
                 extends ElementsArrayFieldM[BsonDate, R, C](
                           getter, setter, name)

  abstract class DateArrayFieldD[R, C[_]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr,
                            fromRepr: R => BsonDate, toRepr: BsonDate => R)
                 extends ElementsArrayFieldD[BsonDate, R, C](name)

  abstract class IdArrayField[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => DocRepr,
                   name: String = null)(
                   implicit fromRepr: R => BsonId, toRepr: BsonId => R)
                 extends ElementsArrayField[BsonId, R, C](
                           getter, setter, name)

  abstract class IdArrayFieldM[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => Unit,
                   name: String = null)(
                   implicit fromRepr: R => BsonId, toRepr: BsonId => R)
                 extends ElementsArrayFieldM[BsonId, R, C](
                           getter, setter, name)

  abstract class IdArrayFieldD[R, C[_]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr,
                            fromRepr: R => BsonId, toRepr: BsonId => R)
                 extends ElementsArrayFieldD[BsonId, R, C](name)

  abstract class AbstractDocumentsArrayField(name: String = null)
                   extends AbstractField(name)
                      with DocumentsArrayFieldBase

  abstract class DocumentsArrayField[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => DocRepr,
                   name: String = null)
                 extends Field(getter, setter, name)
                    with DocumentsArrayFieldBase {
    final type DocRepr = R
  }

  abstract class DocumentsArrayFieldM[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => Unit,
                   name: String = null)
                 extends FieldM(getter, setter, name)
                    with DocumentsArrayFieldBase {
    final type DocRepr = R
  }

  abstract class DocumentsArrayFieldD[R, C[_]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with DocumentsArrayFieldBase {
    final type DocRepr = R
    final type Repr = C[R] 
  }

  abstract class DocumentsArrayFieldDD[C[_]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with DocumentsArrayFieldBase
                    with DefaultReprDocument {
    final type Repr = C[DocRepr] 
  }

  abstract class DocumentsMapField[R, K, C[_, _]](
                   getter: DocRepr => C[K, R],
                   setter: (DocRepr, C[K, R]) => DocRepr,
                   name: String = null)
                 extends Field(getter, setter, name)
                    with DocumentsArrayFieldBase {
    final type DocRepr = R
    final type Key = K
  }

  abstract class DocumentsMapFieldM[R, K, C[_, _]](
                   getter: DocRepr => C[K, R],
                   setter: (DocRepr, C[K, R]) => Unit,
                   name: String = null)
                 extends FieldM(getter, setter, name)
                    with DocumentsArrayFieldBase {
    final type DocRepr = R
    final type Key = K
  }

  abstract class DocumentsMapFieldD[R, K, C[_, _]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with DocumentsArrayFieldBase {
    final type DocRepr = R
    final type Key = K
    final type Repr = C[K, R] 
  }

  abstract class DocumentsMapFieldDD[K, C[_, _]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with DocumentsArrayFieldBase
                    with DefaultReprDocument {
    final type Key = K
    final type Repr = C[K, DocRepr] 
  }

  final def toBson(value: DocRepr): BsonObject =
    BsonObject(fields.map(f => f.fieldName -> f.fieldBson(f.get(value))).toMap)

  final class DocObject private[smogon](
                private var doc: DocRepr) extends DBObject {
    import scala.collection.JavaConversions._

    def repr = doc
    def get(key: String): AnyRef = fieldByName.get(key) match {
      case Some(field) => field match {
        case field: BasicFieldBase =>
          val bson = field.toBson(field.get(doc))
          if (key == "_id" && field.bsonClass == classOf[BsonId] &&
              bson == BsonId.Zero && document.isInstanceOf[Collection])
            null
          else
            Bson.toRaw(bson)
        case field: EmbeddingFieldBase =>
          field.dbObject(field.get(doc))
        case field: ElementsArrayFieldBase =>
          asIterable(
            field.iterator(field.get(doc)).map(field.toBson(_)).toIterable)
        case field: DocumentsArrayFieldBase =>
          asIterable(
            field.iterator(field.get(doc)).map(field.dbObject).toIterable)
      }
      case None =>
        if (key == "_transientFields")
          Nil
        else
          throw new NoSuchElementException(key)
    }
    def put(key: String, value: AnyRef): AnyRef = fieldByName.get(key) match {
      case Some(field) =>
        field match {
          case field: BasicFieldBase =>
            doc = field.set(doc, field.fromBson(Bson.fromRaw(value).
                                   asInstanceOf[field.Bson]))
          case field: EmbeddingFieldBase =>
            val dbo = field.dbObject(field.create)
            dbo.putAll(value.asInstanceOf[DBObject])
            doc = field.set(doc, dbo.repr)
          case field: ElementsArrayFieldBase =>
            var es = field.newArrayRepr
            val it: Iterator[AnyRef] = value match {
              case elems: java.lang.Iterable[_] =>
                elems.iterator.asInstanceOf[java.util.Iterator[AnyRef]]
              case elems: DBObject =>
                elems.toMap.valuesIterator.asInstanceOf[Iterator[AnyRef]]
            }
            it.foreach { e =>
              val elem = field.fromBson(
                           Bson.fromRaw(e).asInstanceOf[field.Bson])
              es = field.append(es, elem)
            }
            doc = field.set(doc, es)
          case field: DocumentsArrayFieldBase =>
            var ds = field.newArrayRepr
            val it: Iterator[DBObject] = value match {
              case elems: java.lang.Iterable[_] =>
                elems.iterator.asInstanceOf[java.util.Iterator[DBObject]]
              case elems: DBObject =>
                elems.toMap.valuesIterator.asInstanceOf[Iterator[DBObject]]
            }
            it.foreach { e =>
              val d = field.dbObject(field.create)
              d.putAll(e)
              ds = field.append(ds, d.repr)
            }
            doc = field.set(doc, ds)
        }
        value
      case None => value
    }
    def putAll(map: java.util.Map[_, _]) =
      map.foreach { case (k, v) => put(k.toString, v.asInstanceOf[AnyRef]) }
    def putAll(obj: BSONObject) =
      obj.keySet.foreach { k => put(k, obj.get(k)) }
    def removeField(key: String) = throw new UnsupportedOperationException
    def keySet = fieldByName.keySet
    def containsKey(key: String) = fieldByName.contains(key)
    def containsField(key: String) = fieldByName.contains(key)
    def toMap = Map[AnyRef, AnyRef]()
    def isPartialObject = false
    def markAsPartialObject() {}
  }

  final def dbObject(doc: DocRepr) = new DocObject(doc)

  final def jsonSpec: JsonSpec.NoDefault[this.type, InOut] =
    JsonSpec.Many[this.type, InOut](fieldByIndex)
  def enter[IO <: Direction](
        name: String)(
        spec: JsonSpec.NoDefault[d.type, IO] forSome { val d: this.type }) =
    JsonSpec.InCustomEmbedding[this.type, IO](
          name, spec.asInstanceOf[JsonSpec.NoDefault[this.type, IO]])
  def putOpt(name: String)(conv: DocRepr => Option[JsonValue]) =
    JsonSpec.CustomFieldTo[this.type](name, this, conv)
  def put(name: String)(conv: DocRepr => JsonValue) =
    putOpt(name)(d => Some(conv(d)))

  final def fromJson(
              spec: this.type =>
                    JsonSpec[d.type, In] forSome { val d: this.type }) =
    new JsonSpec.Reader[this.type](
          spec(this).asInstanceOf[JsonSpec[this.type, In]])
  final def fromJson =
    new JsonSpec.Reader[this.type](
          JsonSpec.Many[this.type, In](fieldByIndex))
  final def toJson(
              spec: this.type =>
                    JsonSpec[d.type, Out] forSome { val d: this.type }) =
    new JsonSpec.Writer[this.type](
          spec(this).asInstanceOf[JsonSpec[this.type, Out]])
  final def toJson =
    new JsonSpec.Writer[this.type](
          JsonSpec.Many[this.type, Out](fieldByIndex))
}

trait DefaultReprDocument extends Document {
  final type DocRepr = DefaultDocRepr
  final protected def newDocRepr() = new DefaultDocRepr
}

sealed trait Safety
object Safety {
  object Default extends Safety
  sealed trait Concrete extends Safety {
    val value: Int
  }
  object Concrete {
    def unapply(x: Any): Option[Int] = x match {
      case x: Concrete => Some(x.value)
      case _ => None
    }
  }
  object Unsafe extends Concrete {
    val value = -1
  }
  object Network extends Concrete {
    val value = 0
  }
  final case class Safe(slaves: Int = 0, timeout: Int = 0) extends Concrete {
    require(slaves >= 0)
    require(timeout >= 0)
    val value = 1 + slaves
  }
}

object Collection {
  val DuplicateKeyRegex = """E[^\$]+\$([^\s]+).*""".r

  def genId(): BsonId = {
    val id = ObjectId.get()
    BsonId(id._time, id._machine, id._inc)
  }

  def writeConcern(safety: Safety.Concrete): WriteConcern = safety match {
    case Safety.Unsafe => WriteConcern.NONE
    case Safety.Network => WriteConcern.NORMAL
    case Safety.Safe(slaves, timeout) => new WriteConcern(1 + slaves, timeout)
  }

  def safetyOf(dbc: DBCollection,
               safety: Safety = Safety.Default): Safety.Concrete =
    safety match {
      case Safety.Default =>
        val wc = dbc.getWriteConcern
        wc.getW match {
          case w if w < 0 => Safety.Unsafe
          case 0 => Safety.Network
          case w => Safety.Safe(w - 1, wc.getWtimeout)
        }
      case safety: Safety.Concrete => safety
    }

  def handleErrors(body: => WriteResult): WriteResult =
    try {
      body
    } catch {
      case e: MongoException.DuplicateKey =>
        throw new DuplicateKeyException(e.getMessage match {
                      case DuplicateKeyRegex(name) => name
                      case _ => "unknown"
                    })
      case e: MongoException => throw new SmogonException(e)
    }
  }

trait Collection extends Documents {
  import Collection._

  final type Coll = this.type

  val id: BasicFieldBase

  final def defaultSafetyOf(dbc: DBCollection): Safety.Concrete = safetyOf(dbc)
  final def defaultSafetyOf(dbc: DBCollection, safety: Safety.Concrete) =
    dbc.setWriteConcern(writeConcern(safety))

  final def insertInto(dbc: DBCollection, doc: DocRepr,
                       safety: Safety = Safety.Default,
                       timeout: Int = 0): DocRepr = {
    val dbo = dbObject(doc)
    val cs = safetyOf(dbc, safety)
    handleErrors(dbc.insert(dbo))
    dbo.repr
  }

  final def saveInto(dbc: DBCollection, doc: DocRepr,
                     safety: Safety = Safety.Default,
                     timeout: Int = 0): DocRepr = {
    val dbo = dbObject(doc)
    val cs = safetyOf(dbc, safety)
    handleErrors(dbc.save(dbo))
    dbo.repr
  }

  final def sizeOf(dbc: DBCollection): Long = dbc.count

  final def ensureIndexIn(dbc: DBCollection,
                          index: this.type => Sort[c.type]
                                                forSome { val c: this.type },
                          name: String = null, unique: Boolean = false) {
    val dbo = new BasicDBObject
    val sort = index(this)
    sort.sortByFields.foreach { case (f, asc, _) =>
      dbo.put(f.fieldRootName, if (asc) 1 else -1)
    }
    dbc.ensureIndex(dbo, if (name == null) DBCollection.genIndexName(dbo)
                         else name, unique)
  }

  def apply(filter: this.type =>
                    Filter[c.type]
                      forSome { val c: this.type }): Query[this.type] =
    new Query[this.type](this, filter(this).asInstanceOf[Filter[this.type]])
  def apply(): Query[this.type] =
    new Query[this.type](this)
}

trait DefaultReprCollection extends Collection with DefaultReprDocument

trait AssociatedCollection extends Collection {
  protected def dbCollection: DBCollection
  private[smogon] def getDbCollection = dbCollection

  final def defaultSafety() = defaultSafetyOf(dbCollection)
  final def defaultSafety(safety: Safety.Concrete) =
    defaultSafetyOf(dbCollection, safety)
  final def insert(doc: DocRepr, safety: Safety = Safety.Default,
                   timeout: Int = 0): DocRepr =
    insertInto(dbCollection, doc, safety, timeout)
  final def save(doc: DocRepr, safety: Safety = Safety.Default,
                 timeout: Int = 0): DocRepr =
    saveInto(dbCollection, doc, safety, timeout)
  final def size() = sizeOf(dbCollection)
  final def ensureIndex(
              index: this.type => Sort[c.type] forSome { val c: this.type },
              name: String = null, unique: Boolean = false) =
    ensureIndexIn(dbCollection, index, name, unique)
}
