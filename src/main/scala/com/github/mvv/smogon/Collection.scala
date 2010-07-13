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

import scala.collection.generic.{SeqFactory, GenericTraversableTemplate}
import scala.util.matching.Regex
import org.bson.BSONObject
import com.mongodb.{DBObject, BasicDBObject, DBCollection}

trait BsonValue {
  type ValueRepr
  type Bson <: BsonType

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
  def toBson(include: Boolean): DBObject = {
    val value = if (include) 1 else 0
    val bson = new BasicDBObject
    projectedFields.foreach { case (f, _) =>
      bson.put(f.fieldFullName, value)
    }
    bson
  }
  def &(proj: Projection[C]): Projection[C] =
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
  def toBson: DBObject = {
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

sealed trait Document { document =>
  import Document._

  type Coll <: Collection
  type Root <: Documents
  type DocRepr

  sealed trait FieldBase extends HasName
                            with Projection[Coll]
                            with Sort[Root] { field =>
    final type Doc = document.type
    type Repr

    val fieldIndex: Int

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
  }

  private val fieldByName: scala.collection.mutable.Map[String, FieldBase] =
    new scala.collection.mutable.HashMap()
  private val fieldByIndex: scala.collection.mutable.Buffer[FieldBase] =
    new scala.collection.mutable.ListBuffer()
  
  def field(name: String): Option[FieldBase] = fieldByName.get(name)
  def fields: Iterator[FieldBase] = fieldByIndex.iterator

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
      fieldByIndex += this
      fieldByName.size - 1
    }
  }

  sealed trait BasicFieldBase extends FieldBase with BsonValue {
    final type ValueRepr = Repr

    val bsonClass: Class[Bson]

    def ===(value: Repr) = Filter.Eq[Doc, this.type](this, value)
    def !==(value: Repr) = !(this === value)
    def in(values: Set[Repr]) = Filter.In[Doc, this.type](this, values)
    def notIn(values: Set[Repr]) = !(this in values)
  }

  object BasicFieldBase {
    final class BasicFieldOps[F <: BasicFieldBase] private[BasicFieldBase](
                  field: F)(implicit witness: F#Bson <:< BasicBsonType) {
      def <(value: F#Repr) = Filter.Less[F#Doc, F](field, value)
      def <=(value: F#Repr) = Filter.LessOrEq[F#Doc, F](field, value)
      def >(value: F#Repr) = !(this <= value)
      def >=(value: F#Repr) = !(this < value)
    }

    implicit def basicFieldOps[F <: BasicFieldBase](
                   field: F)(
                   implicit witness: F#Bson <:< BasicBsonType) =
      new BasicFieldOps(field)

    final class ModResultFilter[F <: BasicFieldBase] private[BasicFieldBase](
                  field: F, devisor: F#Repr)(
                  implicit witness: F#Bson <:< IntegralBsonType) {
      def ===(result: F#Repr) = Filter.Mod[F#Doc, F](field, devisor, result)
      def !==(result: F#Repr) = !(this === result)
    }

    final class IntegralFieldOps[F <: BasicFieldBase] private[BasicFieldBase](
                  field: F)(implicit witness: F#Bson <:< IntegralBsonType) {
      def %(devisor: F#Repr) = new ModResultFilter(field, devisor)
    }

    implicit def integralFieldOps[F <: BasicFieldBase](
                   field: F)(
                   implicit witness: F#Bson <:< IntegralBsonType) =
      new IntegralFieldOps(field)

    final class StringFieldOps[F <: BasicFieldBase] private[BasicFieldBase](
                  field: F)(implicit witness: F#Bson =:= BsonString) {
      def =~(regex: Regex) = Filter.Match[F#Doc, F](field, regex)
      def !~(regex: Regex) = !(this =~ regex)
    }

    implicit def stringFieldOps[F <: BasicFieldBase](
                   field: F)(
                   implicit witness: F#Bson =:= BsonString) =
      new StringFieldOps(field)
  }

  sealed trait EmbeddingFieldBase extends FieldBase with Document {
    final type Coll = document.Coll
    final type Root = document.Root
    final type DocRepr = Repr

    final def default = create
  }

  sealed trait ArrayFieldBase extends FieldBase {
    type ElemRepr

    protected def newArrayRepr(): Repr
    protected def append(repr: Repr, value: ElemRepr): Repr
    def iterator(repr: Repr): Iterator[ElemRepr]

    final def default = newArrayRepr

    final def size(n: Long) = Filter.Size[Doc, this.type](this, n)
  }

  trait SeqArrayField[S[X] <: Seq[X] with GenericTraversableTemplate[X, S]]
          extends AbstractField with ArrayFieldBase {
    type Repr >: S[ElemRepr] <: Seq[ElemRepr]

    protected def seqFactory: SeqFactory[S]

    protected def newArrayRepr(): Repr = seqFactory.empty[ElemRepr]
    protected def append(repr: Repr, value: ElemRepr): Repr =
      seqFactory.concat(repr :+ value)
    def iterator(repr: Repr): Iterator[ElemRepr] = repr.iterator
  }

  sealed trait ElementsArrayFieldBase extends ArrayFieldBase with BsonValue {
    final type ElemRepr = ValueRepr

    final def contains(filter: ValueFilterBuilder[this.type] => ValueFilter[this.type]) =
      Filter.ContainsElem[Doc, this.type](
        this, filter(new ValueFilterBuilder[this.type]))
  }

  trait DocumentsArrayFieldBase extends ArrayFieldBase with Documents {
    final type Coll = document.Coll
    final type ElemRepr = DocRepr

    final def contains(filter: this.type => Filter[this.type]): Filter[Doc#Root] =
      Filter.Contains[Doc, this.type](this, filter(this))
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
    final def set(doc: DocRepr, value: Repr) = witness(doc).set[field.type](field, value)
  }

  abstract class AbstractBasicField(name: String)
                   extends AbstractField(name)
                      with BasicFieldBase

  abstract class BasicField[B <: BsonType, R] private[Document](
                   getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
                   name: String = null)(
                   implicit fromRepr: R => B, toRepr: B => R,
                            bsonManifest: ClassManifest[B])
                 extends Field(getter, setter, name) with BasicFieldBase {
    final type Bson = B

    final val bsonClass = bsonManifest.erasure.asInstanceOf[Class[B]]

    final def fromBson(bson: B): R = toRepr(bson)
    final def toBson(repr: R): B = fromRepr(repr)

    def default = fromBson(BsonType.default[B])
  }

  abstract class BasicFieldM[B <: BsonType, R] private[Document](
                   getter: DocRepr => R, setter: (DocRepr, R) => Unit,
                   name: String = null)(
                   implicit fromRepr: R => B, toRepr: B => R,
                            bsonManifest: ClassManifest[B])
                 extends BasicField[B, R](getter, (d, r) => { setter(d, r); d },
                                          name)

  abstract class BasicFieldD[B <: BsonType, R](
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

    def default = fromBson(BsonType.default[B])
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
          implicit fromRepr: R => BsonString, toRepr: BsonString => R)
        extends BasicField[BsonString, R](getter, setter, name)

  class StringFieldM[R](
          getter: DocRepr => R, setter: (DocRepr, R) => Unit,
          name: String = null)(
          implicit fromRepr: R => BsonString, toRepr: BsonString => R)
        extends BasicFieldM[BsonString, R](getter, setter, name)

  class OptStringField[R](
          getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
          name: String = null)(
          implicit fromRepr: R => OptBsonString, toRepr: OptBsonString => R)
        extends BasicField[OptBsonString, R](getter, setter, name)

  class OptStringFieldM[R](
          getter: DocRepr => R, setter: (DocRepr, R) => Unit,
          name: String = null)(
          implicit fromRepr: R => OptBsonString, toRepr: OptBsonString => R)
        extends BasicFieldM[OptBsonString, R](getter, setter, name)

  class StringFieldD[R](name: String = null)(
          implicit witness: DocRepr =:= DefaultDocRepr,
                   fromRepr: R => BsonString, toRepr: BsonString => R)
        extends BasicFieldD[BsonString, R](name)

  class OptStringFieldD[R](name: String = null)(
          implicit witness: DocRepr =:= DefaultDocRepr,
                   fromRepr: R => OptBsonString, toRepr: OptBsonString => R)
        extends BasicFieldD[OptBsonString, R](name)

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

  abstract class AbstractElementsArrayField(fieldName: String = null)
                   extends AbstractField(fieldName)
                      with ElementsArrayFieldBase

  abstract class ElementsArrayField[B <: BsonType, R, C[X]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => DocRepr,
                   fieldName: String = null)(
                   implicit fromRepr: R => B, toRepr: B => R)
                 extends Field(getter, setter, fieldName)
                    with ElementsArrayFieldBase {
    final type ValueRepr = R
    final type Bson = B

    final def fromBson(bson: B): R = toRepr(bson)
    final def toBson(repr: R): B = fromRepr(repr)
  }

  abstract class ElementsArrayFieldD[B <: BsonType, R, C[_]](
                   fieldName: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr,
                            fromRepr: R => B, toRepr: B => R)
                 extends FieldD(fieldName)
                    with ElementsArrayFieldBase {
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
                 extends ElementsArrayField[BsonBool, R, C](getter, setter, name)

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

  abstract class IntArrayFieldD[R, C[_]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr,
                            fromRepr: R => BsonInt, toRepr: BsonInt => R)
                 extends ElementsArrayFieldD[BsonInt, R, C](name)

  abstract class AbstractDocumentsArrayField(name: String = null)
                   extends AbstractField(name)
                      with DocumentsArrayFieldBase

  abstract class DocumentsArrayField[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => DocRepr,
                   name: String = null)
                 extends Field(getter, setter, name)
                    with DocumentsArrayFieldBase

  abstract class DocumentsArrayFieldM[R, C[_]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => Unit,
                   name: String = null)
                 extends FieldM(getter, setter, name)
                    with DocumentsArrayFieldBase

  abstract class DocumentsArrayFieldD[R, C[_]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with DocumentsArrayFieldBase {
    final type Repr = C[R] 
    final type DocRepr = R
  }

  abstract class DocumentsArrayFieldDD[C[_]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with DocumentsArrayFieldBase
                    with DefaultReprDocument {
    final type Repr = C[DocRepr] 
  }

  final class DocObject private[smogon](private var doc: DocRepr) extends DBObject {
    import scala.collection.JavaConversions._

    def repr = doc
    def get(key: String): AnyRef = fieldByName.get(key) match {
      case Some(field) => field match {
        case field: BasicFieldBase =>
          field.toBson(field.get(doc)).toBson
        case field: EmbeddingFieldBase =>
          field.dbObject(field.get(doc))
        case field: ElementsArrayFieldBase =>
          asIterable(field.iterator(field.get(doc)).map(field.toBson(_).toBson).toIterable)
        case field: DocumentsArrayFieldBase =>
          asIterable(field.iterator(field.get(doc)).map(field.dbObject).toIterable)
      }
      case None =>
        if (key == "_transientFields")
          Nil
        else
          throw new NoSuchElementException(key)
    }
    def put(key: String, value: AnyRef): AnyRef = fieldByName.get(key) match {
      case Some(field) => field match {
          case field: BasicFieldBase =>
            doc = field.set(doc, field.fromBson(BsonType.fromRaw(value).
                                   asInstanceOf[field.Bson]))
          case field: EmbeddingFieldBase =>
            val dbo = field.dbObject(field.create)
            dbo.putAll(value.asInstanceOf[DBObject])
            doc = field.set(doc, dbo.repr)
          case field: ElementsArrayFieldBase =>
          case field: DocumentsArrayFieldBase =>
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
  final case class Safe(slaves: Int = 0) extends Concrete {
    require(slaves >= 0)
    val value = 1 + slaves
  }
}

trait Collection extends Documents {
  final type Coll = this.type

  val id: BasicFieldBase

  final def saveInto(dbc: DBCollection, doc: DocRepr,
                     safety: Safety = Safety.Default, timeout: Int = 0): DocRepr = {
    val dbo = dbObject(doc)
    dbc.save(dbo)
    dbo.repr
  }

  final def ensureIndexIn(dbc: DBCollection,
                          index: this.type => Sort[c.type] forSome { val c: this.type },
                          name: String = null, unique: Boolean = false) {
    val dbo = new BasicDBObject
    val sort = index(this)
    sort.sortByFields.foreach { case (f, asc, _) =>
      dbo.put(f.fieldRootName, if (asc) 1 else -1)
    }
    dbc.ensureIndex(dbo, if (name == null) DBCollection.genIndexName(dbo)
                         else name, unique)
  }

  def apply(filter: this.type => Filter[c.type] forSome { val c: this.type }): Query[this.type] =
    new Query[this.type](this, filter(this).asInstanceOf[Filter[this.type]])
  def apply(): Query[this.type] =
    new Query[this.type](this)
}

trait DefaultReprCollection extends Collection with DefaultReprDocument

trait AssociatedCollection extends Collection {
  protected def dbCollection: DBCollection
  private[smogon] def getDbCollection = dbCollection

  final def save(doc: DocRepr, safety: Safety = Safety.Default,
                 timeout: Int = 0): DocRepr =
    saveInto(dbCollection, doc, safety, timeout)
  final def ensureIndex(index: this.type => Sort[c.type] forSome { val c: this.type },
                        name: String = null, unique: Boolean = false) =
    ensureIndexIn(dbCollection, index, name, unique)
}
