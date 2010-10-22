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
         SeqFactory, SetFactory, MapFactory, GenericTraversableTemplate,
         GenericSetTemplate, SortedSetFactory, SortedMapFactory}
import scala.collection.{SetLike, MapLike, SortedSet, SortedSetLike,
                         SortedMap, SortedMapLike}
import scala.util.matching.Regex
import org.slf4j.LoggerFactory
import com.github.mvv.layson
import layson.bson._
import layson.json._
import org.bson.types.ObjectId
import com.mongodb.{
         DBObject, BasicDBObject, DBCollection, WriteConcern, WriteResult,
         MongoException}

trait ReprBsonValue {
  type ValueRepr
  type Bson <: BsonValue

  val bsonClass: Class[Bson]

  def fromBson(bson: Bson): ValueRepr
  def toBson(repr: ValueRepr): Bson

  def toJson(value: ValueRepr) = Bson.toJson(toBson(value))
  val fromJson: PartialFunction[JsonValue, ValueRepr] = {
    case value => fromBson(Bson.fromJson(value, bsonClass))
  }

  val validator: Validator[ValueRepr] = Validator.Ok
}

sealed trait Documents extends Document {
  final type Root = this.type
}

sealed trait HasFieldName {
  type Name
  val fieldName: Name
  val fieldNameString: String
  val fieldRootName: String
  val fieldFullName: String
}

sealed trait Projection[C <: Collection] {
  def projectedFields: Set[(F, F#Doc#Coll =:= C) forSome {
                             type D <: Document
                             type F <: D#FieldBase
                           }]
  final def projectionBson(include: Boolean): BsonObject = {
    val value = if (include) 1 else 0
    BsonObject {
      projectedFields.map { case (f, _) =>
        f.fieldFullName -> BsonInt(value)
      } .toMap
    }
  }
  final def *(proj: Projection[C]): Projection[C] =
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
  final def sortBson = BsonObject {
    sortByFields.map { case (f, asc, _) =>
      f.fieldRootName -> BsonInt(if (asc) 1 else -1)
    } .toMap
  }
  final def &(sort: Sort[DS]): Sort[DS] =
    new Sort.Many[DS](sortByFields ++ sort.sortByFields)
}

object Sort {
  final class Many[DS <: Documents](
                val sortByFields: Seq[(F, Boolean, F#Doc#Root =:= DS) forSome {
                                        type D <: Document
                                        type F <: D#FieldBase
                                      }]) extends Sort[DS]
}

sealed trait Fields[D <: Document] {
  def fieldsSet: Set[D#FieldBase]
  final def +(field: D#FieldBase) = Fields.Many(fieldsSet + field)
}

object Fields {
  final case class Many[D <: Document](fieldsSet: Set[D#FieldBase])
                   extends Fields[D]
}

object Field {
  def unapply[D <: Document](x: D#FieldBase): Option[D#FieldBase] =
    Some(x)
}
object BasicField {
  def unapply[D <: Document](
        x: D#BasicFieldBase): Option[D#BasicFieldBase] =
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
object ElementsArrayField {
  def unapply[D <: Document](
        x: D#ElementsArrayFieldBase): Option[D#ElementsArrayFieldBase] =
    Some(x)
}
object DocumentsArrayField {
  def unapply[D <: Document](
        x: D#DocumentsArrayFieldBase): Option[D#DocumentsArrayFieldBase] =
    Some(x)
}

trait DefaultReprDocument { self: Document =>
  final type DocRepr = DefaultDocRepr
  final protected def newDocRepr() = newDefaultDocRepr
}

sealed trait Document { document =>
  type Coll <: Collection
  type Root <: Documents
  type DocRepr
  type DefaultDocRepr

  type FieldName
  type AbstractField <: FieldBase

  def nameToString(name: FieldName): String
  def stringToName(str: String): Option[FieldName]

  def staticFields: Seq[AbstractField]
  def fieldsNames(doc: DocRepr): Iterator[FieldName]
  def containsField(doc: DocRepr, name: FieldName): Boolean
  def containsField(doc: DocRepr, name: String): Boolean

  protected def newDefaultDocRepr(): DefaultDocRepr
  protected def newDocRepr(): DocRepr
  def create(): DocRepr

  private[smogon] val isCollection = document.isInstanceOf[Collection]
  private[smogon] lazy val fieldRootNamePrefix = document match {
    case _: Documents => ""
    case hn: HasFieldName => hn.fieldRootName + '.'
  }
  private[smogon] lazy val fieldFullNamePrefix = document match {
    case _: Collection => ""
    case hn: HasFieldName => hn.fieldFullName + '.'
  }

  sealed trait FieldBase
               extends HasFieldName
                  with Projection[Coll]
                  with Sort[Root]
                  with JsonSpec.DocumentField[document.type]
                  with Fields[document.type] { self: AbstractField =>
    final type Name = FieldName
    final type Doc = document.type
    type Repr

    final lazy val fieldRootName = fieldRootNamePrefix + fieldName 
    final lazy val fieldFullName = fieldFullNamePrefix + fieldName 
    final def fieldDocument: Doc = document
    final def fieldsSet = Set(this)

    def fieldBson(value: Repr): BsonValue

    def default: Repr

    def getOpt(repr: DocRepr): Option[Repr]
    def get(repr: DocRepr): Repr
    def set(repr: DocRepr, value: Repr): DocRepr

    final def |(field: FieldBase) = this

    final def projectedFields = {
      val e = (this, implicitly[Doc#Coll =:= Coll])
      Set(e)
    }

    final def sortByFields = {
      val e = (this, true, implicitly[Doc#Root =:= Root])
      Seq(e)
    }
    final val asc = this
    final def desc = new Sort[Root] {
      def sortByFields = self.sortByFields.map {
        case (f, _, w) => (f, false, w)
      }
    }

    final def ===(value: Repr) = Filter.Eq[Doc, this.type](this, value)
    final def !==(value: Repr) = Filter.Not[Doc, this.type](this === value)
    final def in(values: Set[Repr]): Filter.In[Doc, this.type] =
      Filter.In[Doc, this.type](this, values)
    final def in(values: Repr*): Filter.In[Doc, this.type] = in(values.toSet)
    final def notIn(values: Set[Repr]) =
      Filter.Not[Doc, this.type](this in values)
    final def notIn(values: Repr*) =
      Filter.Not[Doc, this.type](this in values.toSet)

    final def =#(value: Repr) = Update.SetTo[Doc, this.type](this, value)

    final def as(name: String) = JsonSpec.Renamed[Doc, this.type](name, this)
    final def toOpt(conv: Repr => Option[JsonValue]) =
      JsonSpec.ConvTo[Doc, this.type](fieldNameString, this, conv)
    final def to(conv: Repr => JsonValue) = toOpt(r => Some(conv(r)))
    final def from(conv: PartialFunction[JsonValue, Repr]) =
      JsonSpec.ConvFrom[Doc, this.type](fieldNameString, this, conv)
    final def const(value: Repr) =
      new JsonSpec.Const[Doc, this.type](this, value)
    final def eval(expr: => Repr) =
      new JsonSpec.Eval[Doc, this.type](this, expr)

    def toRaw(value: Repr): AnyRef
    def fromRaw(value: AnyRef): Repr
  }

  sealed trait BasicFieldBase extends FieldBase
                                 with ReprBsonValue { self: AbstractField =>
    final type ValueRepr = Repr

    final def fieldBson(value: Repr): Bson = toBson(value)

    def +=(value: Repr)(implicit witness: Bson <:< OptNumericBsonValue) =
      Update.Increment[Doc, this.type](this, value)
    def -=(value: Repr)(implicit witness: Bson <:< OptNumericBsonValue) =
      Update.Decrement[Doc, this.type](this, value)

    final def toRaw(value: Repr) = {
      val bson = toBson(value)
      if (fieldName == "_id" && bson == BsonId.Zero && isCollection)
        null
      else
        Bson.toRaw(bson)
    }
    final def fromRaw(value: AnyRef) =
      fromBson(Bson.fromRaw(value).asInstanceOf[Bson])
  }

  object BasicFieldBase {
    final class BasicFieldOps[F <: BasicFieldBase] private[BasicFieldBase](
                  field: F)(implicit witness: F#Bson <:< OptSimpleBsonValue) {
      def <(value: F#Repr) = Filter.Less[F#Doc, F](field, value)
      def <=(value: F#Repr) = Filter.LessOrEq[F#Doc, F](field, value)
      def >(value: F#Repr) = Filter.Not[F#Doc, F](this <= value)
      def >=(value: F#Repr) = Filter.Not[F#Doc, F](this < value)
    }

    implicit def basicFieldOps[F <: BasicFieldBase](
                   field: F)(
                   implicit witness: F#Bson <:< OptSimpleBsonValue) =
      new BasicFieldOps(field)

    final class ModResultFilter[F <: BasicFieldBase] private[BasicFieldBase](
                  field: F, devisor: F#Repr)(
                  implicit witness: F#Bson <:< OptIntegralBsonValue) {
      def ===(result: F#Repr) = Filter.Mod[F#Doc, F](field, devisor, result)
      def !==(result: F#Repr) = Filter.Not[F#Doc, F](this === result)
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
      def !~(regex: Regex) = Filter.Not[F#Doc, F](this =~ regex)
    }

    implicit def stringFieldOps[F <: BasicFieldBase](
                   field: F)(
                   implicit witness: F#Bson <:< OptBsonStr) =
      new StringFieldOps(field)
  }

  sealed trait EmbeddingFieldBase
               extends FieldBase
                  with StaticDocument { self: AbstractField =>
    final type Coll = document.Coll
    final type Root = document.Root
    final type DocRepr = Repr

    def default = create
    def fieldBson(value: Repr): OptBsonObject

    final def enter[IO <: Direction](
                spec: this.type => JsonSpec[d.type, IO]
                                     forSome { val d: this.type }) =
      new JsonSpec.InEmbedding[Doc, this.type, IO](
            jsonMember, this, spec(this).asInstanceOf[JsonSpec[this.type, IO]])
    final def open[IO <: Direction](
                spec: this.type =>
                  JsonSpec.NotEmpty[d.type, IO]
                    forSome { val d: this.type }): JsonSpec.NotEmpty[Doc, IO] =
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

    def toRaw(value: Repr): DBObject
  }

  sealed trait MandatoryEmbeddingFieldBase
               extends EmbeddingFieldBase { self: AbstractField =>
    final def fieldBson(value: Repr): BsonObject = toBson(value)
    final def toRaw(value: Repr) = toDBObject(value)
    final def fromRaw(value: AnyRef) = {
      val dbo = toDBObject(default)
      dbo.putAll(value.asInstanceOf[DBObject])
      dbo.repr
    }
  }

  sealed trait OptEmbeddingFieldBase
               extends EmbeddingFieldBase { self: AbstractField =>
    def nullDocRepr: DocRepr
    def isNull(repr: DocRepr): Boolean
    override def default = nullDocRepr
    final def fieldBson(value: Repr) =
      if (isNull(value)) BsonNull
      else toBson(value)
    final def toRaw(value: Repr) =
      if (isNull(value)) null
      else toDBObject(value)
    final def fromRaw(value: AnyRef) = {
      if (value == null) nullDocRepr
      else {
        val dbo = toDBObject(default)
        dbo.putAll(value.asInstanceOf[DBObject])
        dbo.repr
      }
    }
  }

  sealed trait ArrayFieldBase extends FieldBase { self: AbstractField =>
    type ElemRepr

    def newArrayRepr(): Repr
    def append(repr: Repr, value: ElemRepr): Repr
    def iterator(repr: Repr): Iterator[ElemRepr]

    def default = newArrayRepr
    def elementBson(elem: ElemRepr): BsonValue
    final def fieldBson(value: Repr) =
      BsonArray(iterator(value).map(elementBson(_)).toSeq: _*)

    final def size(n: Long) = Filter.Size[Doc, this.type](this, n)
    final def containsAll(
                elems: Set[ElemRepr]): Filter.ContainsAll[Doc, this.type] =
      Filter.ContainsAll[Doc, this.type](this, elems)
    final def containsAll(
                elems: ElemRepr*): Filter.ContainsAll[Doc, this.type] =
      containsAll(elems.toSet)

    final def pushBack(elems: ElemRepr*) =
      Update.Push[Doc, this.type](this, elems)
    final def addToSet(elems: Set[ElemRepr]): Update.AddToSet[Doc, this.type] =
      Update.AddToSet[Doc, this.type](this, elems)
    final def addToSet(elems: ElemRepr*): Update.AddToSet[Doc, this.type] =
      addToSet(elems.toSet)
    final def pull(elems: Set[ElemRepr]): Update.Pull[Doc, this.type] =
      Update.Pull[Doc, this.type](this, elems)
    final def pull(elems: ElemRepr*): Update.Pull[Doc, this.type] =
      pull(elems.toSet)
    final def popFront() = Update.Pop[Doc, this.type](this, true)
    final def popBack() = Update.Pop[Doc, this.type](this, false)
  }

  trait SeqArrayField[S[X] <: Seq[X] with GenericTraversableTemplate[X, S]]
        extends ArrayFieldBase { self: AbstractField =>
    type Repr >: S[ElemRepr] <: Seq[ElemRepr]

    protected val seqFactory: SeqFactory[S]

    def newArrayRepr(): Repr = seqFactory.empty[ElemRepr]
    def append(repr: Repr, value: ElemRepr): Repr =
      seqFactory.concat(repr :+ value)
    def iterator(repr: Repr): Iterator[ElemRepr] = repr.iterator
  }

  trait SetArrayField[S[X] <: Set[X] with GenericSetTemplate[X, S]
                                     with SetLike[X, S[X]]]
        extends ArrayFieldBase { self: AbstractField =>
    type Repr >: S[ElemRepr] <: Set[ElemRepr]

    protected val setFactory: SetFactory[S]

    def newArrayRepr(): Repr = setFactory.empty[ElemRepr]
    def append(repr: Repr, value: ElemRepr): Repr =
      ((setFactory.newBuilder[ElemRepr] ++= repr) += value).result
    def iterator(repr: Repr): Iterator[ElemRepr] = repr.iterator
  }

  trait MapArrayField[M[K, V] <: Map[K, V] with MapLike[K, V, M[K, V]]]
        extends ArrayFieldBase { self: AbstractField =>
    type Key
    type Repr >: M[Key, ElemRepr] <: Map[Key, ElemRepr]

    protected val mapFactory: MapFactory[M]
    def elemKey(repr: ElemRepr): Key

    def newArrayRepr(): Repr = mapFactory.empty[Key, ElemRepr]
    def append(repr: Repr, value: ElemRepr): Repr =
      ((mapFactory.newBuilder[Key, ElemRepr] ++= repr) +=
       (elemKey(value) -> value)).result
    def iterator(repr: Repr): Iterator[ElemRepr] = repr.valuesIterator
  }

  trait PairsArrayField[M[K, V] <: Map[K, V] with MapLike[K, V, M[K, V]]]
        extends ArrayFieldBase { self: AbstractField =>
    type Key
    type Value
    type ElemRepr >: (Key, Value) <: (Key, Value)
    type Repr >: M[Key, Value] <: Map[Key, Value]

    protected val mapFactory: MapFactory[M]

    def newArrayRepr(): Repr = mapFactory.empty[Key, Value]
    def append(repr: Repr, value: ElemRepr): Repr =
      ((mapFactory.newBuilder[Key, Value] ++= repr) += value).result
    def iterator(repr: Repr): Iterator[ElemRepr] = repr.iterator
  }

  trait SortedSetArrayField[S[X] <: SortedSet[X] with SortedSetLike[X, S[X]]]
        extends ArrayFieldBase { self: AbstractField =>
    type Repr >: S[ElemRepr] <: SortedSet[ElemRepr]

    protected val elemOrdering: Ordering[ElemRepr]
    protected val setFactory: SortedSetFactory[S]

    def newArrayRepr(): Repr = setFactory.empty[ElemRepr](elemOrdering)
    def append(repr: Repr, value: ElemRepr): Repr =
      ((setFactory.newBuilder[ElemRepr](elemOrdering) ++= repr) += value).result
    def iterator(repr: Repr): Iterator[ElemRepr] = repr.iterator
  }

  trait SortedMapArrayField[
          M[K, V] <: SortedMap[K, V] with SortedMapLike[K, V, M[K, V]]]
        extends ArrayFieldBase { self: AbstractField =>
    type Key
    type Repr >: M[Key, ElemRepr] <: SortedMap[Key, ElemRepr]

    protected val keyOrdering: Ordering[Key]
    protected val mapFactory: SortedMapFactory[M]
    def elemKey(repr: ElemRepr): Key

    def newArrayRepr(): Repr = mapFactory.empty[Key, ElemRepr](keyOrdering)
    def append(repr: Repr, value: ElemRepr): Repr =
      ((mapFactory.newBuilder[Key, ElemRepr](keyOrdering) ++= repr) +=
       (elemKey(value) -> value)).result
    def iterator(repr: Repr): Iterator[ElemRepr] = repr.valuesIterator
  }

  sealed trait ElementsArrayFieldBase
               extends ArrayFieldBase
                  with ReprBsonValue { self: AbstractField =>
    final type ElemRepr = ValueRepr

    final def elementBson(elem: ElemRepr) = toBson(elem)

    final def contains(filter: ValueFilterBuilder[this.type] =>
                               ValueFilter[this.type]) =
      Filter.ContainsElem[Doc, this.type](
        this, filter(new ValueFilterBuilder[this.type](this)))

    final def toRaw(value: Repr) =
      scala.collection.JavaConversions.asIterable(
        iterator(value).map(e => Bson.toRaw(toBson(e))).toStream)
    final def fromRaw(value: AnyRef) = {
      import scala.collection.JavaConversions._
      var es = newArrayRepr
      val it: Iterator[AnyRef] = value match {
        case elems: java.lang.Iterable[_] =>
          elems.iterator.asInstanceOf[java.util.Iterator[AnyRef]]
        case elems: DBObject =>
          elems.toMap.valuesIterator.asInstanceOf[Iterator[AnyRef]]
      }
      it.foreach { e =>
        val elem = fromBson(Bson.fromRaw(e).asInstanceOf[Bson])
        es = append(es, elem)
      }
      es
    }
  }

  sealed trait DocumentsArrayFieldBase
               extends ArrayFieldBase
                  with StaticDocument
                  with Documents { self: AbstractField =>
    final type Coll = document.Coll
    final type ElemRepr = DocRepr

    final def elementBson(elem: ElemRepr) = toBson(elem)

    final def contains(filter: this.type => Filter[d.type] forSome {
                                 val d: this.type
                               }): Filter[Doc#Root] =
      Filter.Contains[Doc, this.type](
        this, filter(this).asInstanceOf[Filter[this.type]])

    final def foreach[IO <: Direction](
                spec: this.type => JsonSpec[d.type, IO]
                  forSome { val d: this.type }) =
      JsonSpec.InDocuments[Doc, this.type, IO](
        jsonMember, this, spec(this).asInstanceOf[JsonSpec[this.type, IO]],
        identity)
    final def transform(trans: Iterator[DocRepr] => Iterator[DocRepr]) =
      new JsonSpec.Transformed[Doc, this.type](jsonMember, this, trans)

    final def toRaw(value: Repr) =
      scala.collection.JavaConversions.asIterable(
        iterator(value).map(e => toDBObject(e)).toStream)
    final def fromRaw(value: AnyRef) = {
      import scala.collection.JavaConversions._
      var es = newArrayRepr
      val it: Iterator[DBObject] = value match {
        case elems: java.lang.Iterable[_] =>
          elems.iterator.asInstanceOf[java.util.Iterator[DBObject]]
        case elems: DBObject =>
          elems.toMap.valuesIterator.asInstanceOf[Iterator[DBObject]]
      }
      it.foreach { e =>
        val elem = toDBObject(create)
        elem.putAll(e)
        es = append(es, elem.repr)
      }
      es
    }
  }

  sealed trait DynamicFieldBase
               extends FieldBase
                  with DynamicDocument { self: AbstractField =>
    final type Coll = document.Coll
    final type Root = document.Root
    final type DocRepr = Repr

    def default = create
    final def fieldBson(value: Repr): BsonObject = toBson(value)

    final def toRaw(value: Repr): DBObject =
      new DynamicDocumentDBObject[this.type](this, value)
    final def fromRaw(value: AnyRef) = {
      val dbo = toDBObject(default)
      dbo.putAll(value.asInstanceOf[DBObject])
      dbo.repr
    }
  }

  trait MapDynamicField[M[K, V] <: Map[K, V] with MapLike[K, V, M[K, V]]]
        extends DynamicFieldBase { self: AbstractField =>
    type Repr >: M[FieldName, field.Repr] <: Map[FieldName, field.Repr]

    protected val mapFactory: MapFactory[M]

    protected final def newDocRepr = mapFactory.empty[FieldName, field.Repr]
    final def get(doc: DocRepr, name: FieldName) = doc.get(name)
    final def set(doc: DocRepr, name: FieldName, value: field.Repr) =
      ((mapFactory.newBuilder[FieldName, field.Repr] ++= doc) +=
       (name -> value)).result

    final def containsField(doc: DocRepr, name: FieldName) = doc.contains(name)
    final def fields(doc: DocRepr) = doc.iterator
    final def fieldsSeq(doc: DocRepr) = doc.iterator.toStream
    final def fieldsMap(doc: DocRepr) = doc
    final def fieldsNamesSet(doc: DocRepr) = new KeySet(doc)
  }

  def toBson(doc: DocRepr): BsonObject
  def toDBObject(doc: DocRepr): DocumentDBObject[document.type]

  def fieldSpec(name: FieldName): Option[JsonSpec.Single[document.type, InOut]]

  final def enter[IO <: Direction](
              name: String)(
              spec: JsonSpec.NotEmpty[d.type, IO]
                      forSome { val d: this.type }) =
    JsonSpec.InCustomEmbedding[this.type, IO](
               name, spec.asInstanceOf[JsonSpec.NotEmpty[this.type, IO]])
  final def putOpt(name: String)(conv: DocRepr => Option[JsonValue]) =
    JsonSpec.CustomFieldTo[this.type](name, this, conv)
  final def put(name: String)(conv: DocRepr => JsonValue) =
    putOpt(name)(d => Some(conv(d)))

  final def fromJson(
              spec: this.type =>
                    JsonSpec[d.type, In] forSome { val d: this.type }) =
    new JsonSpec.Reader[document.type](
          spec(this).asInstanceOf[JsonSpec[document.type, In]])
  def fromJson: JsonSpec.Reader[document.type]
  def fromJsonExcept(
              notFields: this.type =>
                         Fields[d.type] forSome { val d: this.type },
              ignoreUnknown: Boolean = false): JsonSpec.Reader[document.type]
  final def toJson(
              spec: this.type =>
                    JsonSpec[d.type, Out] forSome { val d: this.type }) =
    new JsonSpec.Writer[document.type](
          spec(this).asInstanceOf[JsonSpec[document.type, Out]])
  def toJson: JsonSpec.Writer[document.type]
  def toJsonExcept(
        notFields: this.type =>
                   Fields[d.type] forSome {
                       val d: this.type
                     }): JsonSpec.Writer[document.type]
}

final class DefaultStaticDocRepr[D <: StaticDocument](val docDef: D) {
  private val fields: Array[Any] = new Array[Any](docDef.staticFields.size)

  def get[F <: docDef.AbstractField](field: F): F#Repr =
    fields(field.fieldIndex).asInstanceOf[F#Repr]
  def set[F <: docDef.AbstractField](field: F, value: F#Repr): D#DocRepr = {
    fields(field.fieldIndex) = value
    this.asInstanceOf[D#DocRepr]
  }
}

object StaticDocument {
  val ObjectNameRegex = ".*\\$([a-zA-Z_][a-zA-Z0-9_]*)\\$".r
}

// TODO: Return the seal when Scala bug #3951 is fixed
/*sealed*/ trait StaticDocument extends Document { document =>
  final type FieldName = String

  final def nameToString(name: String) = name
  final def stringToName(str: String) = Some(str)

  private var fieldByName: Map[String, AbstractField] = Map.empty
  private var fieldByIndex: Seq[AbstractField] = Vector.empty
 
  final def staticFields = fieldByIndex
  final def fieldsNames(doc: DocRepr) = fieldByName.keysIterator
  final def containsField(doc: DocRepr, name: String) =
    fieldByName.contains(name)

  final def fields = fieldByIndex
  final def fieldsNamesSet = new KeySet(fieldByName)
  final def fieldsMap = fieldByName
  final def field(name: String): Option[AbstractField] = fieldByName.get(name)

  sealed abstract class AbstractField(name: String) extends FieldBase {
    final val fieldName = name match {
      case null => getClass.getSimpleName match {
        case StaticDocument.ObjectNameRegex(name) =>
          if (document.isCollection && name == "id") "_id" else name
        case _ =>
          throw new IllegalArgumentException
      }
      case _ => name
    }
    final val fieldNameString = fieldName
    final val fieldIndex = {
      if (fieldByName.contains(fieldName))
        throw new IllegalArgumentException(
                    "Field with name '" + fieldName + "' already exists")
      fieldByName += (fieldName -> this)
      fieldByIndex :+= this
      fieldByName.size - 1
    }

    final def getOpt(doc: DocRepr) = Some(get(doc))
  }

  final def create = staticFields.foldLeft(newDocRepr) { (doc, field) =>
    field.set(doc, field.default)
  }

  type DefaultDocRepr = DefaultStaticDocRepr[document.type]
  final def newDefaultDocRepr = new DefaultStaticDocRepr[document.type](this)

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

  abstract class BasicField[B <: BsonValue, R] private[StaticDocument](
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

  abstract class BasicFieldM[B <: BsonValue, R] private[StaticDocument](
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
                      with MandatoryEmbeddingFieldBase

  abstract class EmbeddingField[R](
                   getter: DocRepr => R, setter: (DocRepr, R) => DocRepr,
                   name: String = null)
                 extends Field(getter, setter, name)
                    with MandatoryEmbeddingFieldBase

  abstract class EmbeddingFieldM[R](
                   getter: DocRepr => R, setter: (DocRepr, R) => Unit,
                   name: String = null)
                 extends FieldM(getter, setter, name)
                    with MandatoryEmbeddingFieldBase

  abstract class EmbeddingFieldD[R](name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with MandatoryEmbeddingFieldBase {
    final type Repr = R
  }

  abstract class EmbeddingFieldDD(
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with MandatoryEmbeddingFieldBase {
    final type Repr = DefaultDocRepr
    final protected def newDocRepr() = newDefaultDocRepr
  }

  abstract class AbstractOptEmbeddingField(name: String)
                   extends AbstractField(name)
                      with OptEmbeddingFieldBase

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
    final protected def newDocRepr() = newDefaultDocRepr
    final def nullDocRepr = null
  }

  abstract class AbstractElementsArrayField(fieldName: String = null)
                   extends AbstractField(fieldName)
                      with ElementsArrayFieldBase

  abstract class ElementsArrayField[B <: BsonValue, R, C[X]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => DocRepr,
                   name: String = null)(
                   implicit fromRepr: R => B, toRepr: B => R,
                            bsonManifest: ClassManifest[B])
                 extends Field(getter, setter, name)
                    with ElementsArrayFieldBase {
    final type ValueRepr = R
    final type Bson = B

    final val bsonClass = bsonManifest.erasure.asInstanceOf[Class[B]]

    final def fromBson(bson: B): R = toRepr(bson)
    final def toBson(repr: R): B = fromRepr(repr)
  }

  abstract class ElementsArrayFieldM[B <: BsonValue, R, C[X]](
                   getter: DocRepr => C[R], setter: (DocRepr, C[R]) => Unit,
                   name: String = null)(
                   implicit fromRepr: R => B, toRepr: B => R,
                            bsonManifest: ClassManifest[B])
                 extends ElementsArrayField[B, R, C](
                           getter, (doc, c) => { setter(doc, c); doc }, name)

  abstract class ElementsArrayFieldD[B <: BsonValue, R, C[_]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr,
                            fromRepr: R => B, toRepr: B => R,
                            bsonManifest: ClassManifest[B])
                 extends FieldD(name) with ElementsArrayFieldBase {
    final type Repr = C[R]
    final type ValueRepr = R
    final type Bson = B

    final val bsonClass = bsonManifest.erasure.asInstanceOf[Class[B]]

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

  abstract class DocumentsMapArrayField[R, K, C[_, _]](
                   getter: DocRepr => C[K, R],
                   setter: (DocRepr, C[K, R]) => DocRepr,
                   name: String = null)
                 extends Field(getter, setter, name)
                    with DocumentsArrayFieldBase {
    final type DocRepr = R
    final type Key = K
  }

  abstract class DocumentsMapArrayFieldM[R, K, C[_, _]](
                   getter: DocRepr => C[K, R],
                   setter: (DocRepr, C[K, R]) => Unit,
                   name: String = null)
                 extends FieldM(getter, setter, name)
                    with DocumentsArrayFieldBase {
    final type DocRepr = R
    final type Key = K
  }

  abstract class DocumentsMapArrayFieldD[R, K, C[_, _]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with DocumentsArrayFieldBase {
    final type DocRepr = R
    final type Key = K
    final type Repr = C[K, R] 
  }

  abstract class DocumentsMapArrayFieldDD[K, C[_, _]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with DocumentsArrayFieldBase
                    with DefaultReprDocument {
    final type Key = K
    final type Repr = C[K, DocRepr] 
  }

  abstract class DocumentsPairsArrayField[K, V, C[_, _]](
                   getter: DocRepr => C[K, V],
                   setter: (DocRepr, C[K, V]) => DocRepr,
                   name: String = null)
                 extends Field(getter, setter, name)
                    with DocumentsArrayFieldBase {
    final type DocRepr = (K, V)
    final type Key = K
    final type Value = V
  }

  abstract class DocumentsPairsArrayFieldM[K, V, C[_, _]](
                   getter: DocRepr => C[K, V],
                   setter: (DocRepr, C[K, V]) => Unit,
                   name: String = null)
                 extends FieldM(getter, setter, name)
                    with DocumentsArrayFieldBase {
    final type DocRepr = (K, V)
    final type Key = K
    final type Value = V
  }

  abstract class DocumentsPairsArrayFieldD[K, V, C[_, _]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with DocumentsArrayFieldBase {
    final type DocRepr = (K, V)
    final type Key = K
    final type Value = V
    final type Repr = C[K, V] 
  }

  abstract class AbstractDynamicField(name: String = null)
                   extends AbstractField(name)
                      with DynamicFieldBase

  abstract class DynamicField[K, V, C[_, _]](
                   getter: DocRepr => C[K, V],
                   setter: (DocRepr, C[K, V]) => DocRepr,
                   name: String = null)
                 extends Field(getter, setter, name)
                    with DynamicFieldBase {
    final type FieldName = K

    val field: AbstractField with AnyRef {
                 type Repr = V
               }
  }

  abstract class DynamicFieldM[K, V, C[_, _]](
                   getter: DocRepr => C[K, V],
                   setter: (DocRepr, C[K, V]) => Unit,
                   name: String = null)
                 extends FieldM(getter, setter, name)
                    with DynamicFieldBase {
    final type FieldName = K

    val field: AbstractField with AnyRef {
                 type Repr = V
               }
  }

  abstract class DynamicFieldD[K](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with DynamicFieldBase {
    final type FieldName = K
  }

  final def toBson(doc: DocRepr) =
    new StaticDocumentBsonObject[document.type](document, doc)
  final def toDBObject(doc: DocRepr) =
    new StaticDocumentDBObject[document.type](document, doc)

  final def fieldSpec(name: FieldName) = field(name)

  final def jsonSpec: JsonSpec.NoDefault[this.type, InOut] =
    JsonSpec.Many[this.type, InOut](fieldByIndex)

  final def fromJson = new JsonSpec.Reader[this.type](jsonSpec)
  final def fromJsonExcept(
              notFields: this.type =>
                         Fields[d.type] forSome { val d: this.type },
              ignoreUnknown: Boolean) = {
    val except = notFields(this).fieldsSet.asInstanceOf[Set[FieldBase]]
    new JsonSpec.Reader[this.type](
          new JsonSpec.RestIn[this.type](
                except.map(_.fieldName), ignoreUnknown,
                JsonSpec.Many[this.type, In](
                  fieldByIndex.filterNot(except(_)))))
  }
  final def toJson = new JsonSpec.Writer[this.type](jsonSpec)
  final def toJsonExcept(
              notFields: this.type =>
                         Fields[d.type] forSome { val d: this.type }) = {
    val except = notFields(this).fieldsSet.asInstanceOf[Set[FieldBase]]
    new JsonSpec.Writer[this.type](
          new JsonSpec.Many[this.type, Out](
                fieldByIndex.filterNot(except(_))))
  }
}

final class DefaultDynamicDocRepr[D <: DynamicDocument](val docDef: D) {
  private val fields =
    new scala.collection.mutable.HashMap[docDef.FieldName, docDef.field.Repr]

  def get(name: docDef.FieldName): Option[docDef.field.Repr] = fields.get(name)
  def set(name: docDef.FieldName, value: docDef.field.Repr) = {
    fields.update(name, value)
    this.asInstanceOf[docDef.DocRepr]
  }
}

// TODO: Return the seal when Scala bug #3951 is fixed
/*sealed*/ trait DynamicDocument extends Document { document =>
  def nameToString(name: FieldName) = name.toString

  final def staticFields = Vector.empty
  def fieldsNames(doc: DocRepr): Iterator[FieldName] = fields(doc).map(_._1)
  final def containsField(doc: DocRepr, name: String): Boolean =
    stringToName(name).map(n => containsField(doc, n)).getOrElse(false)

  final type DefaultDocRepr = DefaultDynamicDocRepr[document.type]
  protected final def newDefaultDocRepr =
    new DefaultDynamicDocRepr[document.type](document)

  sealed abstract class AbstractField extends FieldBase {
    private[smogon] var fieldNameOpt: Option[FieldName] = None
    final lazy val fieldName = fieldNameOpt.get
    final lazy val fieldNameString = document.nameToString(fieldName)

    final def getOpt(doc: DocRepr) =
      document.get(doc, fieldName).asInstanceOf[Option[Repr]]
    final def get(doc: DocRepr) =
      document.get(doc, fieldName).get.asInstanceOf[Repr]
    final def set(doc: DocRepr, value: Repr) =
      document.set(doc, fieldName, value.asInstanceOf[field.Repr])
  }

  val field: AbstractField

  def fields(doc: DocRepr): Iterator[(FieldName, field.Repr)]
  def fieldsSeq(doc: DocRepr): Seq[(FieldName, field.Repr)]
  def fieldsMap(doc: DocRepr): Map[FieldName, field.Repr]
  def fieldsNamesSet(doc: DocRepr): Set[FieldName]

  def get(doc: DocRepr, name: FieldName): Option[field.Repr]
  def set(doc: DocRepr, name: FieldName, value: field.Repr): DocRepr

  final def create = newDocRepr

  final def apply(name: FieldName): field.type = {
    val f = field.getClass.newInstance.asInstanceOf[AbstractField]
    f.fieldNameOpt = Some(name)
    f.asInstanceOf[field.type]
  }

  abstract class AbstractBasicField extends AbstractField
                                       with BasicFieldBase

  abstract class BasicField[B <: BsonValue, R]()(
                   implicit fromRepr: R => B, toRepr: B => R,
                            bsonManifest: ClassManifest[B])
                 extends AbstractField
                    with BasicFieldBase {
    final type Repr = R
    final type Bson = B

    final val bsonClass = bsonManifest.erasure.asInstanceOf[Class[B]]

    final def fromBson(bson: B): R = toRepr(bson)
    final def toBson(repr: R): B = fromRepr(repr)

    def default = fromBson(Bson.default[B])
  }

  class BoolField[R]()(
          implicit fromRepr: R => BsonBool, toRepr: BsonBool => R)
        extends BasicField[BsonBool, R]
  class OptBoolField[R]()(implicit fromRepr: R => OptBsonBool,
                                   toRepr: OptBsonBool => R)
        extends BasicField[OptBsonBool, R]

  class IntField[R]()(
          implicit fromRepr: R => BsonInt, toRepr: BsonInt => R)
        extends BasicField[BsonInt, R]
  class OptIntField[R]()(
          implicit fromRepr: R => OptBsonInt, toRepr: OptBsonInt => R)
        extends BasicField[OptBsonInt, R]

  class LongField[R]()(
          implicit fromRepr: R => BsonLong, toRepr: BsonLong => R)
        extends BasicField[BsonLong, R]
  class OptLongField[R]()(
          implicit fromRepr: R => OptBsonLong, toRepr: OptBsonLong => R)
        extends BasicField[OptBsonLong, R]

  class DoubleField[R]()(
          implicit fromRepr: R => BsonDouble, toRepr: BsonDouble => R)
        extends BasicField[BsonDouble, R]
  class OptDoubleField[R]()(
          implicit fromRepr: R => OptBsonDouble, toRepr: OptBsonDouble => R)
        extends BasicField[OptBsonDouble, R]

  class StringField[R]()(
          implicit fromRepr: R => BsonStr, toRepr: BsonStr => R)
        extends BasicField[BsonStr, R]
  class OptStringField[R]()(
          implicit fromRepr: R => OptBsonStr, toRepr: OptBsonStr => R)
        extends BasicField[OptBsonStr, R]

  class DateField[R]()(
          implicit fromRepr: R => BsonDate, toRepr: BsonDate => R)
        extends BasicField[BsonDate, R]
  class OptDateField[R]()(
          implicit fromRepr: R => OptBsonDate, toRepr: OptBsonDate => R)
        extends BasicField[OptBsonDate, R]

  class IdField[R]()(
          implicit fromRepr: R => BsonId, toRepr: BsonId => R)
        extends BasicField[BsonId, R]
  class OptIdField[R]()(
          implicit fromRepr: R => OptBsonId, toRepr: OptBsonId => R)
        extends BasicField[OptBsonId, R]

  final def toBson(doc: DocRepr) =
    new DynamicDocumentBsonObject[document.type](document, doc)
  final def toDBObject(doc: DocRepr) =
    new DynamicDocumentDBObject[document.type](document, doc)

  final def fieldSpec(name: FieldName) =
    Some(new JsonSpec.DynamicField[document.type](document, name))

  final def fromJson =
    new JsonSpec.Reader[document.type](
          new JsonSpec.RestIn[document.type](
                Set.empty, false,
                new JsonSpec.Empty[document.type](document)))
  final def fromJsonExcept(
              notFields: this.type =>
                         Fields[d.type] forSome { val d: this.type },
              ignoreUnknown: Boolean) =
    new JsonSpec.Reader[document.type](
          new JsonSpec.RestIn[document.type](
                notFields(this).fieldsSet.map(_.fieldName), ignoreUnknown,
                new JsonSpec.Empty[document.type](document)))
  final def fromJsonExcept(
              ignoreUnknown: Boolean,
              notFields: FieldName*): JsonSpec.Reader[document.type] =
    new JsonSpec.Reader[document.type](
          new JsonSpec.RestIn[document.type](
                notFields.toSet, ignoreUnknown,
                new JsonSpec.Empty[document.type](document)))
  final def fromJsonExcept(
              notFields: FieldName*): JsonSpec.Reader[document.type]  =
    fromJsonExcept(false, notFields: _*)
  final def toJson =
    new JsonSpec.Writer[document.type](
          new JsonSpec.RestOut[document.type](
                Set.empty, new JsonSpec.Empty[document.type](document)))
  final def toJsonExcept(
              notFields: this.type =>
                         Fields[d.type] forSome { val d: this.type }) =
    new JsonSpec.Writer[document.type](
          new JsonSpec.RestOut[document.type](
                notFields(this).fieldsSet.map(_.fieldName),
                new JsonSpec.Empty[document.type](document)))
}

sealed trait Safety
object Safety {
  object Default extends Safety
  sealed trait Concrete extends Safety {
    val value: Int
  }
  object Concrete {
    def unapply(x: Safety): Option[Int] = x match {
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
  sealed case class Safe(slaves: Int = 0, timeout: Int = 0) extends Concrete {
    require(slaves >= 0)
    require(timeout >= 0)
    val value = 1 + slaves
  }
  object Safe extends Safe(0, 0)
}

object Collection {
  private[smogon] val logger = LoggerFactory.getLogger(classOf[Collection])
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

trait Collection extends StaticDocument with Documents {
  import Collection._

  final type Coll = this.type

  val id: BasicFieldBase

  final def defaultSafetyOf(dbc: DBCollection): Safety.Concrete = safetyOf(dbc)
  final def defaultSafetyOf(dbc: DBCollection, safety: Safety.Concrete) =
    dbc.setWriteConcern(writeConcern(safety))

  final def insertInto(dbc: DBCollection, doc: DocRepr,
                       safety: Safety = Safety.Default,
                       timeout: Int = 0): DocRepr = {
    val dbo = toDBObject(doc)
    val cs = safetyOf(dbc, safety)
    if (logger.isTraceEnabled)
      logger.trace(dbc.getName + ".insert(" + dbo + "), safety=" + cs)
    handleErrors(dbc.insert(dbo, writeConcern(cs)))
    dbo.repr
  }

  final def saveInto(dbc: DBCollection, doc: DocRepr,
                     safety: Safety = Safety.Default,
                     timeout: Int = 0): DocRepr = {
    val dbo = toDBObject(doc)
    val cs = safetyOf(dbc, safety)
    if (logger.isTraceEnabled)
      logger.trace(dbc.getName + ".save(" + dbo + "), safety=" + cs)
    handleErrors(dbc.save(dbo, writeConcern(cs)))
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
