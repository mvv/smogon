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

sealed trait Direction
sealed trait In extends Direction
sealed trait Out extends Direction
sealed trait InOut extends In with Out

sealed trait JsonSpec[D <: Document, +IO <: Direction] {
  def jsonDocument: D
  def jsonMembers: Iterator[JsonSpec.Single[D, IO]] 
}

object JsonSpec {
  sealed trait Verdict
  object Ignore extends Verdict
  object Raise extends Verdict

  sealed class ReadingException(message: String, cause: Throwable)
                 extends RuntimeException(message, cause) {
    def this(message: String) = this(message, null)
  }
  case class UnexpectedFieldException(name: String)
             extends ReadingException("Unexpected field '" + name + "'")
  case class MissingFieldException(name: String)
             extends ReadingException("Mandatory field '" + name +
                                      "' is missing")
  case class IllegalFieldValueException(name: String, value: JsonValue,
                                        cause: Throwable)
        extends ReadingException(
                  "Field '" + name + "' has illegal value '" +
                  value.serialize.mkString + "'") {
    def this(name: String, value: JsonValue) = this(name, value, null)
  }

  final class Reader[D <: Document](spec: JsonSpec[D, In])
              extends (JsonObject => D#DocRepr) {
    private def process[DD <: Document](
                 path: Seq[String], jo: JsonObject, spec: JsonSpec[DD, In],
                 repr: DD#DocRepr): DD#DocRepr = {
      val d = spec.jsonDocument
      var doc = repr.asInstanceOf[d.DocRepr]

      def basicField[F <: DDD#BasicFieldBase forSome { type DDD <: Document }](
            field: F, name: String, value: JsonValue) {
        val f = field.asInstanceOf[d.BasicFieldBase]
        val r = try {
                  f.fromBson(Bson.fromJson(value, f.bsonClass))
                } catch {
                  case e: Exception =>
                    throw new IllegalFieldValueException(
                                (path :+ name).mkString("."), value, e)
                }
        doc = f.set(doc, r)
      }
      def embeddingFieldWith[F <: DD#EmbeddingFieldBase](
            field: F, name: String, obj: JsonObject, spec: JsonSpec[F, In]) {
        val f = field.asInstanceOf[d.EmbeddingFieldBase]
        doc = f.set(doc, process[f.type](
                           path :+ name, obj,
                           spec.asInstanceOf[JsonSpec[f.type, In]]))
      }
      def embeddingField[F <: DDD#EmbeddingFieldBase
                                forSome { type DDD <: Document }](
            field: F, name: String, obj: JsonObject) {
        val f = field.asInstanceOf[d.EmbeddingFieldBase]
        doc = f.set(doc, process[f.type](path :+ name, obj, f.jsonSpec))
      }
      def optEmbeddingField[F <: DDD#OptEmbeddingFieldBase
                                   forSome { type DDD <: Document }](
            field: F, name: String) {
        val f = field.asInstanceOf[d.OptEmbeddingFieldBase]
        doc = f.set(doc, f.nullDocRepr)
      }
      def documentsFieldWith[F <: DDD#DocumentsArrayFieldBase
                                    forSome { type DDD <: Document }](
            field: F, name: String, array: JsonArray, spec: JsonSpec[F, In]) {
        val f = field.asInstanceOf[d.DocumentsArrayFieldBase]
        var elems = f.newArrayRepr
        array.iterator.zipWithIndex.foreach {
          case (obj: JsonObject, _) =>
            val elem = process[f.type](
                         path :+ name, obj,
                         spec.asInstanceOf[JsonSpec[f.type, In]])
            elems = f.append(elems, elem)
          case (value, i) =>
            throw new IllegalFieldValueException(
                        (path :+ name).mkString(".") + "[" + i + "]", value)
        }
        doc = f.set(doc, elems)
      }
      def documentsField[F <: DDD#DocumentsArrayFieldBase
                                forSome { type DDD <: Document }](
            field: F, name: String, array: JsonArray) {
        val f = field.asInstanceOf[d.DocumentsArrayFieldBase]
        documentsFieldWith[f.type](f, name, array, f.jsonSpec)
      }
      def customEmbedding(
            name: String, obj: JsonObject, spec: NoDefault[DD, In]) {
        doc = process[d.type](
                path :+ name, obj,
                spec.asInstanceOf[NoDefault[d.type, In]], doc)
      }
      def liftedFromField[F <: DD#EmbeddingFieldBase](
            field: F, name: String, value: JsonValue, spec: Single[F, In]) {
        val f = field.asInstanceOf[d.EmbeddingFieldBase]
        val r = process[f.type](path, JsonObject(name -> value),
                                spec.asInstanceOf[Single[f.type, In]],
                                f.get(doc))
        doc = f.set(doc, r)
      }

      val hl = spec.asInstanceOf[HasLookup[DD, In]]
      var seen = Set[String]()
      jo.iterator.foreach { case (name, value) =>
        hl.jsonMemberSpec(name) match {
          case Right(s) =>
            seen += name
            (s match {
              case OptMember(spec, _) => spec
              case _ => s
            }) match {
              case DocumentField(_, BasicField(field)) =>
                basicField(field, name, value)
              case DocumentField(_, EmbeddingField(field)) =>
                (value, field) match {
                  case (obj: JsonObject, field) =>
                    embeddingField(field, name, obj)
                  case (JsonNull, OptEmbeddingField(field)) =>
                    optEmbeddingField(field, name)
                  case _ =>
                    throw new IllegalFieldValueException(
                                (path :+ name).mkString("."), value)
                }
              case DocumentField(_, DocumentsArrayField(field)) =>
                value match {
                  case array: JsonArray =>
                    documentsField(field, name, array)
                  case _ =>
                    throw new IllegalFieldValueException(
                                (path :+ name).mkString("."), value)
                }
              case InEmbedding(_, field, spec) =>
                (value, field) match {
                  case (obj: JsonObject, field) =>
                    embeddingFieldWith(field, name, obj, spec)
                  case (JsonNull, OptEmbeddingField(field)) =>
                    optEmbeddingField(field, name)
                  case _ =>
                    throw new IllegalFieldValueException(
                                (path :+ name).mkString("."), value)
                }
              case InDocuments(_, field, spec, _) =>
                value match {
                  case array: JsonArray =>
                    documentsFieldWith(field, name, array, spec)
                  case _ =>
                    throw new IllegalFieldValueException(
                                (path :+ name).mkString("."), value)
                }
              case InCustomEmbedding(_, spec) =>
                value match {
                  case obj: JsonObject =>
                    customEmbedding(name, obj, spec)
                  case _ =>
                    throw new IllegalFieldValueException(
                                (path :+ name).mkString("."), value)
                }
              case Lifted(field, spec) =>
                liftedFromField(field, name, value, spec)
            }
          case Left(Raise) =>
            throw new UnexpectedFieldException((path :+ name).mkString("."))
          case Left(Ignore) =>
        }
      }
      hl.jsonMembers.foreach { single =>
        if (!seen.contains(single.jsonMember)) {
          single match {
            case OptMember(_, alt) =>
              doc = alt(doc).asInstanceOf[d.DocRepr]
            case _ =>
              throw new MissingFieldException(
                          (path :+ single.jsonMember).mkString("."))
          }
        }
      }
      doc
    }

    private def process[DD <: Document](
                 path: Seq[String],
                 jo: JsonObject, spec: JsonSpec[DD, In]): DD#DocRepr = {
      var doc = spec.jsonDocument.create
      process(path, jo, spec, doc)
    }

    def apply(jo: JsonObject): D#DocRepr = process(Vector.empty, jo, spec)
  }

  final class Writer[D <: Document](spec: JsonSpec[D, Out])
              extends (D#DocRepr => JsonObject) {
    private def process[DD <: Document](
                 spec: JsonSpec[DD, Out], repr: DD#DocRepr): JsonObject = {
      val d = spec.jsonDocument
      val dr = repr.asInstanceOf[d.DocRepr]
      JsonObject(spec.jsonMembers.map { s =>
        (s match {
          case CondOut(spec, test) => if (test(dr)) Some(spec) else None
          case spec => Some(spec)
        }) .flatMap {
          case ConvTo(_, field, conv) =>
            val f = field.asInstanceOf[d.BasicFieldBase]
            conv(f.get(dr))
          case CustomFieldTo(_, _, conv) =>
            conv(dr)
          case DocumentField(_, field) => field match {
            case BasicField(field) =>
              val f = field.asInstanceOf[d.BasicFieldBase]
              Some(Bson.toJson(f.toBson(f.get(dr))))
            case EmbeddingField(field) =>
              if (field.isInstanceOf[d.OptEmbeddingFieldBase]) {
                val f = field.asInstanceOf[d.OptEmbeddingFieldBase]
                val doc = f.get(dr)
                Some(if (f.isNull(doc)) JsonNull else f.toJson(doc))
              } else {
                val f = field.asInstanceOf[d.EmbeddingFieldBase]
                Some(f.toJson(f.get(dr)))
              }
            case DocumentsArrayField(field) =>
              val f = field.asInstanceOf[d.DocumentsArrayFieldBase]
              Some(JsonArray(f.iterator(f.get(dr)).map(f.toJson(_))))
          }
          case InEmbedding(_, field, spec) =>
            if (field.isInstanceOf[d.OptEmbeddingFieldBase]) {
              val f = field.asInstanceOf[d.OptEmbeddingFieldBase]
              val r = f.get(dr)
              Some {
                if (f.isNull(r))
                  JsonNull
                else
                  process[f.type](spec.asInstanceOf[JsonSpec[f.type, Out]], r)
              }
            } else {
              val f = field.asInstanceOf[d.EmbeddingFieldBase]
              val r = f.get(dr)
              Some(
                process[f.type](spec.asInstanceOf[JsonSpec[f.type, Out]], r))
            }
          case InDocuments(_, field, spec, trans) =>
            val f = field.asInstanceOf[d.DocumentsArrayFieldBase]
            val elems = f.get(dr)
            Some(JsonArray(trans(f.iterator(elems)).map { elem =>
                process[f.type](spec.asInstanceOf[JsonSpec[f.type, Out]],
                                elem.asInstanceOf[f.DocRepr])
              }))
          case InCustomEmbedding(_, spec) =>
            Some(
              process[d.type](spec.asInstanceOf[NoDefault[d.type, Out]], dr))
          case Lifted(field, spec) =>
            if (field.isInstanceOf[d.OptEmbeddingFieldBase] && {
                  val f = field.asInstanceOf[d.OptEmbeddingFieldBase]
                  f.isNull(f.get(dr))
                })
              None
            else {
              val f = field.asInstanceOf[d.EmbeddingFieldBase]
              process[f.type](spec.asInstanceOf[Single[f.type, Out]],
                              f.get(dr)).iterator.toSeq.headOption.map(_._2)
            }
        } .map(v => s.jsonMember -> v)
      } .filter(_.isDefined).map(_.get))
    }

    def apply(repr: D#DocRepr): JsonObject = process(spec, repr)
  }

  sealed trait HasLookup[D <: Document, +IO <: Direction]
               extends JsonSpec[D, IO] {
    def jsonMemberSpec(name: String): Either[Verdict, Single[D, IO]]
  }

  final case class IgnoreRestIn[D <: Document](spec: NoDefault[D, In])
                   extends HasLookup[D, In] {
    def jsonDocument = spec.jsonDocument 
    def jsonMemberSpec(name: String) = spec.jsonMemberSpec(name) match {
      case Left(Raise) => Left(Ignore)
      case v => v
    }
    def jsonMembers = spec.jsonMembers
  }
  final case class RestIn[D <: Document](
                     notFields: Set[D#FieldBase],
                     ignoreUnknown: Boolean,
                     spec: NoDefault[D, In]) extends HasLookup[D, In] {
    def jsonDocument = spec.jsonDocument 
    def jsonMemberSpec(name: String) = spec.jsonMemberSpec(name) match {
      case Left(Raise) => jsonDocument.field(name) match {
        case Some(field) =>
          if (spec.jsonFields.contains(field) || notFields.contains(field))
            Left(Raise)
          else
            Right(field.asInstanceOf[Single[D, In]])
        case None =>
          if (ignoreUnknown)
            Left(Ignore)
          else
            Left(Raise)
      }
      case v => v
    }
    def jsonMembers = spec.jsonMembers ++ {
      jsonDocument.fields.iterator.filter { field =>
        !spec.jsonFields.contains(field) && !notFields.contains(field)
      } .asInstanceOf[Iterator[Single[D, In]]]
    }
  }
  final class RestOut[D <: Document](
                notFields: Set[D#FieldBase],
                spec: NoDefault[D, Out]) extends JsonSpec[D, Out] {
    def jsonDocument = spec.jsonDocument 
    def jsonMembers = spec.jsonMembers ++ {
      jsonDocument.fields.iterator.filter { field =>
        !spec.jsonFields.contains(field) && !notFields.contains(field)
      } .asInstanceOf[Iterator[Single[D, Out]]]
    }
  }

  sealed trait NoDefault[D <: Document, +IO <: Direction]
               extends HasLookup[D, IO] {
    def jsonFields: Set[D#FieldBase]
    def jsonMembersMap: Map[String, Single[D, IO]]

    def jsonMemberSpec(name: String) = jsonMembersMap.get(name) match {
      case Some(spec) => Right(spec)
      case None => Left(Raise)
    }
    def jsonMembers = jsonMembersMap.valuesIterator

    def >>(spec: NoDefault[D, Out])(
           implicit witness: IO <:< Out): NoDefault[D, Out]
    def <<(spec: NoDefault[D, In])(
           implicit witness: IO <:< In): NoDefault[D, In]
    def >>!(notFields: D => Seq[_ <: D#FieldBase])(
            implicit witness: IO <:< Out) =
      new RestOut(notFields(jsonDocument).toSet[D#FieldBase],
                  this.asInstanceOf[NoDefault[D, Out]])
    def >>*()(implicit witness: IO <:< Out) = this >>! (d => Nil)
    def <<!(notFields: D => Seq[_ <: D#FieldBase],
            ignoreUnknown: Boolean = false)(
            implicit witness: IO <:< In) =
      new RestIn(notFields(jsonDocument).toSet[D#FieldBase],
                 ignoreUnknown, this.asInstanceOf[NoDefault[D, In]])
    def <<*()(implicit witness: IO <:< In) =
      this <<! (d => Nil, false)
    def <<*#()(implicit witness: IO <:< In) =
      this <<! (d => Nil, true)
    def <<#()(implicit witness: IO <:< In) =
      new IgnoreRestIn[D](this.asInstanceOf[NoDefault[D, In]])
  }
  object NoDefault {
    def unapply[D <: Document, IO <: Direction](
          spec: NoDefault[D, IO]): Option[NoDefault[D, IO]] = Some(spec)
  }
  sealed trait Single[D <: Document, +IO <: Direction]
               extends NoDefault[D, IO] {
    def jsonMember: String
    def jsonMembersMap = Map(jsonMember -> this)

    def >>(spec: NoDefault[D, Out])(
           implicit witness: IO <:< Out): NoDefault[D, Out] = spec match {
      case Many(specs) =>
        Many[D, Out](this.asInstanceOf[Single[D, Out]] +: specs)
      case spec =>
        Many(Seq(this.asInstanceOf[Single[D, Out]],
                 spec.asInstanceOf[Single[D, Out]]))
    }
    def <<(spec: NoDefault[D, In])(
           implicit witness: IO <:< In): NoDefault[D, In] = spec match {
      case Many(specs) =>
        Many[D, In](this.asInstanceOf[Single[D, In]] +: specs)
      case spec =>
        Many(Seq(this.asInstanceOf[Single[D, In]],
                 spec.asInstanceOf[Single[D, In]]))
    }
    def onlyIf(test: D#DocRepr => Boolean)(
               implicit witness: IO <:< Out) =
      CondOut(this.asInstanceOf[Single[D, Out]], test)
  }
  object Single {
    def unapply[D <: Document, IO <: Direction](
          spec: Single[D, IO]): Option[Single[D, IO]] = Some(spec)
  }
  final case class Many[D <: Document, IO <: Direction](
                     specs: Seq[Single[D, IO]])
                   extends NoDefault[D, IO] {
    def jsonDocument = specs(0).jsonDocument
    lazy val jsonFields = specs.map(_.jsonFields).reduceLeft(_ ++ _)
    lazy val jsonMembersMap = specs.map(_.jsonMembersMap).reduceLeft(_ ++ _)

    def >>(spec: NoDefault[D, Out])(implicit witness: IO <:< Out) = spec match {
      case Many(ss) =>
        Many[D, Out](specs.asInstanceOf[Seq[Single[D, Out]]] ++ ss)
      case spec =>
        Many(specs.asInstanceOf[Seq[Single[D, Out]]] :+
             spec.asInstanceOf[Single[D, Out]])
    }
    def <<(spec: NoDefault[D, In])(implicit witness: IO <:< In) = spec match {
      case Many(ss) =>
        Many[D, In](specs.asInstanceOf[Seq[Single[D, In]]] ++ ss)
      case spec =>
        Many(specs.asInstanceOf[Seq[Single[D, In]]] :+
             spec.asInstanceOf[Single[D, In]])
    }
  }

  final case class CondOut[D <: Document](
                     spec: Single[D, Out], test: D#DocRepr => Boolean)
                   extends Single[D, Out] {
    def jsonDocument = spec.jsonDocument
    def jsonMember = spec.jsonMember
    def jsonFields = spec.jsonFields
  }
  final case class OptMember[D <: Document](
                     spec: Single[D, In], alt: D#DocRepr => D#DocRepr)
                   extends Single[D, In] {
    def jsonDocument = spec.jsonDocument
    def jsonMember = spec.jsonMember
    def jsonFields = spec.jsonFields
  }
  sealed trait Member[D <: Document, +IO <: Direction] extends Single[D, IO] {
    def jsonFields = Set[D#FieldBase]()

    def orElse(alt: D#DocRepr => D#DocRepr)(
               implicit witness: IO <:< In): OptMember[D] =
      new OptMember[D](this.asInstanceOf[Single[D, In]], alt)
    def ?()(implicit witness: IO <:< In): OptMember[D] = this orElse (d => d)
  }
  sealed trait Field[D <: Document, +IO <: Direction] extends Member[D, IO] {
    def jsonField: D#FieldBase
    def jsonDocument = jsonField.fieldDocument
    override def jsonFields = Set(jsonField)
    def jsonMember = jsonField.fieldName
  }

  final case class Renamed[D <: Document, F <: D#FieldBase](
                     name: String, field: F)
                   extends Field[D, InOut] {
    def jsonField: F = field
    override def jsonMember = name

    def toOpt(conv: F#Repr => Option[JsonValue]) =
      ConvTo[D, F](name, field, conv)
    def to(conv: F#Repr => JsonValue) = toOpt(r => Some(conv(r)))
    def from(conv: PartialFunction[JsonValue, F#Repr]) =
      ConvFrom[D, F](name, field, conv)
    def enter[IO <: Direction](
          spec: F => JsonSpec[d.type, IO] forSome { val d: F })(
          implicit witness: F <:< D#EmbeddingFieldBase): Field[D, IO] = {
      val embed = witness(field)
      InEmbedding[D, embed.type, IO](
        name, embed, spec(field).asInstanceOf[JsonSpec[embed.type, IO]])
    }
    def foreach[IO <: Direction](
          spec: F => JsonSpec[d.type, IO] forSome { val d: F })(
          implicit witness: F <:< D#DocumentsArrayFieldBase): Field[D, IO] = {
      val array = witness(field)
      InDocuments[D, array.type, IO](
        name, array, spec(field).asInstanceOf[JsonSpec[array.type, IO]],
        identity)
    }
  }
  object DocumentField {
    def unapply[D <: Document, IO <: Direction](
          spec: Single[D, IO]): Option[(String, D#FieldBase)] = {
      def cast[DD <: Document](field: DD#FieldBase): D#FieldBase =
        field.asInstanceOf[D#FieldBase]
      spec match {
        case Field(field) => Some((field.fieldName, cast(field)))
        case Renamed(name, field) => Some((name, cast(field)))
        case _ => None
      }
    }
  }
  final case class ConvTo[D <: Document, F <: D#FieldBase](
                     name: String, field: F, conv: F#Repr => Option[JsonValue])
                   extends Field[D, Out] {
    def jsonField: F = field
    override def jsonMember = name
  }
  final case class ConvFrom[D <: Document, F <: D#FieldBase](
                     name: String, field: F,
                     conv: PartialFunction[JsonValue, F#Repr])
                   extends Field[D, In] {
    def jsonField: F = field
    override def jsonMember = name
  }
  final case class InCustomEmbedding[D <: Document, IO <: Direction](
                     name: String, spec: NoDefault[D, IO])
                   extends Member[D, IO] {
    def jsonDocument = spec.jsonDocument
    def jsonMember = name
    override def jsonFields = spec.jsonFields
  }
  final case class CustomFieldFrom[D <: Document](
                     name: String, val jsonDocument: D,
                     conv: PartialFunction[(D#DocRepr, JsonValue), D#DocRepr])
                   extends Member[D, In] {
    def jsonMember = name
  }
  final case class CustomFieldTo[D <: Document](
                     name: String, val jsonDocument: D,
                     conv: D#DocRepr => Option[JsonValue])
                   extends Member[D, Out] {
    def jsonMember = name
  }
  final case class InEmbedding[D <: Document, F <: D#EmbeddingFieldBase,
                               IO <: Direction](
                     name: String, field: F, spec: JsonSpec[F, IO])
                   extends Field[D, IO] {
    def jsonField: F = field 
    override def jsonMember = name
  }
  final class Transformed[D <: Document, F <: D#DocumentsArrayFieldBase](
                     name: String, field: F,
                     trans: Iterator[F#DocRepr] => Iterator[F#DocRepr])
                   extends InDocuments[D, F, Out](
                             name, field,
                             field.jsonSpec.asInstanceOf[JsonSpec[F, Out]],
                             trans) {
    final def foreach(
                spec: F => JsonSpec[d.type, Out] forSome { val d: F }) =
      JsonSpec.InDocuments[D, F, Out](
        jsonMember, field, spec(field).asInstanceOf[JsonSpec[F, Out]],
        trans)
  }
  sealed case class InDocuments[D <: Document, F <: D#DocumentsArrayFieldBase,
                                +IO <: Direction](
                      name: String, field: F, spec: JsonSpec[F, IO],
                      trans: Iterator[F#DocRepr] => Iterator[F#DocRepr])
                    extends Field[D, IO] {
    def jsonField: F = field 
    override def jsonMember = name
  }
  final case class Lifted[D <: Document, F <: D#EmbeddingFieldBase,
                          +IO <: Direction](
                     field: F, spec: Single[F, IO])
                   extends Single[D, IO] {
    def jsonDocument = field.jsonDocument
    def jsonMember = spec.jsonMember
    def jsonFields = Set[D#FieldBase]()
  }
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
                            with JsonSpec.Field[document.type, InOut] { field =>
    final type Doc = document.type
    type Repr

    val fieldIndex: Int
    def fieldDocument: Doc = document

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

    final def jsonField: this.type = this
    final def as(name: String) = JsonSpec.Renamed[Doc, this.type](name, this)
    final def toOpt(conv: Repr => Option[JsonValue]) =
      JsonSpec.ConvTo[Doc, this.type](fieldName, this, conv)
    final def to(conv: Repr => JsonValue) = toOpt(r => Some(conv(r)))
    final def from(conv: PartialFunction[JsonValue, Repr]) =
      JsonSpec.ConvFrom[Doc, this.type](fieldName, this, conv)
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

    def ===(value: Repr) = Filter.Eq[Doc, this.type](this, value)
    def !==(value: Repr) = !(this === value)
    def in(values: Set[Repr]) = Filter.In[Doc, this.type](this, values)
    def notIn(values: Set[Repr]) = !(this in values)
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

  trait MapArrayField[M[K, V] <: Map[K, V] with MapLike[K, V, M[K, V]]]
          extends AbstractField with ArrayFieldBase {
    type Key
    type Repr >: M[Key, ElemRepr] <: Map[Key, ElemRepr]

    protected def mapFactory: MapFactory[M]
    protected def elemKey(repr: ElemRepr): Key

    protected def newArrayRepr(): Repr = mapFactory.empty[Key, ElemRepr]
    protected def append(repr: Repr, value: ElemRepr): Repr =
      ((mapFactory.newBuilder[Key, ElemRepr] ++= repr) +=
       (elemKey(value) -> value)).result
    def iterator(repr: Repr): Iterator[ElemRepr] = repr.valuesIterator
  }

  sealed trait ElementsArrayFieldBase extends ArrayFieldBase with ReprBsonValue {
    final type ElemRepr = ValueRepr

    final def contains(filter: ValueFilterBuilder[this.type] =>
                               ValueFilter[this.type]) =
      Filter.ContainsElem[Doc, this.type](
        this, filter(new ValueFilterBuilder[this.type]))
  }

  trait DocumentsArrayFieldBase extends ArrayFieldBase with Documents {
    final type Coll = document.Coll
    final type ElemRepr = DocRepr

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
                   fieldName: String = null)(
                   implicit fromRepr: R => B, toRepr: B => R)
                 extends Field(getter, setter, fieldName)
                    with ElementsArrayFieldBase {
    final type ValueRepr = R
    final type Bson = B

    final def fromBson(bson: B): R = toRepr(bson)
    final def toBson(repr: R): B = fromRepr(repr)
  }

  abstract class ElementsArrayFieldD[B <: BsonValue, R, C[_]](
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
                 extends ElementsArrayField[BsonBool, R, C](
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

  abstract class DocumentsMapField[R, K, C[_, _]](
                   getter: DocRepr => C[K, R],
                   setter: (DocRepr, C[K, R]) => DocRepr,
                   name: String = null)
                 extends Field(getter, setter, name)
                    with DocumentsArrayFieldBase {
    final type Key = K
  }

  abstract class DocumentsMapFieldM[R, K, C[_, _]](
                   getter: DocRepr => C[K, R],
                   setter: (DocRepr, C[K, R]) => Unit,
                   name: String = null)
                 extends FieldM(getter, setter, name)
                    with DocumentsArrayFieldBase {
    final type Key = K
  }

  abstract class DocumentsMapFieldD[R, K, C[_, _]](
                   name: String = null)(
                   implicit witness: DocRepr =:= DefaultDocRepr)
                 extends FieldD(name)
                    with DocumentsArrayFieldBase {
    final type Key = K
    final type Repr = C[K, R] 
    final type DocRepr = R
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
      case Some(field) => field match {
          case field: BasicFieldBase =>
            doc = field.set(doc, field.fromBson(Bson.fromRaw(value).
                                   asInstanceOf[field.Bson]))
          case field: EmbeddingFieldBase =>
            val dbo = field.dbObject(field.create)
            dbo.putAll(value.asInstanceOf[DBObject])
            doc = field.set(doc, dbo.repr)
          case field: ElementsArrayFieldBase =>
          case field: DocumentsArrayFieldBase =>
            var ds = field.newArrayRepr
            value match {
              case elems: java.lang.Iterable[_] =>
                elems.foreach { e =>
                  val d = field.dbObject(field.create)
                  d.putAll(e.asInstanceOf[DBObject])
                  ds = field.append(ds, d.repr)
                }
              case elems: DBObject =>
                elems.toMap.valuesIterator.foreach { e =>
                  val d = field.dbObject(field.create)
                  d.putAll(e.asInstanceOf[DBObject])
                  ds = field.append(ds, d.repr)
                }
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
      case e: MongoException.DuplicateKey => throw new DuplicateKeyException
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
