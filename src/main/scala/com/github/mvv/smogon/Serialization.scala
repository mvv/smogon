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

import com.github.mvv.layson.json._

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
      def elementsField[F <: DDD#ElementsArrayFieldBase
                               forSome { type DDD <: Document }](
            field: F, name: String, array: JsonArray) {
        val f = field.asInstanceOf[d.ElementsArrayFieldBase]
        var elems = f.newArrayRepr
        array.iterator.zipWithIndex.foreach { case (value, i) =>
          val elem = try {
                       f.fromBson(Bson.fromJson(value, f.bsonClass))
                     } catch {
                       case e: Exception =>
                         throw new IllegalFieldValueException(
                           (path :+ name).mkString(".") + "[" + i + "]",
                           value, e)
                     }
          elems = f.append(elems, elem)
        }
        doc = f.set(doc, elems)
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
      def convField[F <: DDD#FieldBase forSome { type DDD <: Document }](
            field: F, name: String, conv: PartialFunction[JsonValue, F#Repr],
            value: JsonValue) {
        val f = field.asInstanceOf[d.FieldBase]
        val r = try {
                  conv(value).asInstanceOf[f.Repr]
                } catch {
                  case e: Exception =>
                    throw new IllegalFieldValueException(
                                (path :+ name).mkString("."), value, e)
                }
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
              case _: IgnoreInput[_, _] =>
                throw new UnexpectedFieldException((path :+ name).mkString("."))
              case ConvFrom(name, field, conv) =>
                convField(field, name, conv, value)
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
              case DocumentField(_, ElementsArrayField(field)) =>
                value match {
                  case array: JsonArray =>
                    elementsField(field, name, array)
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
            case IgnoreInput(i) =>
              val f = i.jsonField.asInstanceOf[d.FieldBase]
              doc = f.set(doc, i.value.asInstanceOf[f.Repr])
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
            case ElementsArrayField(field) =>
              val f = field.asInstanceOf[d.ElementsArrayFieldBase]
              Some(JsonArray(f.iterator(f.get(dr)).
                               map(e => Bson.toJson(f.toBson(e)))))
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
    def <<!(notFields: D#FieldBase*)(implicit witness: IO <:< In) =
      new RestIn(notFields.toSet, false, this.asInstanceOf[NoDefault[D, In]])
    def <<!#(notFields: D#FieldBase*)(implicit witness: IO <:< In) =
      new RestIn(notFields.toSet, true, this.asInstanceOf[NoDefault[D, In]])
    def <<*()(implicit witness: IO <:< In) =
      this <<! (Nil: _*)
    def <<*#()(implicit witness: IO <:< In) =
      this <<!# (Nil: _*)
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
               implicit witness: IO <:< Out): CondOut[D] =
      CondOut(this.asInstanceOf[Single[D, Out]], test)
    def onlyIf(test: Boolean)(
               implicit witness: IO <:< Out): CondOut[D] =
      onlyIf(_ => test)
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
    final def jsonDocument = jsonField.fieldDocument
    override def jsonFields = Set(jsonField)
    def jsonMember = jsonField.fieldName
  }

  trait DocumentField[D <: Document]
        extends Field[D, InOut] { self: D#FieldBase with DocumentField[D] => 
    final val jsonField = self
    final override val jsonFields: Set[D#FieldBase] = Set(self)
    final override def jsonMember = self.fieldName
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
  sealed trait IgnoreInput[D <: Document, F <: D#FieldBase]
               extends Single[D, In] {
    def jsonField: F
    def value: F#Repr

    def jsonDocument = jsonField.jsonDocument
    def jsonMember = jsonField.fieldName
    def jsonFields = Set(jsonField)
  }
  object IgnoreInput {
    def unapply[D <: Document, F <: D#FieldBase](
          spec: IgnoreInput[D, F]): Option[IgnoreInput[D, F]] = Some(spec)
  }
  final class Const[D <: Document, F <: D#FieldBase](
                field: F, val value: F#Repr)
              extends IgnoreInput[D, F] {
    def jsonField = field
  }
  final class Eval[D <: Document, F <: D#FieldBase](
                field: F, expr: => F#Repr)
              extends IgnoreInput[D, F] {
    def jsonField = field
    def value = expr
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

