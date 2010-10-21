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
  val jsonDocument: D
  def jsonMembers: Iterator[JsonSpec.Single[D, IO]]
  def jsonMembers(doc: D#DocRepr): Iterator[JsonSpec.Single[D, IO]] =
    jsonMembers
}

object JsonSpec {
  sealed trait Verdict
  object Ignore extends Verdict
  object Raise extends Verdict

  sealed trait Path {
    def toPrefixString: String
    final def :+(name: String) = FieldPath(this, name)
  }
  object EmptyPath extends Path {
    def toPrefixString = ""
    override def toString = "JsonSpec.EmptyPath"
  }
  sealed trait NonEmptyPath extends Path {
    def prefix: Path
    def toPrefixString = toString + '.'
  }
  final case class FieldPath(prefix: Path, name: String) extends NonEmptyPath {
    override def toString = prefix.toPrefixString + name
    def :+(index: Int) = ElementPath(this, index)
  }
  final case class ElementPath(prefix: FieldPath, index: Int)
                   extends NonEmptyPath {
    override def toString = prefix.toString + '[' + index + ']'
  }

  sealed class ReadingException(message: String, cause: Throwable)
               extends RuntimeException(message, cause) {
    def this(message: String) = this(message, null)
  }
  final case class UnexpectedFieldException(path: NonEmptyPath)
                   extends ReadingException("Unexpected field '" + path + "'")
  final case class MissingFieldException(path: NonEmptyPath)
                   extends ReadingException(
                             "Mandatory field '" + path + "' is missing")
  final case class IllegalFieldValue(path: NonEmptyPath,
                                     value: JsonValue, error: String) {
    def getMessage =
      "Field '" + path + "' has illegal value " + value + ": " + error
  }
  final case class IllegalFieldsValuesException(errors: Seq[IllegalFieldValue])
                   extends ReadingException(
                             errors.map(_.getMessage).mkString("\n"))

  final class Reader[D <: Document](val spec: JsonSpec[D, In])
              extends (JsonObject => D#DocRepr) {
    private def processRepr[DD <: Document](
                  path: Path, jo: JsonObject, spec: JsonSpec[DD, In],
                  repr: DD#DocRepr): (DD#DocRepr, Seq[IllegalFieldValue]) = {
      val d = spec.jsonDocument
      var doc = repr.asInstanceOf[d.DocRepr]

      def basicField[F <: DDD#BasicFieldBase forSome { type DDD <: Document }](
            field: F, name: String, value: JsonValue) = {
        val f = field.asInstanceOf[d.BasicFieldBase]
        val fullPath = path :+ name
        try {
          val r = f.fromJson(value)
          doc = f.set(doc, r)
          f.validator(r).errors.map(IllegalFieldValue(fullPath, value, _))
        } catch {
          case e: Exception =>
            Vector(IllegalFieldValue(fullPath, value, e.getMessage))
        }
      }
      def embeddingFieldWith[F <: DD#EmbeddingFieldBase](
            field: F, name: String, obj: JsonObject, spec: JsonSpec[F, In]) = {
        val f = field.asInstanceOf[d.EmbeddingFieldBase]
        val (r, errors) = process[f.type](
                            path :+ name, obj,
                            spec.asInstanceOf[JsonSpec[f.type, In]])
        doc = f.set(doc, r)
        errors
      }
      def embeddingField[F <: DDD#EmbeddingFieldBase
                                forSome { type DDD <: Document }](
            field: F, name: String, obj: JsonObject) = {
        val f = field.asInstanceOf[d.EmbeddingFieldBase]
        val (r, errors) = process[f.type](path :+ name, obj, f.jsonSpec)
        doc = f.set(doc, r)
        errors
      }
      def optEmbeddingField[F <: DDD#OptEmbeddingFieldBase
                                   forSome { type DDD <: Document }](
            field: F, name: String) {
        val f = field.asInstanceOf[d.OptEmbeddingFieldBase]
        doc = f.set(doc, f.nullDocRepr)
      }
      def elementsField[F <: DDD#ElementsArrayFieldBase
                               forSome { type DDD <: Document }](
            field: F, name: String,
            array: JsonArray): Seq[IllegalFieldValue] = {
        val f = field.asInstanceOf[d.ElementsArrayFieldBase]
        val fullPath = path :+ name
        var elems = f.newArrayRepr
        val errors = array.iterator.zipWithIndex.
                       foldLeft(Vector[IllegalFieldValue]()) {
          case (errors, (value, i)) =>
            try {
              val elem = f.fromJson(value)
              elems = f.append(elems, elem)
              errors ++ f.validator(elem).errors.map(
                          IllegalFieldValue(fullPath :+ i, value, _))
            } catch {
              case e: Exception =>
                errors :+ IllegalFieldValue(fullPath :+ i, value, e.getMessage)
            }
        }
        doc = f.set(doc, elems)
        errors
      }
      def documentsFieldWith[F <: DDD#DocumentsArrayFieldBase
                                    forSome { type DDD <: Document }](
            field: F, name: String, array: JsonArray, spec: JsonSpec[F, In]) = {
        val f = field.asInstanceOf[d.DocumentsArrayFieldBase]
        val fullPath = path :+ name
        val (elems, errors) = array.iterator.zipWithIndex.
                                foldLeft((f.newArrayRepr,
                                          Vector[IllegalFieldValue]())) {
          case ((elems, errors), (obj: JsonObject, i)) =>
            val (elem, elemErrors) =
              process[f.type](fullPath :+ i, obj,
                              spec.asInstanceOf[JsonSpec[f.type, In]])
            (f.append(elems, elem), errors ++ elemErrors)
          case ((elems, errors), (value, i)) =>
            (elems, errors :+ IllegalFieldValue(fullPath :+ i, value,
                                                "Not a JSON document"))
        }
        doc = f.set(doc, elems)
        errors
      }
      def documentsField[F <: DDD#DocumentsArrayFieldBase
                                forSome { type DDD <: Document }](
            field: F, name: String, array: JsonArray) = {
        val f = field.asInstanceOf[d.DocumentsArrayFieldBase]
        documentsFieldWith[f.type](f, name, array, f.jsonSpec)
      }
      def customEmbedding(
            name: String, obj: JsonObject, spec: NotEmpty[DD, In]) = {
        val (updatedDoc, errors) = processRepr[d.type](
                                     path :+ name, obj,
                                     spec.asInstanceOf[NotEmpty[d.type, In]],
                                     doc)
        doc = updatedDoc
        errors
      }
      def liftedFromField[F <: DD#EmbeddingFieldBase](
            field: F, name: String, value: JsonValue, spec: Single[F, In]) = {
        val f = field.asInstanceOf[d.EmbeddingFieldBase]
        val (r, errors) = processRepr[f.type](
                            path, JsonObject(name -> value),
                            spec.asInstanceOf[Single[f.type, In]], f.get(doc))
        doc = f.set(doc, r)
        errors
      }
      def convField[F <: DDD#FieldBase forSome { type DDD <: Document }](
            field: F, name: String, conv: PartialFunction[JsonValue, F#Repr],
            value: JsonValue) = {
        val f = field.asInstanceOf[d.FieldBase]
        val fullPath = path :+ name
        try {
          val r = conv(value).asInstanceOf[f.Repr]
          doc = f.set(doc, r)
          Vector.empty
        } catch {
          case e: Exception =>
            Vector(IllegalFieldValue(fullPath, value, e.getMessage))
        }
      }

      val hl = spec.asInstanceOf[HasLookup[DD, In]]
      var seen = Set[String]()
      var errors = Vector[IllegalFieldValue]()
      jo.iterator.foreach { case (name, value) =>
        hl.jsonMemberSpec(name) match {
          case Right(s) =>
            seen += name
            (s match {
              case OptMember(spec, _) => spec
              case _ => s
            }) match {
              case _: IgnoreInput[_, _] =>
                throw new UnexpectedFieldException(path :+ name)
              case ConvFrom(name, field, conv) =>
                errors ++= convField(field, name, conv, value)
              case DocumentField(_, BasicField(field)) =>
                errors ++= basicField(field, name, value)
              case DocumentField(_, EmbeddingField(field)) =>
                (value, field) match {
                  case (obj: JsonObject, field) =>
                    errors ++= embeddingField(field, name, obj)
                  case (JsonNull, OptEmbeddingField(field)) =>
                    optEmbeddingField(field, name)
                  case _ =>
                    errors :+= IllegalFieldValue(path :+ name, value,
                                                 "Not a JSON document")
                }
              case DocumentField(_, ElementsArrayField(field)) =>
                value match {
                  case array: JsonArray =>
                    errors ++= elementsField(field, name, array)
                  case _ =>
                    errors :+= IllegalFieldValue(path :+ name, value,
                                                 "Not a JSON array")
                }
              case DocumentField(_, DocumentsArrayField(field)) =>
                value match {
                  case array: JsonArray =>
                    errors ++= documentsField(field, name, array)
                  case _ =>
                    errors :+= IllegalFieldValue(path :+ name, value,
                                                 "Not a JSON array")
                }
              case InEmbedding(_, field, spec) =>
                (value, field) match {
                  case (obj: JsonObject, field) =>
                    errors ++= embeddingFieldWith(field, name, obj, spec)
                  case (JsonNull, OptEmbeddingField(field)) =>
                    optEmbeddingField(field, name)
                  case _ =>
                    errors :+= IllegalFieldValue(path :+ name, value,
                                                 "Not a JSON document")
                }
              case InDocuments(_, field, spec, _) =>
                value match {
                  case array: JsonArray =>
                    errors ++= documentsFieldWith(field, name, array, spec)
                  case _ =>
                    errors :+= IllegalFieldValue(path :+ name, value,
                                                 "Not a JSON array")
                }
              case InCustomEmbedding(_, spec) =>
                value match {
                  case obj: JsonObject =>
                    errors ++= customEmbedding(name, obj, spec)
                  case _ =>
                    errors :+= IllegalFieldValue(path :+ name, value,
                                                 "Not a JSON document")
                }
              case Lifted(field, spec) =>
                errors ++= liftedFromField(field, name, value, spec)
            }
          case Left(Raise) =>
            throw new UnexpectedFieldException(path :+ name)
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
              throw new MissingFieldException(path :+ single.jsonMember)
          }
        }
      }
      (doc, errors)
    }

    private def process[DD <: Document](
                  path: Path, jo: JsonObject,
                  spec: JsonSpec[DD, In]): (DD#DocRepr,
                                            Seq[IllegalFieldValue]) = {
      var doc = spec.jsonDocument.create
      processRepr(path, jo, spec, doc)
    }

    def apply(jo: JsonObject): D#DocRepr = {
      val (result, errors) = process(EmptyPath, jo, spec)
      if (!errors.isEmpty)
        throw new IllegalFieldsValuesException(errors)
      result
    }
  }

  final class Writer[D <: Document](spec: JsonSpec[D, Out])
              extends (D#DocRepr => JsonObject) {
    private def process[DD <: Document](
                 spec: JsonSpec[DD, Out], repr: DD#DocRepr): JsonObject = {
      val d = spec.jsonDocument
      val dr = repr.asInstanceOf[d.DocRepr]
      JsonObject(spec.jsonMembers(dr).map { s =>
        (s match {
          case CondOut(spec, test) => if (test(dr)) Some(spec) else None
          case spec => Some(spec)
        }) .flatMap {
          case ConvTo(_, field, conv) =>
            val f = field.asInstanceOf[d.FieldBase]
            conv(f.get(dr))
          case CustomFieldTo(_, _, conv) =>
            conv(dr)
          case DocumentField(_, field) => field match {
            case BasicField(field) =>
              val f = field.asInstanceOf[d.BasicFieldBase]
              Some(f.toJson(f.get(dr)))
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
              Some(JsonArray(f.iterator(f.get(dr)).map(e => f.toJson(e))))
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
              process[d.type](spec.asInstanceOf[NotEmpty[d.type, Out]], dr))
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
    val jsonDocument = spec.jsonDocument 
    def jsonMemberSpec(name: String) = spec.jsonMemberSpec(name) match {
      case Left(Raise) => Left(Ignore)
      case v => v
    }
    def jsonMembers = spec.jsonMembers
  }
  final case class RestIn[D <: Document](
                     notFields: Set[D#FieldName],
                     ignoreUnknown: Boolean,
                     spec: NoDefault[D, In]) extends HasLookup[D, In] {
    val jsonDocument = spec.jsonDocument 
    def jsonMemberSpec(name: String) = spec.jsonMemberSpec(name) match {
      case Left(Raise) =>
        (for {
          name <- jsonDocument.stringToName(name)
          fieldSpec <- jsonDocument.fieldSpec(name)
        } yield {
          if (notFields.contains(name) ||
              spec.jsonFieldsNames.contains(name))
            Left(Raise)
          else
            Right(fieldSpec.asInstanceOf[Single[D, In]])
        }) .getOrElse {
          if (ignoreUnknown)
            Left(Ignore)
          else
            Left(Raise)
        }
      case v => v
    }
    def jsonMembers = spec.jsonMembers ++ {
      jsonDocument.staticFields.iterator.filterNot { field =>
        notFields.contains(field.fieldName) ||
        spec.jsonFieldsNames.contains(field.fieldName)
      } .asInstanceOf[Iterator[Single[D, In]]]
    }
  }
  final class RestOut[D <: Document](
                notFields: Set[D#FieldName],
                spec: NoDefault[D, Out]) extends JsonSpec[D, Out] {
    val jsonDocument = spec.jsonDocument 
    def jsonMembers = spec.jsonMembers ++ {
      jsonDocument.staticFields.iterator.filterNot { field =>
        notFields.contains(field.fieldName) ||
        spec.jsonFieldsNames.contains(field.fieldName)
      } .asInstanceOf[Iterator[Single[D, Out]]]
    }
    override def jsonMembers(doc: D#DocRepr) = {
      val d = doc.asInstanceOf[jsonDocument.DocRepr]
      spec.jsonMembers ++
      jsonDocument.fieldsNames(d).filterNot { name =>
        notFields.contains(name) ||
        spec.jsonFieldsNames.contains(name)
      } .map { name =>
        jsonDocument.fieldSpec(name)
      } .filter(_.isDefined).map(_.get).asInstanceOf[Iterator[Single[D, Out]]]
    }
  }

  sealed trait NoDefault[D <: Document, +IO <: Direction]
               extends HasLookup[D, IO] {
    val jsonFieldsNames: Set[D#FieldName]
    def jsonMembersMap: Map[String, Single[D, IO]]

    def jsonMemberSpec(name: String) = jsonMembersMap.get(name) match {
      case Some(spec) => Right(spec)
      case None => Left(Raise)
    }
    def jsonMembers = jsonMembersMap.valuesIterator

    def >>!(notFields: D#FieldName*)(
            implicit docWitness: D <:< DynamicDocument,
                     dirWitness: IO <:< Out) =
      new RestOut[D](notFields.toSet, this.asInstanceOf[NoDefault[D, Out]])
    def >>!(notField1: D#FieldBase, notFields: D#FieldBase*)(
            implicit witness: IO <:< Out) =
      new RestOut[D](notFields.map(_.fieldName).toSet + notField1.fieldName,
                     this.asInstanceOf[NoDefault[D, Out]])
    def >>*()(implicit witness: IO <:< Out) =
      new RestOut[D](Set[D#FieldName](), this.asInstanceOf[NoDefault[D, Out]])
    def <<!(notFields: D#FieldName*)(
            implicit docWitness: D <:< DynamicDocument,
                     dirWitness: IO <:< In) =
      new RestIn(notFields.toSet, false, this.asInstanceOf[NoDefault[D, In]])
    def <<!#(notFields: D#FieldName*)(
             implicit docWitness: D <:< DynamicDocument,
                      dirWitness: IO <:< In) =
      new RestIn(notFields.toSet, true, this.asInstanceOf[NoDefault[D, In]])
    def <<!(notField1: D#FieldBase, notFields: D#FieldBase*)(
            implicit witness: IO <:< In) =
      new RestIn(notFields.map(_.fieldName).toSet + notField1.fieldName, false,
                 this.asInstanceOf[NoDefault[D, In]])
    def <<!#(notField1: D#FieldBase, notFields: D#FieldBase*)(
             implicit witness: IO <:< In) =
      new RestIn(notFields.map(_.fieldName).toSet + notField1.fieldName, true,
                 this.asInstanceOf[NoDefault[D, In]])
    def <<*()(implicit witness: IO <:< In) =
      new RestIn(Set[D#FieldName](), false, this.asInstanceOf[NoDefault[D, In]])
    def <<*#()(implicit witness: IO <:< In) =
      new RestIn(Set[D#FieldName](), true, this.asInstanceOf[NoDefault[D, In]])
    def <<#()(implicit witness: IO <:< In) =
      new IgnoreRestIn[D](this.asInstanceOf[NoDefault[D, In]])
  }
  object NoDefault {
    def unapply[D <: Document, IO <: Direction](
          spec: NoDefault[D, IO]): Option[NoDefault[D, IO]] = Some(spec)
  }
  sealed trait NotEmpty[D <: Document, +IO <: Direction]
               extends NoDefault[D, IO] {
    def >>(spec: NotEmpty[D, Out])(
           implicit witness: IO <:< Out): NotEmpty[D, Out]
    def <<(spec: NotEmpty[D, In])(
           implicit witness: IO <:< In): NotEmpty[D, In]
  }
  final class Empty[D <: Document](val jsonDocument: D)
              extends NoDefault[D, InOut] {
    val jsonFieldsNames = Set[D#FieldName]()
    val jsonMembersMap = Map[String, Single[D, InOut]]()
  }
  sealed trait Single[D <: Document, +IO <: Direction]
               extends NotEmpty[D, IO] {
    val jsonMember: String
    lazy val jsonMembersMap = Map(jsonMember -> this)

    final def >>(spec: NotEmpty[D, Out])(
                 implicit witness: IO <:< Out): NotEmpty[D, Out] = spec match {
      case Many(specs) =>
        Many[D, Out](this.asInstanceOf[Single[D, Out]] +: specs)
      case spec =>
        Many(Seq(this.asInstanceOf[Single[D, Out]],
                 spec.asInstanceOf[Single[D, Out]]))
    }
    final def <<(spec: NotEmpty[D, In])(
                 implicit witness: IO <:< In): NotEmpty[D, In] = spec match {
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
                   extends NotEmpty[D, IO] {
    val jsonDocument = specs(0).jsonDocument
    lazy val jsonFieldsNames = specs.map(_.jsonFieldsNames).reduceLeft(_ ++ _)
    lazy val jsonMembersMap = specs.map(_.jsonMembersMap).reduceLeft(_ ++ _)

    final def >>(spec: NotEmpty[D, Out])(
                 implicit witness: IO <:< Out) = spec match {
      case Many(ss) =>
        Many[D, Out](specs.asInstanceOf[Seq[Single[D, Out]]] ++ ss)
      case spec =>
        Many(specs.asInstanceOf[Seq[Single[D, Out]]] :+
             spec.asInstanceOf[Single[D, Out]])
    }
    final def <<(spec: NotEmpty[D, In])(
                 implicit witness: IO <:< In) = spec match {
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
    val jsonDocument = spec.jsonDocument
    val jsonMember = spec.jsonMember
    val jsonFieldsNames = spec.jsonFieldsNames
  }
  final case class OptMember[D <: Document](
                     spec: Single[D, In], alt: D#DocRepr => D#DocRepr)
                   extends Single[D, In] {
    val jsonDocument = spec.jsonDocument
    val jsonMember = spec.jsonMember
    val jsonFieldsNames = spec.jsonFieldsNames
  }
  sealed trait Member[D <: Document, +IO <: Direction] extends Single[D, IO] {
    def orElse(alt: D#DocRepr => D#DocRepr)(
               implicit witness: IO <:< In): OptMember[D] =
      new OptMember[D](this.asInstanceOf[Single[D, In]], alt)
    def orDo(alt: => Unit)(implicit witness: IO <:< In): OptMember[D] =
      this orElse (d => { alt; d })
    def ?()(implicit witness: IO <:< In): OptMember[D] = this orElse (d => d)
  }
  sealed trait Field[D <: Document, +IO <: Direction] extends Member[D, IO] {
    val jsonField: D#FieldBase
    final lazy val jsonDocument = jsonField.fieldDocument
    final lazy val jsonFieldsNames = Set(jsonField.fieldName)
    lazy val jsonMember = jsonField.fieldNameString
  }
  final class DynamicField[D <: DynamicDocument](
                val jsonDocument: D, name: D#FieldName)
              extends Member[D, InOut] {
    val jsonFieldsNames = Set[D#FieldName]()
    val jsonMember =
      jsonDocument.nameToString(name.asInstanceOf[jsonDocument.FieldName])
  }

  trait DocumentField[D <: Document]
        extends Field[D, InOut] { self: D#FieldBase with DocumentField[D] => 
    final val jsonField = self
    final override lazy val jsonMember = fieldNameString
  }
  object DocumentField {
    def unapply[D <: Document, IO <: Direction](
          spec: Single[D, IO]): Option[(String, D#FieldBase)] = {
      def cast[DD <: Document](field: DD#FieldBase): D#FieldBase =
        field.asInstanceOf[D#FieldBase]
      spec match {
        case Field(field) => Some((field.fieldNameString, cast(field)))
        case Renamed(name, field) => Some((name, cast(field)))
        case _ => None
      }
    }
  }
  sealed trait IgnoreInput[D <: Document, F <: D#FieldBase]
               extends Single[D, In] {
    val jsonField: F
    def value: F#Repr

    val jsonDocument = jsonField.jsonDocument
    lazy val jsonMember = jsonField.fieldNameString
    lazy val jsonFieldsNames = Set(jsonField.fieldName)
  }
  object IgnoreInput {
    def unapply[D <: Document, F <: D#FieldBase](
          spec: IgnoreInput[D, F]): Option[IgnoreInput[D, F]] = Some(spec)
  }
  final class Const[D <: Document, F <: D#FieldBase](
                val jsonField: F, val value: F#Repr)
              extends IgnoreInput[D, F]
  final class Eval[D <: Document, F <: D#FieldBase](
                val jsonField: F, expr: => F#Repr)
              extends IgnoreInput[D, F] {
    def value = expr
  }
  final case class Renamed[D <: Document, F <: D#FieldBase](
                     name: String, field: F)
                   extends Field[D, InOut] {
    val jsonField: F = field
    override lazy val jsonMember = name

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
    val jsonField: F = field
    override lazy val jsonMember = name
  }
  final case class ConvFrom[D <: Document, F <: D#FieldBase](
                     name: String, field: F,
                     conv: PartialFunction[JsonValue, F#Repr])
                   extends Field[D, In] {
    val jsonField: F = field
    override lazy val jsonMember = name
  }
  final case class InCustomEmbedding[D <: Document, IO <: Direction](
                     name: String, spec: NotEmpty[D, IO])
                   extends Member[D, IO] {
    val jsonDocument = spec.jsonDocument
    val jsonMember = name
    val jsonFieldsNames = spec.jsonFieldsNames
  }
  final case class CustomFieldFrom[D <: Document](
                     name: String, val jsonDocument: D,
                     conv: PartialFunction[(D#DocRepr, JsonValue), D#DocRepr])
                   extends Member[D, In] {
    val jsonMember = name
    val jsonFieldsNames = Set[D#FieldName]()
  }
  final case class CustomFieldTo[D <: Document](
                     name: String, val jsonDocument: D,
                     conv: D#DocRepr => Option[JsonValue])
                   extends Member[D, Out] {
    val jsonMember = name
    val jsonFieldsNames = Set[D#FieldName]()
  }
  final case class InEmbedding[D <: Document, F <: D#EmbeddingFieldBase,
                               IO <: Direction](
                     name: String, field: F, spec: JsonSpec[F, IO])
                   extends Field[D, IO] {
    val jsonField: F = field 
    override lazy val jsonMember = name
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
    val jsonField: F = field 
    override lazy val jsonMember = name
  }
  final case class Lifted[D <: Document, F <: D#EmbeddingFieldBase,
                          +IO <: Direction](
                     field: F, spec: Single[F, IO])
                   extends Single[D, IO] {
    val jsonDocument = field.jsonDocument
    val jsonMember = spec.jsonMember
    val jsonFieldsNames = Set[D#FieldName]()
  }
}

