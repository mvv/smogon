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

import scala.util.matching.Regex
import org.slf4j.LoggerFactory
import com.github.mvv.layson.bson._
import com.mongodb.{
         DBObject, BasicDBObject, BasicDBList, DBCollection, WriteResult}

sealed trait ValueFilter[V <: ReprBsonValue] {
  import ValueFilter._

  def unary_!(): ValueFilter[V]
  def &&(filter: ValueFilter[V]) = And(this, filter)
  def ||(filter: ValueFilter[V]) = Or(this, filter)

  def linearize: Iterator[ValueFilter[V]] = Iterator.single(this)
  def normalForm: ValueFilter[V]
}

object ValueFilter {
  sealed trait Simple[V <: ReprBsonValue] extends ValueFilter[V] {
    val rbv: V

    def unary_!(): ValueFilter[V] = Not(this)
    def operatorName: Option[String] = None
    def negationName: Option[String] = None

    final def normalForm = this

    def valueBson: BsonValue
    final def conditionBson = operatorName match {
      case Some(name) => BsonObject(name -> valueBson)
      case None => valueBson
    }
  }
  final case class Eq[V <: ReprBsonValue](rbv: V, value: V#ValueRepr)
                   extends Simple[V] {
    override def negationName = Some("$ne")
    def valueBson = rbv.toBson(value.asInstanceOf[rbv.ValueRepr])
  }
  final case class In[V <: ReprBsonValue](rbv: V, values: Set[V#ValueRepr])
                   extends Simple[V] {
    override def operatorName = Some("$in")
    override def negationName = Some("$nin")
    def valueBson =
      BsonArray(values.iterator.map(v =>
                  rbv.toBson(v.asInstanceOf[rbv.ValueRepr])).toSeq: _*)
  }
  final case class Less[V <: ReprBsonValue](
                     rbv: V, value: V#ValueRepr)(
                     implicit witness: V#Bson <:< OptSimpleBsonValue)
                   extends Simple[V] {
    override def operatorName = Some("$lt")
    override def negationName = Some("$gte")
    def valueBson = rbv.toBson(value.asInstanceOf[rbv.ValueRepr])
  }
  final case class LessOrEq[V <: ReprBsonValue](
                     rbv: V, value: V#ValueRepr)(
                     implicit witness: V#Bson <:< OptSimpleBsonValue)
                   extends Simple[V] {
    override def operatorName = Some("$lte")
    override def negationName = Some("$gt")
    def valueBson = rbv.toBson(value.asInstanceOf[rbv.ValueRepr])
  }
  final case class Mod[V <: ReprBsonValue](
                     rbv: V, divisor: V#ValueRepr, result: V#ValueRepr)(
                     implicit witness: V#Bson <:< OptIntegralBsonValue)
                   extends Simple[V] {
    override def operatorName = Some("$mod")
    def valueBson =
      BsonArray(rbv.toBson(divisor.asInstanceOf[rbv.ValueRepr]),
                rbv.toBson(result.asInstanceOf[rbv.ValueRepr]))
  }
  final case class Match[V <: ReprBsonValue](
                     rbv: V, regex: Regex)(
                     implicit witness: V#Bson <:< OptBsonStr)
                   extends Simple[V] {
    def valueBson = regex
  }
  final case class Not[V <: ReprBsonValue](filter: Simple[V])
                   extends Simple[V] {
    val rbv = filter.rbv
    override def unary_!() = filter
    override def operatorName = filter.negationName.orElse(Some("$not"))
    def valueBson = filter.negationName match {
      case Some(name) => filter.valueBson
      case None => filter.conditionBson
    }
  }
  final case class And[V <: ReprBsonValue](
                     first: ValueFilter[V], second: ValueFilter[V])
                   extends ValueFilter[V] {
    override def unary_!() = Or(!first, !second)
    override def linearize: Iterator[ValueFilter[V]] = (first, second) match {
      case (f @ And(_, _), s @ And(_, _)) => f.linearize ++ s.linearize
      case (f @ And(_, _), s) => f.linearize ++ Iterator.single(s)
      case (f, s @ And(_, _)) => Iterator.single(f) ++ s.linearize
      case _ => Iterator(first, second)
    }
    override def normalForm = (first.normalForm, second.normalForm) match {
      case (f @ Or(_, _), s @ Or(_, _)) =>
        (for { c1 <- f.linearize
               c2 <- s.linearize }
           yield And[V](c1, c2)).reduceLeft[ValueFilter[V]](Or(_, _))
      case (f @ Or(_, _), s) =>
        f.linearize.map(And(_, s)).reduceLeft[ValueFilter[V]](Or(_, _)) 
      case (f, s @ Or(_, _)) =>
        s.linearize.map(And(f, _)).reduceLeft[ValueFilter[V]](Or(_, _)) 
      case (f, s) => And(f, s)
    }
  }
  final case class Or[V <: ReprBsonValue](
                     first: ValueFilter[V], second: ValueFilter[V])
                   extends ValueFilter[V] {
    override def unary_!() = And(!first, !second)
    override def linearize: Iterator[ValueFilter[V]] = (first, second) match {
      case (f @ Or(_, _), s @ Or(_, _)) => f.linearize ++ s.linearize
      case (f @ Or(_, _), s) => f.linearize ++ Iterator.single(s)
      case (f, s @ Or(_, _)) => Iterator.single(f) ++ s.linearize
      case _ => Iterator(first, second)
    }
    override def normalForm = Or(first.normalForm, second.normalForm)
  }
}

final case class ValueFilterBuilder[V <: ReprBsonValue](rbv: V) {
  def ===(value: V#ValueRepr) = ValueFilter.Eq(rbv, value)
  def !==(value: V#ValueRepr) = !(this === value)
  def in(values: Set[V#ValueRepr]) = ValueFilter.In(rbv, values)
  def in(values: V#ValueRepr*) = ValueFilter.In(rbv, values.toSet)
  def notIn(values: Set[V#ValueRepr]) = !(this in values)
  def notIn(values: V#ValueRepr*) = !(this in values.toSet)
}

object ValueFilterBuilder {
  final class BasicFilterOps[V <: ReprBsonValue](
                builder: ValueFilterBuilder[V])(
                implicit witness: V#Bson <:< OptSimpleBsonValue) {
    def <(value: V#ValueRepr) = ValueFilter.Less[V](builder.rbv, value)
    def <=(value: V#ValueRepr) = ValueFilter.LessOrEq[V](builder.rbv, value)
    def >(value: V#ValueRepr) = !(this <= value)
    def >=(value: V#ValueRepr) = !(this < value)
  }

  implicit def basicFilterOps[V <: ReprBsonValue](
                 builder: ValueFilterBuilder[V])(
                 implicit witness: V#Bson <:< OptSimpleBsonValue) =
    new BasicFilterOps(builder)

  final class ModResultFilter[V <: ReprBsonValue](
                builder: ValueFilterBuilder[V], divisor: V#ValueRepr)(
                implicit witness: V#Bson <:< OptIntegralBsonValue) {
    def ===(result: V#ValueRepr) = ValueFilter.Mod[V](builder.rbv, divisor, result)
    def !==(result: V#ValueRepr) = !(this === result)
  }

  final class IntegralFilterOps[V <: ReprBsonValue](
                builder: ValueFilterBuilder[V])(
                implicit witness: V#Bson <:< OptIntegralBsonValue) {
    def %(divisor: V#ValueRepr) = new ModResultFilter[V](builder, divisor)
  }

  implicit def integralFilterOps[V <: ReprBsonValue](
                 builder: ValueFilterBuilder[V])(
                 implicit witness: V#Bson <:< OptIntegralBsonValue) =
    new IntegralFilterOps(builder)

  final class StringFilterOps[V <: ReprBsonValue](
                builder: ValueFilterBuilder[V])(
                implicit witness: V#Bson <:< OptBsonStr) {
    def =~(regex: Regex) = ValueFilter.Match[V](builder.rbv, regex)
    def !~(regex: Regex) = !(this =~ regex)
  }

  implicit def stringFilterOps[V <: ReprBsonValue](
                 builder: ValueFilterBuilder[V])(
                 implicit witness: V#Bson <:< OptBsonStr) =
    new StringFilterOps(builder)
}

sealed trait Filter[-R <: Documents] {
  import Filter._

  def unary_!(): Filter[R]
  def &&[R1 <: R](filter: Filter[R1]): Filter[R1]
  def ||[R1 <: R](filter: Filter[R1]): Filter[R1]

  def normalForm: Filter[R] = this

  def toBson: BsonObject
}

object Filter {
  private[smogon] final class FalseFilterException() extends RuntimeException

  object EmptyFilter extends Filter[Documents] {
    def unary_!() = EmptyFilter
    def &&[R <: Documents](filter: Filter[R]) = filter
    def ||[R <: Documents](filter: Filter[R]) = filter
    def toBson = BsonObject.Empty
  }
  sealed trait NonEmptyFilter[-R <: Documents] extends Filter[R] {
    def unary_!(): NonEmptyFilter[R]
    def &&[R1 <: R](filter: NonEmptyFilter[R1]): NonEmptyFilter[R1] =
      And(this, filter)
    def &&[R1 <: R](filter: Filter[R1]): Filter[R1] = filter match {
      case filter: NonEmptyFilter[_] =>
        And(this, filter.asInstanceOf[NonEmptyFilter[R1]])
      case _ => this
    }
    def ||[R1 <: R](filter: NonEmptyFilter[R1]): NonEmptyFilter[R1] =
      Or(this, filter)
    def ||[R1 <: R](filter: Filter[R1]): Filter[R1] = filter match {
      case filter: NonEmptyFilter[_] =>
        Or(this, filter.asInstanceOf[NonEmptyFilter[R1]])
      case _ => this
    }
 
    def linearize: Iterator[NonEmptyFilter[R]] = Iterator.single(this)
    override def normalForm: NonEmptyFilter[R] = this

    final def onlyIf(flag: Boolean): Filter[R] = if (flag) this else EmptyFilter
  }
  sealed trait Single[-R <: Documents] extends NonEmptyFilter[R]
  sealed trait Simple[D <: Document, +F <: D#FieldBase]
               extends Single[D#Root] {
    val field: F
    def unary_!(): NonEmptyFilter[D#Root] = Not(this)
    def operatorName: Option[String] = None
    def negationName: Option[String] = None
    def valueBson: BsonValue
    final def condition = operatorName match {
      case Some(name) => Right(name -> valueBson)
      case None => Left(valueBson)
    }
    final def conditionBson = operatorName match {
      case Some(name) => BsonObject(name -> valueBson)
      case None => valueBson
    }
    final def toBson = BsonObject(field.fieldRootName -> conditionBson)
  }
  final case class Not[D <: Document, F <: D#FieldBase](filter: Simple[D, F])
                   extends Simple[D, F] {
    val field = filter.field
    override def unary_!() = filter
    override def operatorName = filter.negationName.orElse(Some("$not"))
    def valueBson = filter.negationName match {
      case Some(name) => filter.valueBson
      case None => filter.conditionBson
    }
  }
  final case class And[R <: Documents](first: NonEmptyFilter[R],
                                       second: NonEmptyFilter[R])
                   extends NonEmptyFilter[R] {
    def unary_!() = Or(!first, !second)
    override def linearize: Iterator[NonEmptyFilter[R]] =
      (first, second) match {
        case (f @ And(_, _), s @ And(_, _)) => f.linearize ++ s.linearize
        case (f @ And(_, _), s) => f.linearize ++ Iterator.single(s)
        case (f, s @ And(_, _)) => Iterator.single(f) ++ s.linearize
        case _ => Iterator(first, second)
      }
    override def normalForm = (first.normalForm, second.normalForm) match {
      case (f @ Or(_, _), s @ Or(_, _)) =>
        (for { c1 <- f.linearize
               c2 <- s.linearize }
           yield And[R](c1, c2)).reduceLeft[NonEmptyFilter[R]](Or(_, _))
      case (f @ Or(_, _), s) =>
        f.linearize.map(And(_, s)).reduceLeft[NonEmptyFilter[R]](Or(_, _)) 
      case (f, s @ Or(_, _)) =>
        s.linearize.map(And(f, _)).reduceLeft[NonEmptyFilter[R]](Or(_, _)) 
      case (f, s) => And(f, s)
    }
    def toBson = {
      import And.Context
      val ctxs = linearize.foldLeft(Map[String, Context]()) { (m, elem) =>
        val c = elem.asInstanceOf[Single[R] forSome { type R <: Documents }]
        def combine(valueByOp: Map[String, BsonValue], op: String,
                    value: BsonValue): Map[String, BsonValue] = {
          valueByOp.get(op) match {
            case Some(old) =>
              // TODO: Actual combining
              valueByOp
            case None =>
              valueByOp + (op -> value)
          }
        }
        val (fieldName, v) = c.toBson.members.head
        val ctx = m.get(fieldName) match {
          case Some(ctx @ Context(eqTest, regexTest, notRegexTest,
                                  valueByOp, notValueByOp)) =>
            c match {
              case c @ Eq(_, _) =>
                val bson = c.valueBson
                eqTest match {
                  case Some(value) =>
                    if (bson != value)
                      throw new FalseFilterException
                    ctx
                  case None =>
                    regexTest.foreach { regex =>
                      bson.asInstanceOf[OptBsonStr] match {
                        case BsonNull =>
                          throw new FalseFilterException
                        case BsonStr(str) =>
                          val matcher = regex.pattern.matcher(str)
                          if (!matcher.matches)
                            throw new FalseFilterException
                      }
                    }
                    notRegexTest.foreach { regex =>
                      bson.asInstanceOf[OptBsonStr] match {
                        case BsonNull =>
                        case BsonStr(str) =>
                          val matcher = regex.pattern.matcher(str)
                          if (matcher.matches)
                            throw new FalseFilterException
                      }
                    }
                    ctx.copy(eqTest = Some(bson),
                             regexTest = None, notRegexTest = None)
                }
              case Match(_, r) =>
                eqTest match {
                  case Some(value) =>
                    value.asInstanceOf[OptBsonStr] match {
                      case BsonNull =>
                        throw new FalseFilterException
                      case BsonStr(str) =>
                        val matcher = r.pattern.matcher(str)
                        if (!matcher.matches)
                          throw new FalseFilterException
                    }
                    ctx
                  case None => regexTest match {
                    case Some(regex) =>
                      val combined = ("(" + regex + ")|(" + r + ")").r
                      ctx.copy(regexTest = Some(combined))
                    case None =>
                      ctx.copy(regexTest = Some(r))
                  }
                }
              case Not(Match(_, r)) =>
                eqTest match {
                  case Some(value) =>
                    value.asInstanceOf[OptBsonStr] match {
                      case BsonNull =>
                        throw new FalseFilterException
                      case BsonStr(str) =>
                        val matcher = r.pattern.matcher(str)
                        if (matcher.matches)
                          throw new FalseFilterException
                    }
                    ctx
                  case None => notRegexTest match {
                    case Some(regex) =>
                      val combined = ("(" + regex + ")|(" + r + ")").r
                      ctx.copy(notRegexTest = Some(combined))
                    case None =>
                      ctx.copy(notRegexTest = Some(r))
                  }
                }
              case c @ Not(filter) if c.operatorName == Some("$not") =>
                val (op, value) = filter.condition.right.get
                ctx.copy(notValueByOp = combine(valueByOp, op, value))
              case c =>
                val (op, value) = v.asInstanceOf[BsonObject].members.head
                ctx.copy(valueByOp = combine(valueByOp, op, value))
            }
          case None =>
            c match {
               case Eq(_, _) | ContainsElem(_, ValueFilter.Eq(_, _)) =>
                 Context(Some(v), None, None, Map(), Map())
               case Match(_, r) =>
                 Context(None, Some(r), None, Map(), Map())
               case ContainsElem(_, ValueFilter.Match(_, r)) =>
                 Context(None, Some(r), None, Map(), Map())
               case Not(Match(_, r)) =>
                 Context(None, None, Some(r), Map(), Map())
               case Not(ContainsElem(_, ValueFilter.Match(_, r))) =>
                 Context(None, Some(r), None, Map(), Map())
               case c @ Not(filter) if c.operatorName == Some("$not") =>
                 val (op, value) = filter.condition.right.get
                 Context(None, None, None, Map(), Map(op -> value))
               case c =>
                 val (op, value) = v.asInstanceOf[BsonObject].members.head
                 Context(None, None, None, Map(op -> value), Map())
            }
        }
        m + (fieldName -> ctx)
      }
      BsonObject(ctxs.mapValues(_.toBson(this)))
    }
  }
  object And {
    private object EmptyMap {
      def unapply[A, B](m: Map[A, B]) = if (m.isEmpty) Some(m) else None
    }
    final case class Context(eqTest: Option[BsonValue],
                             regexTest: Option[Regex],
                             notRegexTest: Option[Regex],
                             valueByOp: Map[String, BsonValue],
                             notValueByOp: Map[String, BsonValue]) {
      def toBson[R <: Documents](filter: Filter[R]): BsonValue = this match {
        case Context(Some(value), None, None, EmptyMap(_), EmptyMap(_)) =>
          value
        case Context(Some(value), _, _, _, _) =>
          copy(eqTest = None,
               notValueByOp = (notValueByOp + ("$ne" -> value))).toBson(filter)
        case Context(None, Some(regex), None, EmptyMap(_), EmptyMap(_)) =>
          regex
        case Context(None, None, Some(regex), _, EmptyMap(_)) =>
          BsonObject((valueByOp.iterator ++
                      Iterator.single("$not" -> BsonRegex(regex))).toMap)
        case Context(None, None, None, _, _) =>
          BsonObject((valueByOp.iterator ++
                      (if (notValueByOp.isEmpty)
                         Iterator.empty
                       else
                         Iterator.single(
                           "$not" -> BsonObject(notValueByOp.iterator)))).toMap)
        case _ =>
          throw UntranslatableQueryException(filter)
      }
    }
  }
  final case class Or[R <: Documents](first: NonEmptyFilter[R],
                                      second: NonEmptyFilter[R])
                   extends NonEmptyFilter[R] {
    def unary_!() = And(!first, !second)
    override def linearize: Iterator[NonEmptyFilter[R]] =
      (first, second) match {
        case (f @ Or(_, _), s @ Or(_, _)) => f.linearize ++ s.linearize
        case (f @ Or(_, _), s) => f.linearize ++ Iterator.single(s)
        case (f, s @ Or(_, _)) => Iterator.single(f) ++ s.linearize
        case _ => Iterator(first, second)
      }
    override def normalForm = Or(first.normalForm, second.normalForm)
    def toBson =
      BsonObject("$or" -> BsonArray(linearize.map(_.toBson).toSeq: _*))
  }
  final case class Eq[D <: Document, F <: D#FieldBase](
                     field: F, value: F#Repr) extends Simple[D, F] {
    override def negationName = Some("$ne")
    def valueBson = field.fieldBson(value.asInstanceOf[field.Repr])
  }
  final case class In[D <: Document, F <: D#FieldBase](
                     field: F, values: Set[F#Repr]) extends Simple[D, F] {
    override def operatorName = Some("$in")
    override def negationName = Some("$nin")
    def valueBson =
      BsonArray(values.iterator.map(v =>
                  field.fieldBson(v.asInstanceOf[field.Repr])).toSeq: _*)
  }
  final case class Less[D <: Document, F <: D#BasicFieldBase](
                     field: F, value: F#Repr)(
                     implicit witness: F#Bson <:< OptSimpleBsonValue)
                   extends Simple[D, F] {
    override def operatorName = Some("$lt")
    override def negationName = Some("$gte")
    def valueBson = field.toBson(value.asInstanceOf[field.Repr])
  }
  final case class LessOrEq[D <: Document, F <: D#BasicFieldBase](
                     field: F, value: F#Repr)(
                     implicit witness: F#Bson <:< OptSimpleBsonValue)
                   extends Simple[D, F] {
    override def operatorName = Some("$lte")
    override def negationName = Some("$gt")
    def valueBson = field.toBson(value.asInstanceOf[field.Repr])
  }
  final case class Mod[D <: Document, F <: D#BasicFieldBase](
                     field: F, divisor: F#Repr, result: F#Repr)(
                     implicit witness: F#Bson <:< OptIntegralBsonValue)
                   extends Simple[D, F] {
    override def operatorName = Some("$mod")
    def valueBson =
      BsonArray(field.toBson(divisor.asInstanceOf[field.Repr]),
                field.toBson(result.asInstanceOf[field.Repr]))
  }
  final case class Match[D <: Document, F <: D#BasicFieldBase](
                     field: F, regex: Regex)(
                     implicit witness: F#Bson <:< OptBsonStr)
                   extends Simple[D, F] {
    def valueBson = regex
  }
  final case class Size[D <: Document, F <: D#ArrayFieldBase](
                     field: F, size: Long) extends Simple[D, F] {
    override def operatorName = Some("$size")
    def valueBson = size
  }
  final case class ContainsAll[D <: Document, F <: D#ArrayFieldBase](
                     field: F, elems: Set[F#ElemRepr])
                   extends Simple[D, F] {
    override def operatorName = Some("$all")
    def valueBson =
      BsonArray(elems.asInstanceOf[Set[field.ElemRepr]].
                  iterator.map(field.elementBson(_)))
  }
  final case class ContainsElem[D <: Document, F <: D#ElementsArrayFieldBase](
                     field: F, filter: ValueFilter[F])
                   extends Simple[D, F] {
    override def operatorName = filter match {
      case s: ValueFilter.Simple[_] => s.operatorName
      case _ => throw UntranslatableQueryException(this)
    }
    override def negationName = filter match {
      case s: ValueFilter.Simple[_] => s.negationName
      case _ => throw UntranslatableQueryException(this)
    }
    def valueBson = filter match {
      case s: ValueFilter.Simple[_] => s.valueBson
      case _ => throw UntranslatableQueryException(this)
    }
  }
  final case class Contains[D <: Document, F <: D#DocumentsArrayFieldBase](
                     field: F, filter: Filter[F])
                   extends Simple[D, F] {
    override def operatorName = Some("$elemMatch")
    def valueBson = filter.normalForm.toBson
  }
  final case class Exists[D <: Document, F <: D#DynamicFieldBase](
                     field: F, name: F#FieldName, positive: Boolean)
                   extends Single[D#Root] {
    def unary_!() = Exists[D, F](field, name, !positive)
    def toBson =
      BsonObject((field.fieldRootName + '.' +
                  field.nameToString(name.asInstanceOf[field.FieldName])) ->
                   BsonObject("$exists" -> positive))
  }
}

sealed trait Update[-R <: Documents] {
  def &&[R1 <: R](update: Update[R1]): Update[R1]
  def toBson: BsonObject
  final def onlyIf(flag: Boolean): Update[R] = if (flag) this else EmptyUpdate
}

object EmptyUpdate extends Update[Documents] {
  def &&[R <: Documents](update: Update[R]) = update
  def toBson = BsonObject.Empty
}

object Update {
  sealed trait Single[-R <: Documents] extends Update[R] {
    final def &&[R1 <: R](update: Single[R1]): Update[R1] = 
      Many(Vector(this, update))
    final def &&[R1 <: R](update: Many[R1]): Update[R1] = 
      Many(this +: update.updates)
    final def &&[R1 <: R](update: Update[R1]): Update[R1] = update match {
      case update: Single[_] =>
        Many(Vector(this, update.asInstanceOf[Single[R1]]))
      case Many(updates) => Many(this +: updates)
      case EmptyUpdate => this
    }
  }
  final case class Many[-R <: Documents](
                     updates: Seq[Single[R]]) extends Update[R] {
    def &&[R1 <: R](update: Single[R1]): Update[R1] = 
      Many(this.updates :+ update)
    def &&[R1 <: R](update: Many[R1]): Update[R1] = 
      Many(this.updates ++ update.updates)
    def &&[R1 <: R](update: Update[R1]): Update[R1] = update match {
      case update: Single[_] => Many(this.updates :+ update)
      case Many(updates) => Many(this.updates ++ updates)
      case EmptyUpdate => this
    }
    def toBson = updates.foldLeft(new MapBsonObject()) { case (obj, update) =>
      update.toBson.iterator.foldLeft(obj) { case (obj, (code, op)) =>
        obj.get(code) match {
          case Some(ops: BsonObject) =>
            BsonObject(
              obj.membersMap +
              (code -> new MapBsonObject(
                             op.asInstanceOf[BsonObject].membersMap ++
                             ops.membersMap)))
          case _ => BsonObject(obj.membersMap + (code -> op))
        }
      }
    }
  }
  final case class SetTo[D <: Document, F <: D#FieldBase](
                     field: F, value: F#Repr) extends Single[D#Root] {
    def toBson =
      BsonObject("$set" ->
        BsonObject(field.fieldRootName ->
          field.fieldBson(value.asInstanceOf[field.Repr])))
  }
  final case class Increment[D <: Document, F <: D#BasicFieldBase](
                     field: F, value: F#Repr)(
                     implicit witness: F#Bson <:< OptNumericBsonValue)
                   extends Single[D#Root] {
    def toBson =
      BsonObject("$inc" ->
        BsonObject((field.fieldRootName ->
          field.fieldBson(value.asInstanceOf[field.Repr])) :: Nil))
  }
  final case class Decrement[D <: Document, F <: D#BasicFieldBase](
                     field: F, value: F#Repr)(
                     implicit witness: F#Bson <:< OptNumericBsonValue)
                   extends Single[D#Root] {
    def toBson =
      BsonObject("$inc" ->
        BsonObject((field.fieldRootName ->
          -field.fieldBson(value.asInstanceOf[field.Repr])) :: Nil))
  }
  final case class Push[D <: Document, F <: D#ArrayFieldBase](
                     field: F, values: Seq[F#ElemRepr]) extends Single[D#Root] {
    def toBson =
      if (values.size == 1)
        BsonObject("$push" ->
          BsonObject((field.fieldRootName ->
            field.elementBson(values(0).asInstanceOf[field.ElemRepr])) :: Nil))
      else
        BsonObject("$pushAll" ->
          BsonObject((field.fieldRootName ->
            BsonArray(values.asInstanceOf[Seq[field.ElemRepr]].
                        map(field.elementBson(_)): _*)) :: Nil))
  }
  final case class AddToSet[D <: Document, F <: D#ArrayFieldBase](
                     field: F, values: Set[F#ElemRepr]) extends Single[D#Root] {
    def toBson =
      BsonObject("$addToSet" ->
        BsonObject(field.fieldRootName ->
          (if (values.size == 1)
             field.elementBson(values.head.asInstanceOf[field.ElemRepr])
           else
             BsonObject("$each" ->
               BsonArray(values.toSeq.asInstanceOf[Seq[field.ElemRepr]].
                           map(field.elementBson(_)): _*)))))
  }
  final case class Pull[D <: Document, F <: D#ArrayFieldBase](
                     field: F, values: Set[F#ElemRepr]) extends Single[D#Root] {
    def toBson =
      if (values.size == 1)
        BsonObject("$pull" ->
          BsonObject((field.fieldRootName ->
            field.elementBson(values.head.asInstanceOf[field.ElemRepr])) :: Nil))
      else
        BsonObject("$pullAll" ->
          BsonObject((field.fieldRootName ->
            BsonArray(values.toSeq.asInstanceOf[Seq[field.ElemRepr]].
                        map(field.elementBson(_)): _*)) :: Nil))
  }
  final case class Pop[D <: Document, F <: D#ArrayFieldBase](
                     field: F, front: Boolean) extends Single[D#Root] {
    def toBson =
      BsonObject("$pop" ->
        BsonObject(field.fieldRootName -> (if (front) -1 else 1)))
  }
}

object Query {
  private val falseQueryBson = {
    val dbo = new BasicDBObject
    dbo.put("$nonexistent$", "exists")
    dbo
  }
}
final class Query[C <: Collection] private(
              coll: C, queryBson: DBObject, sortBson: DBObject,
              projectionBson: DBObject) {
  import Query._
  import Collection._

  private[smogon] def this(coll: C, filter: Filter[C]) =
    this(coll, try {
                 Bson.toDBObject(filter.normalForm.toBson)
               } catch {
                 case _: Filter.FalseFilterException =>
                   null
                 case _: UntranslatableQueryException[_] =>
                   throw new UntranslatableQueryException(filter)
               }, new BasicDBObject, new BasicDBObject)
  private[smogon] def this(coll: C) =
    this(coll, new BasicDBObject, new BasicDBObject, new BasicDBObject)

  import scala.collection.JavaConversions._

  private def reprFromBson(obj: DBObject): C#DocRepr = {
    val repr = coll.create
    coll.toDBObject(repr).putAll(obj)
    repr
  }

  def only[CC >: C <: Collection](
        proj: CC => Projection[c.type] forSome { val c: CC }) =
    new Query[C](coll, queryBson, sortBson,
                 new BsonDBObject(proj(coll).projectionBson(true)))
  def except[CC >: C <: Collection](
        proj: CC => Projection[c.type] forSome { val c: CC }) =
    new Query[C](coll, queryBson, sortBson,
                 new BsonDBObject(proj(coll).projectionBson(false)))
  def sort[CC >: C <: Collection](
        sort: CC => Sort[c.type] forSome { val c: CC }) =
    new Query[C](coll, queryBson, new BsonDBObject(sort(coll).sortBson),
                 projectionBson)

  def findIn(dbc: DBCollection,
             skip: Int = 0, limit: Int = -1): Iterator[C#DocRepr] = {
    if (logger.isTraceEnabled)
      logger.trace(dbc.getName + ".find(" + this + "), skip=" + skip +
                   ", limit=" + limit)
    if (limit == 0 || queryBson == null)
      Iterator.empty
    else
      asScalaIterator {
        dbc.find(queryBson, projectionBson, skip, if (limit > 0) -limit else 0).
          sort(sortBson)
      } .map(reprFromBson(_))
  }
  def find(skip: Int = 0, limit: Int = -1)(
           implicit witness: C <:< AssociatedCollection): Iterator[C#DocRepr] =
    findIn(witness(coll).getDbCollection, skip, limit)
  def findOneIn(dbc: DBCollection): Option[C#DocRepr] =
    findIn(dbc, 0, 1).toSeq.headOption
  def findOne()(implicit witness: C <:< AssociatedCollection): Option[C#DocRepr] =
    findOneIn(witness(coll).getDbCollection)

  def count(dbc: DBCollection): Long =
    if (queryBson == null) 0 else dbc.count(queryBson)
  def count()(implicit witness: C <:< AssociatedCollection): Long =
    count(witness(coll).getDbCollection)

  def updateIn[CC >: C <: Collection](
        dbc: DBCollection, up: CC => Update[c.type] forSome { val c: CC },
        safety: Safety = Safety.Default, timeout: Int = 0): Long = {
    if (queryBson == null) {
      if (logger.isTraceEnabled) {
        val cs = Collection.safetyOf(dbc, safety)
        val updateBson = Bson.toDBObject(up(coll).toBson)
        logger.trace(dbc.getName + ".update(" + this + ", " + updateBson +
                     "), safety=" + cs)
        logger.trace(dbc.getName + ".update result is 0")
      }
      0
    } else {
      val cs = Collection.safetyOf(dbc, safety)
      val updateBson = Bson.toDBObject(up(coll).toBson)
      if (logger.isTraceEnabled)
        logger.trace(dbc.getName + ".update(" + this + ", " + updateBson +
                     "), safety=" + cs)
      val result = if (updateBson.keySet.isEmpty)
                     dbc.count(queryBson)
                   else {
                     val wr = dbc.update(queryBson, updateBson, false, true,
                                         writeConcern(cs))
                     cs match {
                       case _: Safety.Safe => wr.getN
                       case _ => 0
                     }
                   }
      if (logger.isTraceEnabled)
        logger.trace(dbc.getName + ".update result is " + result)
      result
    }
  }
  def update[CC >: C <: Collection](
        up: CC => Update[c.type] forSome { val c: CC },
        safety: Safety = Safety.Default, timeout: Int = 0)(
        implicit witness: C <:< AssociatedCollection): Long =
    updateIn[CC](witness(coll).getDbCollection, up, safety, timeout)
  def updateOneIn[CC >: C <: Collection](
        dbc: DBCollection, up: CC => Update[c.type] forSome { val c: CC },
        upsert: Boolean = false, safety: Safety = Safety.Default,
        timeout: Int = 0): Boolean = {
    if (queryBson == null && !upsert) {
      if (logger.isTraceEnabled) {
        val cs = Collection.safetyOf(dbc, safety)
        val updateBson = Bson.toDBObject(up(coll).toBson)
        logger.trace(dbc.getName + ".updateOne(" + this + ", " + updateBson +
                     "), upsert=" + upsert + ", safety=" + cs)
        logger.trace(dbc.getName + ".updateOne result is false")
      }
      false
    } else {
      val cs = Collection.safetyOf(dbc, safety)
      val updateBson = Bson.toDBObject(up(coll).toBson)
      if (logger.isTraceEnabled)
        logger.trace(dbc.getName + ".updateOne(" + this + ", " + updateBson +
                     "), upsert=" + upsert + ", safety=" + cs)
      val result = if (updateBson.keySet.isEmpty && !upsert)
                     dbc.findOne(queryBson, projectionBson) != null
                   else {
                     val wr = dbc.update(
                                if (queryBson == null) falseQueryBson
                                else queryBson, updateBson, upsert, false,
                                writeConcern(cs))
                     cs match {
                       case _: Safety.Safe => wr.getN == 1
                       case _ => false
                     }
                   }
      if (logger.isTraceEnabled)
        logger.trace(dbc.getName + ".updateOne result is " + result)
      result
    }
  }
  def updateOne[CC >: C <: Collection](
        up: CC => Update[c.type] forSome { val c: CC },
        upsert: Boolean = false, safety: Safety = Safety.Default,
        timeout: Int = 0)(
        implicit witness: C <:< AssociatedCollection): Boolean =
    updateOneIn[CC](witness(coll).getDbCollection, up, upsert, safety, timeout)
  def findAndUpdateIn[CC >: C <: Collection](
        dbc: DBCollection, up: CC => Update[c.type] forSome { val c: CC },
        upsert: Boolean = false, returnUpdated: Boolean = false,
        safety: Safety = Safety.Default,
        timeout: Int = 0): Option[C#DocRepr] = {
    if (queryBson == null && !upsert) {
      if (logger.isTraceEnabled) {
        val cs = Collection.safetyOf(dbc, safety)
        val updateBson = Bson.toDBObject(up(coll).toBson)
        logger.trace(dbc.getName + ".findAndUpdate(" + this + ", " +
                     updateBson + "), upsert=" + upsert +
                     ", returnUpdated=" + returnUpdated + ", safety=" + cs)
        logger.trace(dbc.getName + ".findAndUpdate result is null")
      }
      None
    } else {
      val cs = Collection.safetyOf(dbc, safety)
      val updateBson = Bson.toDBObject(up(coll).toBson)
      if (logger.isTraceEnabled)
        logger.trace(dbc.getName + ".findAndUpdate(" + this + ", " +
                     updateBson + "), upsert=" + upsert +
                     ", returnUpdated=" + returnUpdated + ", safety=" + cs)
      val dbo = if (updateBson.keySet.isEmpty && !upsert)
                  dbc.findOne(queryBson, projectionBson)
                else
                  dbc.findAndModify(if (queryBson == null) falseQueryBson
                                    else queryBson, projectionBson, sortBson,
                                    false, updateBson, returnUpdated, upsert)
      if (logger.isTraceEnabled)
        logger.trace(dbc.getName + ".findAndUpdate result is " + dbo)
      if (dbo == null || (upsert && !returnUpdated && dbo.keySet.isEmpty))
        None
      else
        Some(reprFromBson(dbo))
    }
  }
  def findAndUpdate[CC >: C <: Collection](
        up: CC => Update[c.type] forSome { val c: CC },
        upsert: Boolean = false, returnUpdated: Boolean = false,
        safety: Safety = Safety.Default, timeout: Int = 0)(
        implicit witness: C <:< AssociatedCollection): Option[C#DocRepr] =
    findAndUpdateIn[CC](
      witness(coll).getDbCollection, up, upsert, returnUpdated, safety, timeout)

  def replaceIn(
        dbc: DBCollection, doc: C#DocRepr, upsert: Boolean = false,
        safety: Safety = Safety.Default, timeout: Int = 0): Boolean = {
    if (queryBson == null) {
      if (upsert) {
        if (logger.isTraceEnabled)
          logger.trace(dbc.getName + ".replace filter is always false, using " +
                       dbc.getName + ".insert")
        coll.insertInto(dbc, doc.asInstanceOf[coll.DocRepr], safety, timeout)
        true
      } else {
        if (logger.isTraceEnabled) {
          val cs = safetyOf(dbc, safety)
          val dbo = coll.toDBObject(doc.asInstanceOf[coll.DocRepr])
          logger.trace(dbc.getName + ".replace(" + this + ", " + dbo +
                       "), upsert=false, safety=" + cs)
          logger.trace(dbc.getName + ".replace result is false")
        }
        false
      }
    } else {
      val cs = safetyOf(dbc, safety)
      val collDoc = doc.asInstanceOf[coll.DocRepr]
      val dbo = if (upsert)
                  coll.toDBObject(collDoc)
                else
                  Bson.toDBObject(coll.toBson(collDoc))
      if (logger.isTraceEnabled)
        logger.trace(dbc.getName + ".replace(" + this + ", " + dbo +
                     "), upsert=" + upsert + ", safety=" + cs)
      val wr = handleErrors(dbc.update(queryBson, dbo, upsert, false,
                                       writeConcern(cs)))
      val result = cs match {
        case _: Safety.Safe => wr.getN > 0
        case _ => false
      }
      if (logger.isTraceEnabled)
        logger.trace(dbc.getName + ".replace result is " + result)
      result
    }
  }
  def replace(
        doc: C#DocRepr, upsert: Boolean = false,
        safety: Safety = Safety.Default, timeout: Int = 0)(
        implicit witness: C <:< AssociatedCollection): Boolean =
    replaceIn(witness(coll).getDbCollection, doc, upsert, safety, timeout)

  def removeFrom(dbc: DBCollection, safety: Safety = Safety.Default,
                 timeout: Int = 0): Long = {
    if (queryBson == null) {
      if (logger.isTraceEnabled) {
        val cs = safetyOf(dbc, safety)
        logger.trace(dbc.getName + ".remove(" + this + "), safety=" + cs)
        logger.trace(dbc.getName + ".remove result is false")
      }
      0
    } else {
      val cs = safetyOf(dbc, safety)
      if (logger.isTraceEnabled)
        logger.trace(dbc.getName + ".remove(" + this + "), safety=" + cs)
      val wr = handleErrors(dbc.remove(queryBson, writeConcern(cs)))
      val result = cs match {
        case _: Safety.Safe => wr.getN
        case _ => 0
      }
      if (logger.isTraceEnabled)
        logger.trace(dbc.getName + ".remove result is " + result)
      result
    } 
  }
  def remove(safety: Safety = Safety.Default, timeout: Int = 0)(
             implicit witness: C <:< AssociatedCollection): Long =
    removeFrom(witness(coll).getDbCollection, safety, timeout)
  def removeOneFrom(dbc: DBCollection, safety: Safety = Safety.Default,
                    timeout: Int = 0): Boolean = false
  def removeOne(safety: Safety = Safety.Default, timeout: Int = 0)(
                implicit witness: C <:< AssociatedCollection): Boolean =
    removeOneFrom(witness(coll).getDbCollection, safety, timeout)
  def findAndRemoveFrom[CC >: C <: Collection](
        dbc: DBCollection, safety: Safety = Safety.Default,
        timeout: Int = 0): Option[C#DocRepr] = {
    if (queryBson == null) {
      if (logger.isTraceEnabled) {
        val cs = Collection.safetyOf(dbc, safety)
        logger.trace(dbc.getName + ".findAndRemove(" + this + "), safety=" + cs)
        logger.trace(dbc.getName + ".findAndRemove result is null")
      }
      None
    } else {
      val cs = Collection.safetyOf(dbc, safety)
      if (logger.isTraceEnabled)
        logger.trace(dbc.getName + ".findAndRemove(" + this + "), safety=" + cs)
      val dbo = dbc.findAndModify(queryBson, projectionBson, sortBson,
                                  true, null, false, false)
      if (logger.isTraceEnabled)
        logger.trace(dbc.getName + ".findAndRemove result is " + dbo)
      if (dbo == null)
        None
      else
        Some(reprFromBson(dbo))
    }
  }
  def findAndRemove(
        safety: Safety = Safety.Default, timeout: Int = 0)(
        implicit witness: C <:< AssociatedCollection): Option[C#DocRepr] =
    findAndRemoveFrom(witness(coll).getDbCollection, safety, timeout)

  override def toString = "Query(" + queryBson + ", sort=" + sortBson +
                                 ", projection=" + projectionBson + ")"
}
