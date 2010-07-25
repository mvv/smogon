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
import com.github.mvv.layson.bson._
import com.mongodb.{
         DBObject, BasicDBObject, BasicDBList, DBCollection, WriteResult}

sealed trait ValueFilter[V <: ReprBsonValue] {
  import ValueFilter._

  def unary_!() = Not(this)
  def &&(filter: ValueFilter[V]) = And(this, filter)
  def ||(filter: ValueFilter[V]) = Or(this, filter)
}

object ValueFilter {
  final case class Eq[V <: ReprBsonValue](value: V#ValueRepr)
                   extends ValueFilter[V]
  final case class In[V <: ReprBsonValue](values: Set[V#ValueRepr])
                   extends ValueFilter[V]
  final case class Less[V <: ReprBsonValue](
                     value: V#ValueRepr)(
                     implicit witness: V#Bson <:< OptSimpleBsonValue)
                   extends ValueFilter[V]
  final case class LessOrEq[V <: ReprBsonValue](
                     value: V#ValueRepr)(
                     implicit witness: V#Bson <:< OptSimpleBsonValue)
                   extends ValueFilter[V]
  final case class Mod[V <: ReprBsonValue](
                     divisor: V#ValueRepr, result: V#ValueRepr)(
                     implicit witness: V#Bson <:< OptIntegralBsonValue)
                   extends ValueFilter[V]
  final case class Match[V <: ReprBsonValue](
                     regex: String, flags: String)(
                     implicit witness: V#Bson <:< OptBsonStr)
                   extends ValueFilter[V]
  final case class Not[V <: ReprBsonValue](filter: ValueFilter[V])
                   extends ValueFilter[V]
  final case class And[V <: ReprBsonValue](
                     first: ValueFilter[V], second: ValueFilter[V])
                   extends ValueFilter[V]
  final case class Or[V <: ReprBsonValue](
                     first: ValueFilter[V], second: ValueFilter[V])
                   extends ValueFilter[V]
}

final class ValueFilterBuilder[V <: ReprBsonValue] {
  def ===(value: V#ValueRepr) = ValueFilter.Eq(value)
  def !==(value: V#ValueRepr) = !(this === value)
  def in(values: Set[V#ValueRepr]) = ValueFilter.In(values)
  def notIn(values: Set[V#ValueRepr]) = !(this in values)
}

object ValueFilterBuilder {
  final class BasicFilterOps[V <: ReprBsonValue] private[ValueFilterBuilder]()(
                implicit witness: V#Bson <:< OptSimpleBsonValue) {
    def <(value: V#ValueRepr) = ValueFilter.Less[V](value)
    def <=(value: V#ValueRepr) = ValueFilter.LessOrEq[V](value)
    def >(value: V#ValueRepr) = !(this <= value)
    def >=(value: V#ValueRepr) = !(this < value)
  }

  implicit def basicFilterOps[V <: ReprBsonValue](
                 b: ValueFilterBuilder[V])(
                 implicit witness: V#Bson <:< OptSimpleBsonValue) =
    new BasicFilterOps

  final class ModResultFilter[V <: ReprBsonValue] private[ValueFilterBuilder](
                divisor: V#ValueRepr)(
                implicit witness: V#Bson <:< OptIntegralBsonValue) {
    def ===(result: V#ValueRepr) = ValueFilter.Mod[V](divisor, result)
    def !==(result: V#ValueRepr) = !(this === result)
  }

  final class IntegralFilterOps[V <: ReprBsonValue] private[ValueFilterBuilder]()(
                implicit witness: V#Bson <:< OptIntegralBsonValue) {
    def %(divisor: V#ValueRepr) = new ModResultFilter[V](divisor)
  }

  implicit def integralFilterOps[V <: ReprBsonValue](
                 b: ValueFilterBuilder[V])(
                 implicit witness: V#Bson <:< OptIntegralBsonValue) =
    new IntegralFilterOps

  final class StringFilterOps[V <: ReprBsonValue]()(
                implicit witness: V#Bson <:< OptBsonStr) {
    def =~(rf: (String, String)) = ValueFilter.Match[V](rf._1, rf._2)
    def =~(regex: String) = ValueFilter.Match[V](regex, "")
    def !~(rf: (String, String)) = !(this =~ rf)
    def !~(regex: String) = !(this =~ regex)
  }

  implicit def stringFilterOps[V <: ReprBsonValue](
                 b: ValueFilterBuilder[V])(
                 implicit witness: V#Bson <:< OptBsonStr) =
    new StringFilterOps
}

sealed trait Filter[R <: Documents] {
  import Filter._

  def unary_!(): Filter[R]
  def &&(filter: Filter[R]): Filter[R] = And(this, filter)
  def ||(filter: Filter[R]): Filter[R] = Or(this, filter)

  def linearize: Iterator[Filter[R]] = Iterator.single(this)
  def normalForm: Filter[R] = this

  def toBson: DBObject
}

object Filter {
  sealed trait Simple[D <: Document, F <: D#FieldBase] extends Filter[D#Root] {
    def unary_!(): Filter[D#Root] = Not(this)
    val field: F
    def conditionBson: AnyRef
    def toBson = {
      val bson = new BasicDBObject
      bson.put(field.fieldRootName, conditionBson)
      bson
    }
  }
  final case class Not[D <: Document, F <: D#FieldBase](filter: Simple[D, F])
                   extends Simple[D, F] {
    override def unary_!() = filter
    val field = filter.field
    def conditionBson = {
      val bson = new BasicDBObject
      filter match {
        case Eq(_, _) => bson.put("$ne", filter.conditionBson)
        case _ => bson.put("$not", filter.conditionBson)
      }
      bson
    }
  }
  final case class And[R <: Documents](first: Filter[R], second: Filter[R])
                   extends Filter[R] {
    def unary_!() = Or(!first, !second)
    override def linearize: Iterator[Filter[R]] = (first, second) match {
      case (f @ And(_, _), s @ And(_, _)) => f.linearize ++ s.linearize
      case (f @ And(_, _), s) => f.linearize ++ Iterator.single(s)
      case (f, s @ And(_, _)) => Iterator.single(f) ++ s.linearize
      case _ => Iterator(first, second)
    }
    override def normalForm = And(first.normalForm, second.normalForm)
    def toBson = {
      val bson = new BasicDBObject
      linearize.foreach(b => bson.putAll(b.toBson))
      bson
    }
  }
  final case class Or[R <: Documents](first: Filter[R], second: Filter[R])
                   extends Filter[R] {
    def unary_!() = And(!first, !second)
    override def linearize: Iterator[Filter[R]] = (first, second) match {
      case (f @ Or(_, _), s @ Or(_, _)) => f.linearize ++ s.linearize
      case (f @ Or(_, _), s) => f.linearize ++ Iterator.single(s)
      case (f, s @ Or(_, _)) => Iterator.single(f) ++ s.linearize
      case _ => Iterator(first, second)
    }
    override def normalForm = (first.normalForm, second.normalForm) match {
      case (f @ And(_, _), s @ And(_, _)) =>
        (for { d1 <- f.linearize
               d2 <- s.linearize }
           yield Or[R](d1, d2)).reduceLeft[Filter[R]](And(_, _))
      case (f, s @ And(_, _)) =>
        s.linearize.map(Or(f, _)).reduceLeft[Filter[R]](And(_, _)) 
      case (f @ And(_, _), s) =>
        f.linearize.map(Or(s, _)).reduceLeft[Filter[R]](And(_, _)) 
      case (f, s) => Or(f, s)
    }
    def toBson = {
      val bson = new BasicDBObject
      val list = new BasicDBList
      linearize.foreach(b => list.add(b.toBson))
      bson.put("$or", list)
      bson
    }
  }
  final case class Eq[D <: Document, F <: D#BasicFieldBase](
                      field: F, value: F#Repr) extends Simple[D, F] {
    def conditionBson = Bson.toRaw(field.toBson(value.asInstanceOf[field.Repr]))
  }
  final case class In[D <: Document, F <: D#BasicFieldBase](
                      field: F, values: Set[F#Repr]) extends Simple[D, F] {
    def conditionBson = {
      val bson = new BasicDBObject
      val list = new BasicDBList
      values.foreach(v =>
        list.add(Bson.toRaw(field.toBson(v.asInstanceOf[field.Repr]))))
      bson.put("$in", list)
      bson
    }
  }
  final case class Less[D <: Document, F <: D#BasicFieldBase](
                     field: F, value: F#Repr)(
                     implicit witness: F#Bson <:< OptSimpleBsonValue)
                   extends Simple[D, F] {
    def conditionBson = {
      val bson = new BasicDBObject
      bson.put("$lt", Bson.toRaw(field.toBson(value.asInstanceOf[field.Repr])))
      bson
    }
  }
  final case class LessOrEq[D <: Document, F <: D#BasicFieldBase](
                     field: F, value: F#Repr)(
                     implicit witness: F#Bson <:< OptSimpleBsonValue)
                   extends Simple[D, F] {
    def conditionBson = {
      val bson = new BasicDBObject
      bson.put("$lte", Bson.toRaw(field.toBson(value.asInstanceOf[field.Repr])))
      bson
    }
  }
  final case class Mod[D <: Document, F <: D#BasicFieldBase](
                     field: F, divisor: F#Repr, result: F#Repr)(
                     implicit witness: F#Bson <:< OptIntegralBsonValue)
                   extends Simple[D, F] {
    def conditionBson = {
      val bson = new BasicDBObject
      val args = new BasicDBList
      args.add(Bson.toRaw(field.toBson(divisor.asInstanceOf[field.Repr])))
      args.add(Bson.toRaw(field.toBson(result.asInstanceOf[field.Repr])))
      bson.put("$mod", args)
      bson
    }
  }
  final case class Match[D <: Document, F <: D#BasicFieldBase](
                     field: F, regex: Regex)(
                     implicit witness: F#Bson <:< OptBsonStr)
                   extends Simple[D, F] {
    def conditionBson = regex.pattern
  }
  final case class Size[D <: Document, F <: D#ArrayFieldBase](
                     field: F, size: Long) extends Simple[D, F] {
    def conditionBson = {
      val bson = new BasicDBObject
      bson.put("$size", size)
      bson
    }
  }
  final case class ContainsElem[D <: Document, F <: D#ElementsArrayFieldBase](
                     field: F, filter: ValueFilter[F]) extends Simple[D, F] {
    def conditionBson = throw new UnsupportedOperationException
  }
  final case class Contains[D <: Document, F <: D#DocumentsArrayFieldBase](
                     field: F, filter: Filter[F]) extends Simple[D, F] {
    def conditionBson = throw new UnsupportedOperationException
  }
}

sealed trait Update[D <: Documents]

final class Query[+C <: Collection] private(
              coll: C, queryBson: DBObject, sortBson: DBObject,
              projectionBson: DBObject) {
  import Collection._

  private[smogon] def this(coll: C, filter: Filter[C]) =
    this(coll, filter.normalForm.toBson, new BasicDBObject, new BasicDBObject)
  private[smogon] def this(coll: C) =
    this(coll, new BasicDBObject, new BasicDBObject, new BasicDBObject)

  import scala.collection.JavaConversions._

  private def reprFromBson(obj: DBObject): C#DocRepr = {
    val repr = coll.create
    coll.dbObject(repr).putAll(obj)
    repr
  }

  def only[CC >: C <: Collection](
        proj: CC => Projection[c.type] forSome { val c: CC }) =
    new Query[C](coll, queryBson, sortBson, proj(coll).toBson(true))
  def except[CC >: C <: Collection](
        proj: CC => Projection[c.type] forSome { val c: CC }) =
    new Query[C](coll, queryBson, sortBson, proj(coll).toBson(false))
  def sort[CC >: C <: Collection](sort: CC => Sort[c.type] forSome { val c: CC }) =
    new Query[C](coll, queryBson, sort(coll).toBson, projectionBson)

  def findIn(dbc: DBCollection, skip: Int = 0, limit: Int = -1): Iterator[C#DocRepr] = 
    if (limit == 0)
      Iterator.empty
    else
      asIterator {
        dbc.find(queryBson, projectionBson, skip, if (limit > 0) -limit else 0)
      } .map(reprFromBson(_))
  def find(skip: Int = 0, limit: Int = -1)(
           implicit witness: C <:< AssociatedCollection): Iterator[C#DocRepr] =
    findIn(witness(coll).getDbCollection, skip, limit)
  def findOneIn(dbc: DBCollection): Option[C#DocRepr] =
    dbc.findOne(queryBson, projectionBson) match {
      case null => None
      case obj => Some(reprFromBson(obj))
    }
  def findOne()(implicit witness: C <:< AssociatedCollection): Option[C#DocRepr] =
    findOneIn(witness(coll).getDbCollection)

  def count(dbc: DBCollection): Long = dbc.count(queryBson)
  def count()(implicit witness: C <:< AssociatedCollection): Long =
    count(witness(coll).getDbCollection)

  def updateIn[CC >: C <: Collection](
        dbc: DBCollection, up: CC => Update[c.type] forSome { val c: CC },
        safety: Safety = Safety.Default, timeout: Int = 0): Long = 0
  def update[CC >: C <: Collection](
        up: CC => Update[c.type] forSome { val c: CC },
        safety: Safety = Safety.Default, timeout: Int = 0)(
        implicit witness: C <:< AssociatedCollection): Long =
    updateIn[CC](witness(coll).getDbCollection, up, safety, timeout)
  def updateOneIn[CC >: C <: Collection](
        dbc: DBCollection, up: CC => Update[c.type] forSome { val c: CC },
        safety: Safety = Safety.Default, timeout: Int = 0): Boolean = false
  def updateOne[CC >: C <: Collection](
        up: CC => Update[c.type] forSome { val c: CC },
        safety: Safety = Safety.Default, timeout: Int = 0)(
        implicit witness: C <:< AssociatedCollection): Boolean =
    updateOneIn[CC](witness(coll).getDbCollection, up, safety, timeout)

  def removeFrom(dbc: DBCollection, safety: Safety = Safety.Default,
                 timeout: Int = 0): Long = {
    val cs = safetyOf(dbc, safety)
    val wr = handleErrors(dbc.remove(queryBson))
    cs match {
      case Safety.Safe(_, _) => wr.getN
      case _ => 0
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

  override def toString = "Query(" + queryBson + ", " + sortBson + ", " +
                                     projectionBson + ")"
}
