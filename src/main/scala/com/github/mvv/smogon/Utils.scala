package com.github.mvv.smogon

import scala.collection.{SetLike, MapLike}
import org.bson.BSONObject
import com.mongodb.DBObject

final class KeySet[K] private (
              map: Map[K, _], extra: Set[K])
            extends Set[K] with SetLike[K, KeySet[K]] {
  def this(map: Map[K, _]) = this(map, Set.empty)

  override def empty = new KeySet[K](Map.empty, Set.empty)
  def contains(elem: K) = extra(elem) || map.contains(elem)
  def iterator = extra.iterator ++ map.keysIterator
  override def size = map.size + extra.size
  def +(elem: K): KeySet[K] =
    if (contains(elem))
      this
    else
      new KeySet(map, extra + elem)
  override def ++(elems: TraversableOnce[K]): KeySet[K] =
    elems.foldLeft(this)(_ + _)
  def -(elem: K): KeySet[K] =
    if (extra(elem))
      new KeySet(map, extra - elem)
    else if (map.contains(elem))
      new KeySet(map - elem, extra)
    else
      this
  override def --(elems: TraversableOnce[K]): KeySet[K] = 
    elems.foldLeft(this)(_ - _)
}

final class SuperSet[A, I] private(
              set: Set[I], inj: I => A, conv: A => Option[I],
              extra: Set[A])
            extends Set[A] with SetLike[A, SuperSet[A, I]] {
  def this(set: Set[I], inj: I => A, conv: A => Option[I]) =
    this(set, inj, conv, Set.empty)

  override def empty = new SuperSet[A, I](Set.empty, inj, conv, Set.empty)
  def contains(elem: A) = conv(elem).map(set(_)).getOrElse(extra(elem))
  def iterator = set.iterator.map(inj(_)) ++ extra.iterator
  override def size = set.size + extra.size
  def +(elem: A) = conv(elem).map { e =>
      if (set(e))
        this
      else
        new SuperSet(set + e, inj, conv, extra)
    } .getOrElse {
      if (extra(elem))
        this
      else
        new SuperSet(set, inj, conv, extra + elem)
    }
  def -(elem: A) = conv(elem).map { e =>
      if (set(e))
        new SuperSet(set - e, inj, conv, extra)
      else
        this
    } .getOrElse {
      if (extra(elem))
        new SuperSet(set, inj, conv, extra - elem)
      else
        this
    }
}

final class SuperMap[A, I, +B] private(
              map: Map[I, B], inj: I => A, conv: A => Option[I],
              extra: Map[A, B])
            extends Map[A, B] with MapLike[A, B, SuperMap[A, I, B]] {
  def this(map: Map[I, B], inj: I => A, conv: A => Option[I]) =
    this(map, inj, conv, Map.empty)

  override def empty = new SuperMap[A, I, B](Map.empty, inj, conv, Map.empty)
  def get(key: A) = conv(key).map(map.get(_)).getOrElse(extra.get(key))
  def iterator = map.iterator.map { case (k, v) => inj(k) -> v } ++
                 extra.iterator
  override def size = map.size + extra.size
  def +[B1 >: B](kv: (A, B1)): SuperMap[A, I, B1] = conv(kv._1).map { k =>
      new SuperMap(map + (k -> kv._2), inj, conv, extra)
    } .getOrElse {
      new SuperMap(map, inj, conv, extra + kv)
    }
  override def ++[B1 >: B](kvs: TraversableOnce[(A, B1)]): SuperMap[A, I, B1] =
    kvs.foldLeft[SuperMap[A, I, B1]](this)(_ + _)
  def -(key: A): SuperMap[A, I, B] = conv(key).map { k =>
      new SuperMap(map - k, inj, conv, extra)
    } .getOrElse {
      if (extra.contains(key))
        new SuperMap(map, inj, conv, extra - key)
      else
        this
    }
  override def --(keys: TraversableOnce[A]): SuperMap[A, I, B] =
    keys.foldLeft(this)(_ - _)
}

sealed abstract class DocumentDBObject[D <: Document](
                        val docDef: D, docRepr: D#DocRepr)
                      extends DBObject {
  import scala.collection.JavaConversions._

  def repr: docDef.DocRepr
  def putAll(map: java.util.Map[_, _]) =
    map.asInstanceOf[java.util.Map[AnyRef, AnyRef]].foreach { case (k, v) =>
      put(k.toString, v)
    }
  def putAll(obj: BSONObject) =
    obj.keySet.foreach { k => put(k, obj.get(k)) }
  def removeField(key: String) = throw new UnsupportedOperationException
  def containsField(key: String) =
    docDef.stringToName(key).map { name =>
      docDef.containsField(repr, name)
    } .getOrElse(false)
  def containsKey(key: String) = containsField(key)
  def toMap = Map[AnyRef, AnyRef]()
  def isPartialObject = false
  def markAsPartialObject() {}
  override def toString = com.mongodb.util.JSON.serialize(this)
}

final class StaticDocumentDBObject[D <: StaticDocument](
              override val docDef: D, docRepr: D#DocRepr)
            extends DocumentDBObject(docDef, docRepr) {
  import scala.collection.JavaConversions._

  private var doc = docRepr.asInstanceOf[docDef.DocRepr]

  def repr = doc
  def get(key: String): AnyRef = docDef.field(key) match {
    case Some(field) =>
      field.toRaw(field.get(doc))
    case _ =>
      if (key == "_transientFields") Nil
      else throw new NoSuchElementException(key)
  }
  def put(key: String, value: AnyRef): AnyRef = docDef.field(key) match {
    case Some(field) =>
      doc = field.set(doc, field.fromRaw(value))
      value
    case None =>
      value
  }
  def keySet = docDef.fieldsNamesSet
}

final class DynamicDocumentDBObject[D <: DynamicDocument](
              override val docDef: D, docRepr: D#DocRepr)
            extends DocumentDBObject(docDef, docRepr) {
  import scala.collection.JavaConversions._

  private var doc = docRepr.asInstanceOf[docDef.DocRepr]

  def repr = doc
  def get(key: String): AnyRef = 
    (for {
      name <- docDef.stringToName(key)
      value <- docDef.get(doc, name)
    } yield {
      docDef.field.toRaw(value)
    }).getOrElse {
      if (key == "_transientFields") Nil
      else throw new NoSuchElementException(key)
    }
  def put(key: String, value: AnyRef): AnyRef = {
    docDef.stringToName(key).foreach { name =>
      doc = docDef.set(doc, name, docDef.field.fromRaw(value))
    }
    value
  }
  def keySet = new SuperSet[String, docDef.FieldName](
                     docDef.fieldsNamesSet(doc),
                     docDef.nameToString(_), docDef.stringToName(_))
}


