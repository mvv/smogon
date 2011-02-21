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

package com.github.mvv.smogon.tests

import org.specs._
import com.mongodb._
import com.github.mvv.layson.bson._
import com.github.mvv.smogon._

class MyEmbedded {
  var innerField = 0.0
}

class MyDoc {
  var docField = 0
}

class MyEntity {
  var id = BsonId.Zero
  var field = 0.shortValue
  var opt: String = null
  var embedded = new MyEmbedded
  var elems: Seq[Int] = Vector.empty
  var docs: Seq[MyDoc] = Vector.empty
  var dyn: Map[String, Int] = Map.empty
}

class MyCollection extends Collection {
  type DocRepr = MyEntity
  protected def newDocRepr = new MyEntity

  object id extends IdFieldM[BsonId](_.id, _.id = _)
  object field extends IntFieldM[Short](_.field, _.field = _)
  object opt extends OptStringFieldM[String](_.opt, _.opt = _)
  object embedded extends EmbeddingFieldM[MyEmbedded](_.embedded,
                                                      _.embedded = _) {
    protected def newDocRepr = new MyEmbedded
    object innerField extends DoubleFieldM[Double](_.innerField,
                                                   _.innerField = _)
    innerField
  }
  object elems extends IntArrayFieldM[Int, Seq](_.elems, _.elems = _)
                  with SeqArrayField[Vector] {
    protected val seqFactory = Vector
  }
  object docs extends DocumentsArrayFieldM[MyDoc, Seq](_.docs, _.docs = _)
                 with SeqArrayField[Vector] {
    protected def newDocRepr = new MyDoc
    protected val seqFactory = Vector
    object docField extends IntFieldM[Int](_.docField, _.docField = _)
    docField
  }
  object dyn extends DynamicFieldM[String, Int, Map](_.dyn, _.dyn = _)
                with MapDynamicField[Map] {
    def stringToName(str: String) = Some(str)
    protected val mapFactory = Map
    object field extends IntField[Int]
    field
  }
  id | field | opt | embedded | elems | docs | dyn
}

object MyCollection extends MyCollection

class MyCollectionD extends DefaultReprCollection {
  object id extends IdFieldD[BsonId]
  object field extends IntFieldD[Short]
  object opt extends OptStringFieldD[String]
  object embedded extends EmbeddingFieldDD {
    object innerField extends DoubleFieldD[Double]
    innerField
  }
  object elems extends IntArrayFieldD[Int, Seq]
                  with SeqArrayField[Vector] {
    protected val seqFactory = Vector
  }
  object docs extends DocumentsArrayFieldDD[Seq] with SeqArrayField[Vector] {
    protected val seqFactory = Vector
    object docField extends IntFieldD[Int]
    docField
  }
  object dyn extends DynamicFieldDD[String] {
    def stringToName(str: String) = Some(str)
    object field extends IntField[Int]
    field
  }
  id | field | opt | embedded | elems | docs | dyn
}

object MyCollectionD extends MyCollectionD

object SimpleTest {
  var mongo: Mongo = null
  var db: DB = null
}

class SimpleTest extends SpecificationWithJUnit {
  import SimpleTest._

  doBeforeSpec {
    mongo = new Mongo
    db = mongo.getDB("smogon_tests")
  }

  doAfterSpec {
    db.dropDatabase()
  }

  "Custom document representation" should {
    shareVariables()
    setSequential()
    val dbc = db.getCollection("mycoll")
    var id = BsonId.Zero

    "Identity properties must have name '_id'" in {
      MyCollection.id.fieldName must_== "_id"
    }
    
    "Number of fields must be correct" in {
      MyCollection.fields.size must_== 7
    }

    "Setting default safety level must actually set it" in {
      MyCollection.defaultSafetyOf(dbc, Safety.Safe)
      MyCollection.defaultSafetyOf(dbc).isInstanceOf[Safety.Safe] must_== true
    }

    "Creating an index must succeed" in {
      MyCollection.ensureIndexIn(dbc, m => m.field & m.opt.desc)
      true must_== true
    }

    "Saving must set _id" in {
      val my = new MyEntity
      val myDoc = new MyDoc
      myDoc.docField = 10
      my.docs +:= myDoc
      MyCollection.saveInto(dbc, my)
      id = my.id
      id must_!= BsonId.Zero
    }

    "Querying must succeed" in {
      val q = MyCollection(m => m.field > -10 || m.opt === "hello")
      q.findOneIn(dbc).isDefined must_== true
    }

    "Updating a document must succeed" in {
      MyCollection(_.id === id).
        updateIn(dbc, m => m.field =# 100 && m.opt =# "hello" &&
                           m.elems =# Seq(10, 20, 30)) must_== 1
      MyCollection(m => m.field === 100 && m.opt === "hello" &&
                         m.elems.size(3)).
        findOneIn(dbc).isDefined must_== true
    }

    "Replacing a document should succeed" in {
      val myOpt = MyCollection(_.id === id).findOneIn(dbc)
      myOpt.isDefined must_== true
      val my = myOpt.get
      my.field = 255
      MyCollection(_.id === id).replaceIn(dbc, my) must_== true
      val upOpt = MyCollection(m => m.id === id && m.field === 255).
                    findOneIn(dbc) 
      upOpt.isDefined must_== true
    }

    "Quering an embedded primitives array field must succeed" in {
      MyCollection(_.elems.contains(_.in(5, 20))).
        findOneIn(dbc).isDefined must_== true
      MyCollection(_.elems.containsAll(10, 20)).
        findOneIn(dbc).isDefined must_== true
      MyCollection(_.elems.containsAll(20, 30)).
        findOneIn(dbc).isDefined must_== true
      MyCollection(_.elems.containsAll(40, 30)).
        findOneIn(dbc).isDefined must_== false
    }

    "Quering an embedded documents array field must succeed" in {
      MyCollection(_.docs.contains(_.docField === 10)).
        findOneIn(dbc).isDefined must_== true
      MyCollection(_.docs.contains(_.docField === 11)).
        findOneIn(dbc).isDefined must_== false
    }

    "UpdateOne must return true with upsert" in {
      MyCollection(_.field === 1).
        updateOneIn(dbc, _.field =# 1, upsert = true) must_== true
      MyCollection(_.field === 1).findOneIn(dbc).isDefined must_== true
    }

    "FindAndUpdate(upsert, returnUpdated) must return created document" in {
      val myOpt = MyCollection(m => m.field === 2).findAndUpdateIn(dbc, { m =>
        m.field =# 2 &&
        (m.embedded.innerField += 2.0)
      }, upsert = true, returnUpdated = true)
      myOpt.isDefined must_== true
      val my = myOpt.get
      my.field must_== 2
      my.embedded.innerField must_== 2.0
    }

    ("FindAndUpdate(upsert, !returnUpdated) " +
     "on nonexistent document must return None") in {
      val id = Collection.genId()
      MyCollection(_.id === id).findAndUpdateIn(dbc, { m =>
        m.field =# 3 &&
        (m.embedded.innerField += 2.0)
      }, upsert = true).isEmpty must_== true
      MyCollection(m => m.id === id && m.field === 3).
        findOneIn(dbc).isDefined must_== true
    }

    "Setting dynamic field must work" in {
      val updated = MyCollection(_.field === 2).updateOneIn(dbc, { m =>
        m.dyn("test") =# 10
      })
      updated must_== true
      val my = MyCollection(_.field === 2).findOneIn(dbc).get
      my.dyn.get("test") must_== Some(10)
    }
  }

  "Default document representation" should {
    shareVariables()
    setSequential()
    val dbc = db.getCollection("mycolld")
    var id = BsonId.Zero

    "Identity properties must have name '_id'" in {
      MyCollectionD.id.fieldName must_== "_id"
    }
    
    "Number of fields must be correct" in {
      MyCollectionD.fields.size must_== 7
    }

    "Setting default safety level must actually set it" in {
      MyCollectionD.defaultSafetyOf(dbc, Safety.Safe)
      MyCollectionD.defaultSafetyOf(dbc).isInstanceOf[Safety.Safe] must_== true
    }

    "Creating an index must succeed" in {
      MyCollectionD.ensureIndexIn(dbc, m => m.field & m.opt.desc)
      true must_== true
    }

    "Saving must set _id" in {
      val my = MyCollectionD.create
      val myDoc = MyCollectionD.docs.create
      MyCollectionD.docs.docField.set(myDoc, 10)
      MyCollectionD.docs.set(my, Vector(myDoc))
      MyCollectionD.saveInto(dbc, my)
      id = MyCollectionD.id.get(my)
      id must_!= BsonId.Zero
    }

    "Querying must succeed" in {
      val q = MyCollectionD(m => m.field > -10 || m.opt === "hello")
      q.findOneIn(dbc).isDefined must_== true
    }

    "Updating a document must succeed" in {
      MyCollectionD(_.id === id).
        updateIn(dbc, m => m.field =# 100 && m.opt =# "hello" &&
                            m.elems =# Seq(10, 20, 30)) must_== 1
      MyCollectionD(m => m.field === 100 && m.opt === "hello" && m.elems.size(3)).
        findOneIn(dbc).isDefined must_== true
    }

    "Replacing a document should succeed" in {
      val myOpt = MyCollectionD(_.id === id).findOneIn(dbc)
      myOpt.isDefined must_== true
      val my = myOpt.get
      MyCollectionD.field.set(my, 255)
      MyCollectionD(_.id === id).replaceIn(dbc, my) must_== true
      val upOpt = MyCollectionD(m => m.id === id && m.field === 255).
                    findOneIn(dbc) 
      upOpt.isDefined must_== true
    }

    "Quering an embedded primitives array field must succeed" in {
      MyCollectionD(m => m.elems.contains(_.in(5, 20))).
        findOneIn(dbc).isDefined must_== true
      MyCollectionD(m => m.elems.containsAll(10, 20)).
        findOneIn(dbc).isDefined must_== true
      MyCollectionD(m => m.elems.containsAll(20, 30)).
        findOneIn(dbc).isDefined must_== true
      MyCollectionD(m => m.elems.containsAll(40, 30)).
        findOneIn(dbc).isDefined must_== false
    }

    "Quering an embedded documents array field must succeed" in {
      MyCollectionD(m => m.docs.contains(_.docField === 10)).
        findOneIn(dbc).isDefined must_== true
      MyCollectionD(m => m.docs.contains(_.docField === 11)).
        findOneIn(dbc).isDefined must_== false
    }

    "UpdateOne must return true with upsert" in {
      MyCollectionD(_.field === 1).
        updateOneIn(dbc, _.field =# 1, upsert = true) must_== true
      MyCollectionD(_.field === 1).findOneIn(dbc).isDefined must_== true
    }

    "FindAndUpdate(upsert, returnUpdated) must return created document" in {
      val myOpt = MyCollectionD(m => m.field === 2).findAndUpdateIn(dbc, { m =>
        m.field =# 2 &&
        (m.embedded.innerField += 2.0)
      }, upsert = true, returnUpdated = true)
      myOpt.isDefined must_== true
      val my = myOpt.get
      MyCollectionD.field.get(my) must_== 2
      MyCollectionD.embedded.innerField.
        get(MyCollectionD.embedded.get(my)) must_== 2.0
    }

    ("FindAndUpdate(upsert, !returnUpdated) " +
     "on nonexistent document must return None") in {
      val id = Collection.genId()
      MyCollectionD(_.id === id).findAndUpdateIn(dbc, { m =>
        m.field =# 3 &&
        (m.embedded.innerField += 2.0)
      }, upsert = true).isEmpty must_== true
      MyCollectionD(m => m.id === id && m.field === 3).
        findOneIn(dbc).isDefined must_== true
    }

    "Setting dynamic field must work" in {
      val updated = MyCollectionD(_.field === 2).updateOneIn(dbc, { m =>
        m.dyn("test") =# 10
      })
      updated must_== true
      val my = MyCollectionD(_.field === 2).findOneIn(dbc).get
      MyCollectionD.dyn.get(MyCollectionD.dyn.get(my), "test") must_== Some(10)
    }
  }
}
