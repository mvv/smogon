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

class MyCollection extends DefaultReprCollection {
  object id extends IdFieldD[BsonId]
  object field extends IntFieldD[Short]
  object opt extends OptStringFieldD[String]
  object embedded extends EmbeddingFieldDD {
    object innerField extends DoubleFieldD[Double]
    innerField
  }
  id | field | opt | embedded
}

object MyCollection extends MyCollection

object MyCollectionEx extends MyCollection {
  object extra extends DoubleFieldD[Float]
}

object SimpleTest {
  var mongo: Mongo = null
  var db: DB = null
  var dbc: DBCollection = null
}

class SimpleTest extends SpecificationWithJUnit {
  import SimpleTest._

  doBeforeSpec {
    mongo = new Mongo
    db = mongo.getDB("smogon_tests")
    dbc = db.getCollection("mycoll")
  }

  doAfterSpec {
    db.dropDatabase()
  }

  "Identity properties must have name '_id'" in {
    MyCollection.id.fieldName must_== "_id"
  }
  
  "Number of fields must be correct" in {
    MyCollection.fields.size must_== 4
  }

  "Setting default safety level must actually set it" in {
    MyCollection.defaultSafetyOf(dbc, Safety.Safe())
    MyCollection.defaultSafetyOf(dbc).isInstanceOf[Safety.Safe] must_== true
  }

  "Creating index must succeed" in {
    MyCollection.ensureIndexIn(dbc, m => m.field & m.opt.desc)
    true must_== true
  }

  "Saving must set _id" in {
    val my = MyCollection.create
    MyCollection.saveInto(dbc, my)
    val id = MyCollection.id.get(my)
    id must_!= BsonId.Zero
  }

  "Querying must succeed" in {
    val q = MyCollection(m => m.field > -10 || m.opt === "hello")
    q.findOneIn(dbc).isDefined must_== true
  }

  "Updating must succeed" in {
    MyCollection().updateIn(dbc, m => m.field := 100) must_== 1
  }

  "Updating must actually update" in {
    MyCollection(m => m.field === 100).findOneIn(dbc).isDefined must_== true
  }
}
