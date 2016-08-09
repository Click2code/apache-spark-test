/*
 * Copyright 2016 Dennis Vriend
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

package com.github.dnvriend.spark

import com.github.dnvriend.TestSpec
import com.github.dnvriend.TestSpec.Person

class DatasetOperationTest extends TestSpec {
  lazy val p1 = Person(1, "foo", 30)
  lazy val p2 = Person(2, "bar", 25)
  lazy val p3 = Person(3, "baz", 41)

  // Returns a new Dataset containing union of rows in this Dataset and another Dataset.
  it should "union datasets" in withSpark { spark =>
    import spark.implicits._
    val ds1 = Seq(p1).toDS()
    val ds2 = Seq(p2).toDS()
    ds1.union(ds2).collect shouldBe Seq(p1, p2)
  }

  it should "union multiple datasets" in withSpark { spark =>
    import spark.implicits._
    val ds1 = Seq(p1).toDS
    val ds2 = Seq(p2).toDS
    val ds3 = Seq(p3).toDS
    ds1.union(ds2).union(ds3).collect shouldBe Seq(p1, p2, p3)
  }

  // **
  // 'intersect' and 'except' returns distinct rows by comparing the results of two queries
  // **

  //intersect returns distinct rows that are output by both LEFT and RIGHT result
  // Returns a new Dataset containing rows only in both this Dataset and another Dataset
  it should "intersect" in withSpark { spark =>
    import spark.implicits._
    val ds1 = Seq(p1).toDS
    val ds2 = Seq(p2).toDS
    ds1.intersect(ds2).collect shouldBe Nil
  }

  it should "intersect when corresponding rows" in withSpark { spark =>
    import spark.implicits._
    val ds1 = Seq(p1).toDS
    val ds2 = Seq(p1).toDS
    ds1.intersect(ds2).collect shouldBe Seq(p1)
  }

  // EXCEPT returns distinct rows from the LEFT result that aren't output by the RIGHT result
  // Returns a new Dataset containing rows in this Dataset but not in another Dataset.
  it should "except only p1 for ds1=Seq(p1) and ds2=Seq(p2)" in withSpark { spark =>
    import spark.implicits._
    val ds1 = Seq(p1).toDS
    val ds2 = Seq(p2).toDS
    ds1.except(ds2).collect shouldBe Seq(p1)
  }

  it should "except only p1 when ds1=Seq(p1, p2) and ds2=Seq(p2)" in withSpark { spark =>
    import spark.implicits._
    val ds1 = Seq(p1, p2).toDS
    val ds2 = Seq(p2).toDS
    ds1.except(ds2).collect shouldBe Seq(p1)
  }

  it should "except only p2 when ds1=Seq(p1, p2) and ds2=Seq(p1)" in withSpark { spark =>
    import spark.implicits._
    val ds1 = Seq(p1, p2).toDS
    val ds2 = Seq(p1).toDS
    ds1.except(ds2).collect shouldBe Seq(p2)
  }

  it should "except Nil when ds1=Seq(p1, p2) and ds2=Seq(p1, p2)" in withSpark { spark =>
    import spark.implicits._
    val ds1 = Seq(p1, p2).toDS
    val ds2 = Seq(p1, p2).toDS
    ds1.except(ds2).collect shouldBe Nil
  }

  it should "except Nil when ds1=Seq(p1, p2) and ds2=Seq(p2, p1)" in withSpark { spark =>
    import spark.implicits._
    val ds1 = Seq(p1, p2).toDS
    val ds2 = Seq(p2, p1).toDS
    ds1.except(ds2).collect shouldBe Nil
  }
}
