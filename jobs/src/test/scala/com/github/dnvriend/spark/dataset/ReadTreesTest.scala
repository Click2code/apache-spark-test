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

package com.github.dnvriend.spark.dataset

import com.github.dnvriend.TestSpec
import com.github.dnvriend.TestSpec.Tree

import scala.language.postfixOps

// note: in dutch, a tree is called 'boom'
class ReadTreesTest extends TestSpec {
  val oldestTree =
    Tree("BOMEN.fid-42c8dacd_14bfe306016_aff", "950572", "boom in verharding", "Onbekend", Some("overig"), Some(8586), "Verharding", "POINT (147106.879 483519.092)")

  it should "get the oldest tree using sql" in withTrees { spark => trees =>
    import spark.implicits._
    trees.createOrReplaceTempView("trees")
    trees.show(10, truncate = false)

    // number of trees in the dataset
    trees.count shouldBe 148648 // 148k trees in the city
    trees.sqlContext.sql("FROM trees ORDER BY jaar DESC LIMIT 1").as[Tree].head shouldBe oldestTree
  }

  it should "get the oldest tree using dsl" in withTrees { spark => trees =>
    import spark.implicits._
    trees.orderBy('jaar.desc).limit(1).head shouldBe oldestTree
  }

  it should "get the tree family there is most of" in withTrees { spark => trees =>
    import spark.implicits._
    import org.apache.spark.sql.functions._
    trees
      .filter('soort isNotNull)
      .filter('soort notEqual "overig")
      .groupBy('soort)
      .agg(count('soort).as("count"))
      .orderBy('count.desc)
      .select('soort)
      .limit(1)
      .as[String].head shouldBe "Populier"
  }
}
