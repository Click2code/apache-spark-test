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

package com.github.dnvriend.spark.datasources

import com.github.dnvriend.TestSpec
import com.github.dnvriend.spark.datasources.SparkImplicits._
import org.apache.spark.sql.DataFrame

class PersonDataSourceTest extends TestSpec {
  it should "read a simple person xml file using a custom data source" in withSparkSession { spark =>
    import spark.implicits._
    val result: DataFrame = spark.read
      .format("person")
      .load("src/test/resources/people.xml")

    result.as[(Long, String, Int)].collect shouldBe Seq(
      (1, "Jonathan Archer", 41),
      (2, "Reginald Barclay", 45),
      (3, "Julian Bashir", 28),
      (4, "Pavel Chekov", 52),
      (5, "Beverly Crusher", 32),
      (6, "Jadzia Dax", 21),
      (7, "Geordi La Forge", 35)
    )
  }

  it should "read a simple person xml file using implicit conversion" in withSparkSession { spark =>
    import spark.implicits._
    val result: DataFrame = spark.read.person("src/test/resources/people.xml")

    result.as[(Long, String, Int)].collect shouldBe Seq(
      (1, "Jonathan Archer", 41),
      (2, "Reginald Barclay", 45),
      (3, "Julian Bashir", 28),
      (4, "Pavel Chekov", 52),
      (5, "Beverly Crusher", 32),
      (6, "Jadzia Dax", 21),
      (7, "Geordi La Forge", 35)
    )
  }
}