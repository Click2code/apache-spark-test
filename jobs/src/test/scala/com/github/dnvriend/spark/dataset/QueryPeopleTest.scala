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
import org.apache.spark.sql.{ Column, DataFrame }

class QueryPeopleTest extends TestSpec {

  it should "query using DSL" in withSparkSession { spark =>
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val people: DataFrame =
      spark.read.parquet(TestSpec.PersonsParquet).cache() // name, age

    people.select('name).limit(1).as[String].head() shouldBe "foo"
    people.select($"name").limit(1).as[String].head() shouldBe "foo"
    people.select("name").limit(1).as[String].head() shouldBe "foo"

    people.select('age).limit(1).as[Int].head() shouldBe 30
    people.select($"age").limit(1).as[Int].head() shouldBe 30
    people.select("age").limit(1).as[Int].head() shouldBe 30

    // select a column from the Dataset
    val col1: Column = people("name")
    val col2: Column = people.col("name")

    val departments: DataFrame =
      Seq((1, "sales"), (2, "administration"), (3, "human resources"))
        .toDF("department_id", "department_name").cache()

    people
      .withColumn("department_id", lit(1))
      .withColumn("age_plus_ten", people("age") + 10)
      .as[(String, Int, Int, Int)].limit(1).head() shouldBe ("foo", 30, 1, 40)

    people
      .withColumn("department_id", lit(1))
      .withColumn("age_plus_ten", people("age") + 10)
      .as('people_dep_age)
      .join(departments, col("people_dep_age.department_id").equalTo(departments.col("department_id")))
      .select($"people_dep_age.name", col("people_dep_age.age"), departments.col("department_name"))
      .as[(String, Int, String)].limit(1).head() shouldBe ("foo", 30, "sales")

    val peopleDepAge: DataFrame =
      people
        .withColumn("department_id", lit(1))
        .withColumn("age_plus_ten", people("age") + 10)

    peopleDepAge
      .join(departments, peopleDepAge("department_id") === departments("department_id"))
      .select(peopleDepAge("name"), peopleDepAge("age"), departments("department_name"))
      .as[(String, Int, String)].limit(1).head() shouldBe ("foo", 30, "sales")

    peopleDepAge.filter($"age" > 30)
      .join(departments, peopleDepAge("department_id") === departments("department_id"))
      .agg(avg($"age"), max($"age")).limit(1)
      .as[(Double, Int)].head() shouldBe (45.0, 50)
  }
}
