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

class UDFTest extends TestSpec {
  it should "uppercase using a user defined function or UDF" in withSpark { spark =>
    import spark.implicits._
    // lets create a DataFrame
    val df = Seq((0, "hello"), (1, "world")).toDF("id", "text")

    df.as[(Int, String)].collect() shouldBe Seq(
      (0, "hello"),
      (1, "world")
    )

    // define a plain old Scala Function
    val upper: String => String = _.toUpperCase + "- foo"

    // create a User Defined Function
    import org.apache.spark.sql.functions.udf
    val upperUDF = udf(upper)

    // apply the user defined function
    df.withColumn("upper", upperUDF('text))
      .as[(Int, String, String)].collect shouldBe Seq(
        (0, "hello", "HELLO- foo"),
        (1, "world", "WORLD- foo")
      )

    // the UDF can be used in a query
    // first register a temp view so that
    // we can reference the DataFrame
    df.createOrReplaceTempView("df")

    // register the UDF by name 'upperUDF'
    spark.udf.register("upperUDF", upper)

    // use the UDF in a SQL-Query
    spark.sql("SELECT *, upperUDF(text) FROM df")
      .as[(Int, String, String)].collect shouldBe Seq(
        (0, "hello", "HELLO- foo"),
        (1, "world", "WORLD- foo")
      )
  }
}
