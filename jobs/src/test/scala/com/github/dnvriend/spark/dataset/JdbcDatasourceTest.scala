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
import com.github.dnvriend.TestSpec.Order

class JdbcDatasourceTest extends TestSpec {

  val jdbcOptions = Map(
    "url" -> "jdbc:h2:mem:test;INIT=runscript from 'src/test/resources/create.sql'\\;runscript from 'src/test/resources/init.sql'",
    "dbtable" -> "customer",
    "driver" -> "org.h2.Driver",
    "user" -> "root",
    "password" -> "root"
  )

  it should "join JDBC and parquet" in withSpark { spark =>
    import spark.implicits._
    val orders = spark.read.parquet(TestSpec.OrdersParquet).as[Order].cache()
    val customers = spark.read.format("jdbc").options(jdbcOptions).load().cache()

    customers.count() shouldBe 7

    orders
      .join(customers, orders("customer_id") === customers("customer_id"))
      .select(orders("order_id"), $"customer_name", $"customer_age")
      .as[(Int, String, Int)].collect() shouldBe Seq(
        (10308, "Ollie Olson", 34),
        (10309, "Craig Hahn", 21)
      )
  }
}
