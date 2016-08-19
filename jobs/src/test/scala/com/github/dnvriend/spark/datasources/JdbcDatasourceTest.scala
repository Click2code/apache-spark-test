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
import com.github.dnvriend.spark._
import com.github.dnvriend.spark.datasources.SparkImplicits._
import org.apache.spark.sql.DataFrame

object JdbcDatasourceTest {
  implicit val H2Options: Map[String, String] = Map(
    "url" -> "jdbc:h2:mem:test;INIT=runscript from 'src/test/resources/create.sql'\\;runscript from 'src/test/resources/init.sql'",
    "dbtable" -> "customer",
    "driver" -> "org.h2.Driver",
    "user" -> "root",
    "password" -> "root"
  )
  implicit val PostgresOptions: Map[String, String] = Map(
    "url" -> "jdbc:postgresql://localhost:5432/docker?reWriteBatchedInserts=true",
    "driver" -> "org.postgresql.Driver",
    "user" -> "postgres",
    "password" -> ""
  )
}

class JdbcDatasourceTest extends TestSpec {

  it should "join JDBC and parquet" in withSparkSession { spark =>
    import spark.implicits._
    implicit val jdbcOptions = JdbcDatasourceTest.PostgresOptions
    val orders = spark.read.parquet(TestSpec.OrdersParquet).as[Order].cache()
    val customers = spark.read.jdbc("customer").cache()
    customers.count() shouldBe 7

    val orderCustomer = orders
      .join(customers, orders("customer_id") === customers("customer_id"))
      .select(orders("order_id"), 'customer_name, 'customer_age)

    orderCustomer.as[(Int, String, Int)].collect() shouldBe Seq(
      (10308, "Ollie Olson", 34),
      (10309, "Craig Hahn", 21)
    )

    orderCustomer.write.append.jdbc("order_customer")
    val order_cust: DataFrame = spark.read.jdbc("order_customer")
    order_cust.printSchema()
    order_cust.show()
  }

  // http://stackoverflow.com/questions/2901453/sql-standard-to-escape-column-names
  //
  // The SQL-99 standard specifies that double quote (") is used to delimit identifiers.
  //
  //Oracle, PostgreSQL, MySQL, MSSQL and SQlite all support " as the identifier delimiter
  // (though they don't all use " as the 'default' -
  //
  // for example, you have to be running MySQL in ANSI mode and SQL Server only supports it when QUOTED_IDENTIFIER is ON.)
}
