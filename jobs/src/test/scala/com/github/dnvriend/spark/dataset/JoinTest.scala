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

import java.text.SimpleDateFormat

import com.github.dnvriend.TestSpec
import com.github.dnvriend.spark._

class JoinTest extends TestSpec {

  def dt(str: String) = new java.sql.Date(new SimpleDateFormat("yyyy-MM-dd").parse(str).getTime)

  //
  // A 'JOIN' clause is used to combine columns from two or more tables,
  // based on a common field between them. In effect the resulting table
  // will become as wide as the tables combined. The resulting new table
  // will contain only the rows that batch based on a common field between
  // them
  //

  it should "inner join - returns all rows when there is at least one match in BOTH tables" in withSparkSession { spark =>
    import spark.implicits._
    val orders = spark.read.parquet(TestSpec.OrdersParquet).as[Order].cache()
    val customers = spark.read.parquet(TestSpec.CustomersParquet).as[Customer].cache()

    // note: the default `join`, without a type is the `inner` join

    // The INNER JOIN keyword selects all rows from both tables
    // as long as there is a match between the columns in both tables.

    orders
      .join(customers, orders("customer_id") === customers("customer_id"))
      .select(orders("order_id"), $"customer_name", $"country")
      .as[(Int, String, String)].collect() shouldBe Seq(
        (10308, "Ana Trujillo Emparedados y helados", "Mexico"),
        (10309, "Antonio Moreno Taquería", "Mexico")
      )
  }

  it should "left join - return all rows from the left table, and the matched rows from the right table" in withSparkSession { spark =>
    import spark.implicits._
    val orders = spark.read.parquet(TestSpec.OrdersParquet).as[Order].cache()
    val customers = spark.read.parquet(TestSpec.CustomersParquet).as[Customer].cache()

    // note: joinType 'left' is the same as 'leftouter'

    // The LEFT JOIN keyword returns all rows from the left table (orders),
    // with the matching rows in the right table (customers).

    // The result is NULL in the right side when there is no match.

    orders
      .join(customers, orders("customer_id") === customers("customer_id"), "left")
      .select(orders("order_id"), $"customer_name", $"country")
      .as[(Int, Option[String], Option[String])].collect() shouldBe Seq(
        (10308, Some("Ana Trujillo Emparedados y helados"), Some("Mexico")),
        (10309, Some("Antonio Moreno Taquería"), Some("Mexico")),
        (10310, None, None)
      )
  }

  it should "right join - return all rows from the right table, and the matched rows from the left table" in withSparkSession { spark =>
    import spark.implicits._
    val orders = spark.read.parquet(TestSpec.OrdersParquet).as[Order].cache()
    val customers = spark.read.parquet(TestSpec.CustomersParquet).as[Customer].cache()

    // note: joinType 'right' is the same as 'rightouter'

    // The RIGHT JOIN keyword returns all rows from the right table (customers),
    // with the matching rows in the left table (orders).

    // The result is NULL in the left side when there is no match.

    orders
      .join(customers, orders("customer_id") === customers("customer_id"), "right")
      .select(orders("order_id"), $"customer_name", $"country")
      .as[(Option[Int], String, String)].collect() shouldBe Seq(
        (None, "Alfreds Futterkiste", "Germany"),
        (Some(10308), "Ana Trujillo Emparedados y helados", "Mexico"),
        (Some(10309), "Antonio Moreno Taquería", "Mexico")
      )
  }

  it should "full join - return all rows when there is a match in ONE of the tables" in withSparkSession { spark =>
    import spark.implicits._
    val orders = spark.read.parquet(TestSpec.OrdersParquet).as[Order].cache()
    val customers = spark.read.parquet(TestSpec.CustomersParquet).as[Customer].cache()

    // note: 'full' join is the same as 'outer' or 'fullouter'

    // The FULL OUTER JOIN keyword returns all rows from the left table (orders) and from the right table (customers).
    // The FULL OUTER JOIN keyword combines the result of both LEFT and RIGHT joins.

    orders
      .join(customers, orders("customer_id") === customers("customer_id"), "full")
      .select(orders("order_id"), $"customer_name", $"country")
      .as[(Option[Int], Option[String], Option[String])].collect() shouldBe Seq(
        (None, Some("Alfreds Futterkiste"), Some("Germany")),
        (Some(10308), Some("Ana Trujillo Emparedados y helados"), Some("Mexico")),
        (Some(10309), Some("Antonio Moreno Taquería"), Some("Mexico")),
        (Some(10310), None, None)
      )
  }

  it should "left semi join - returns only rows from the left side for which a match on the right side exists" in withSparkSession { spark =>
    import spark.implicits._
    val orders = spark.read.parquet(TestSpec.OrdersParquet).as[Order].cache()
    val customers = spark.read.parquet(TestSpec.CustomersParquet).as[Customer].cache()

    // note: whereas a join returns the columns of both tables, the left-semi only returns
    // the left table, here the orders table, but only where there is a match and
    // each row is returned at most once.

    orders
      .join(customers, orders("customer_id") === customers("customer_id"), "leftsemi")
      .as[Order]
      .collect shouldBe Seq(
        Order(10308, 2, dt("1996-09-18")),
        Order(10309, 3, dt("1996-09-19"))
      )
  }

  it should "left anti semi join - returns only rows from the left side for which no match on the right side exists" in withSparkSession { spark =>
    import spark.implicits._
    val orders = spark.read.parquet(TestSpec.OrdersParquet).as[Order].cache()
    val customers = spark.read.parquet(TestSpec.CustomersParquet).as[Customer].cache()

    // note: whereas the left semi join returns only rows from the left side, for which a match on the right side exists,
    // the left-anti semi join returns only rows from the left side, for which *no* match on the right side exists.

    orders
      .join(customers, orders("customer_id") === customers("customer_id"), "leftanti")
      .as[Order]
      .collect shouldBe Seq(
        Order(10310, 77, dt("1996-09-20"))
      )
  }
}
