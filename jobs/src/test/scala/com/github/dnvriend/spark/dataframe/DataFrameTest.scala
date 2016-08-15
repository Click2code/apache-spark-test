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

package com.github.dnvriend.spark.dataframe

import akka.stream.scaladsl.Source
import com.github.dnvriend.TestSpec
import com.github.dnvriend.TestSpec.Person
import com.github.dnvriend.spark.datasources.SparkImplicits._

import scala.collection.immutable._
import scala.concurrent.Future

class DataFrameTest extends TestSpec {
  it should "create a DataFrame from an akka.stream.scaladsl.Source" in withSparkSession { spark =>
    val df = spark.fromSource(Source(List(Person(1, "foo", 25), Person(2, "bar", 20), Person(3, "baz", 30))))
    df.show()
    df.printSchema()
  }

  it should "create a DataFrame from a Future" in withSparkSession { spark =>
    val df = spark.fromFuture(Future.successful(List(Person(1, "foo", 25), Person(2, "bar", 20), Person(3, "baz", 30))))
    df.show()
    df.printSchema()
  }
}
