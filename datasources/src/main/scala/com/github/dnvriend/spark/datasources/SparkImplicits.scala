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

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{ Sink, Source }
import org.apache.spark.sql.{ DataFrame, DataFrameReader, SparkSession }

import scala.collection.immutable._
import scala.concurrent.duration.{ FiniteDuration, _ }
import scala.concurrent.{ Await, Future }
import scala.reflect.runtime.universe._

object SparkImplicits {
  implicit class DataSourceOps(dfr: DataFrameReader) {
    def helloworld(path: String): DataFrame = dfr.format("helloworld").load(path)
    def person(path: String): DataFrame = dfr.format("person").load(path)
    def databricksCsv(path: String): DataFrame = dfr.format("com.databricks.spark.csv").load(path)
  }

  implicit class SparkSessionOps(spark: SparkSession) {
    def fromFuture[A <: Product: TypeTag](data: Future[Seq[A]])(implicit _timeout: FiniteDuration = null): DataFrame =
      spark.createDataFrame(Await.result(data, Option(_timeout).getOrElse(15.minutes)))

    def fromSource[A <: Product: TypeTag](data: Source[A, NotUsed])(implicit _timeout: FiniteDuration = null, mat: Materializer): DataFrame =
      fromFuture(data.runWith(Sink.seq))
  }
}
