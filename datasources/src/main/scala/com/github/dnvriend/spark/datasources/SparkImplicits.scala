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

import java.util.Properties

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{ Sink, Source }
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.DataStreamReader

import scala.collection.immutable._
import scala.concurrent.duration.{ FiniteDuration, _ }
import scala.concurrent.{ Await, Future }
import scala.reflect.runtime.universe._
import slick.driver.PostgresDriver.api._

object SparkImplicits {
  implicit class DataSourceOps(dfr: DataFrameReader) {
    def helloworld(path: String): DataFrame = dfr.format("helloworld").load(path)
    def person(path: String): DataFrame = dfr.format("person").load(path)
    def jdbc(table: String)(implicit jdbcOptions: Map[String, String]): DataFrame =
      dfr.format("jdbc").options(jdbcOptions ++ Map("dbtable" -> table)).load()
  }

  implicit class DataStreamReaderOps(dsr: DataStreamReader) {
    def currentPersistenceIds(path: String = "jdbc-read-journal"): DataFrame = dsr.format("current-persistence-id").load(path)
    def eventsByPersistenceId(path: String = "jdbc-read-journal"): DataFrame = dsr.format("current-events-by-persistence-id").load(path)
  }

  implicit class DataFrameWriterOps[T](dfw: DataFrameWriter[T]) {
    /**
     * Append mode means that when saving a DataFrame to a data source, if data/table already exists,
     * contents of the DataFrame are expected to be appended to existing data.
     */
    def append = dfw.mode(SaveMode.Append)

    /**
     * Overwrite mode means that when saving a DataFrame to a data source,
     * if data/table already exists, existing data is expected to be overwritten by the contents of
     * the DataFrame.
     */
    def overwrite = dfw.mode(SaveMode.Overwrite)

    /**
     * ErrorIfExists mode means that when saving a DataFrame to a data source, if data already exists,
     * an exception is expected to be thrown.
     */
    def errorIfExists = dfw.mode(SaveMode.ErrorIfExists)

    /**
     * Ignore mode means that when saving a DataFrame to a data source, if data already exists,
     * the save operation is expected to not save the contents of the DataFrame and to not
     * change the existing data.
     */
    def ignore = dfw.mode(SaveMode.Ignore)

    def jdbc(table: String)(implicit jdbcOptions: Map[String, String]) = {
      val properties = jdbcOptions.foldLeft(new Properties) { case (prop, (k, v)) => prop.put(k, v); prop }
      dfw.jdbc(jdbcOptions("url"), table, properties)
      // does not (yet) work see: https://issues.apache.org/jira/browse/SPARK-7646
      // dfw.format("jdbc").mode(SaveMode.Overwrite).options(jdbcOptions ++ Map("dbtable" -> table))
    }
  }

  trait DataFrameQueryGenerator[A] {
    def upsert: String
  }

  implicit class DatasetOps(df: DataFrame) {
    def withSession[A](db: Database)(f: Session => A): A = {
      val session = db.createSession()
      try f(session) finally session.close()
    }

    def withStatement[A](db: Database)(f: java.sql.Statement => A): A =
      withSession(db)(session ⇒ session.withStatement()(f))

    def upsert[A](table: String)(implicit db: Database, dfq: DataFrameQueryGenerator[A]): DataFrame = withStatement(db) { stmt =>
      stmt.executeUpdate(dfq.upsert)
      df
    }
  }

  implicit class SparkSessionOps(spark: SparkSession) {
    def fromFuture[A <: Product: TypeTag](data: Future[Seq[A]])(implicit _timeout: FiniteDuration = null): DataFrame =
      spark.createDataFrame(Await.result(data, Option(_timeout).getOrElse(15.minutes)))

    def fromSource[A <: Product: TypeTag](data: Source[A, NotUsed])(implicit _timeout: FiniteDuration = null, mat: Materializer): DataFrame =
      fromFuture(data.runWith(Sink.seq))
  }
}
