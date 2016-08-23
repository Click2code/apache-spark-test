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

package com.github.dnvriend.spark.sstreaming

import com.github.dnvriend.TestSpec
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.streaming.{ OutputMode, ProcessingTime }
import org.apache.spark.sql.types._
import org.scalatest.Ignore

import scala.concurrent.duration._
import scala.language.implicitConversions

@Ignore
class QueryCsvTest extends TestSpec {
  def copyFiles(nrTimes: Int = 10): Unit = {
    implicit def toFile(str: String): java.io.File = new java.io.File(str)
    FileUtils.deleteDirectory("/tmp/csv")
    FileUtils.forceMkdir("/tmp/csv")
    (1 to nrTimes).foreach { x =>
      FileUtils.copyFile(TestSpec.PeopleCsv, s"/tmp/csv/people-$x")
    }
  }

  val schema: StructType = StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("age", IntegerType, nullable = true)
  ))

  it should "query csv file" in withSparkSession { spark =>
    copyFiles()

    val csv = spark.readStream
      .schema(schema)
      .format("csv")
      .option("maxFilesPerTrigger", 1)
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .option("delimiter", ";")
      .load("/tmp/csv")

    csv.printSchema()

    println("Is the query streaming: " + csv.isStreaming)
    println("Are there any streaming queries? " + spark.streams.active.isEmpty)

    val query = csv
      .writeStream
      .format("console")
      .trigger(ProcessingTime(5.seconds))
      .queryName("consoleStream")
      .outputMode(OutputMode.Append())
      .start()


    // waiting for data
    sleep(3.seconds)
    spark.streams
      .active
      .foreach(println)

    spark.streams
      .active
      .foreach(_.explain(extended = true))

    query.awaitTermination(20.seconds)
  }
}
