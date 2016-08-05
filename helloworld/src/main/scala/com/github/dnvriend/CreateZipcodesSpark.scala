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

package com.github.dnvriend

import akka.actor.ActorSystem
import akka.event.{ Logging, LoggingAdapter }
import akka.stream.{ ActorMaterializer, Materializer }
import org.apache.spark.sql.SparkSession
import spray.json.DefaultJsonProtocol

import scala.compat.Platform
import scala.concurrent.ExecutionContext

object CreateZipcodesSpark extends App with DefaultJsonProtocol {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val log: LoggingAdapter = Logging(system, this.getClass)

  val spark = SparkSession.builder()
    .config("spark.sql.warehouse.dir", "file:/tmp/spark-warehouse")
    .config("spark.cores.max", "4")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.sql.crossJoin.enabled", "true")
    .master("local[*]") // gebruik zoveel threads als cores
    .appName("CreateZipcodesSpark").getOrCreate()

  val start = Platform.currentTime

  import spark.implicits._

  // define an RDD for the district range
  val districts = spark.sparkContext.makeRDD(1000 to 9000).map(_.toString).toDS
  // create temp view
  districts.createOrReplaceTempView("districts")

  // define an RDD with a range for the letters
  val l1 = spark.sparkContext.makeRDD('A' to 'Z').map(_.toString).toDS
  l1.createOrReplaceTempView("l1")

  // join the letters
  val letters = spark.sql("SELECT concat(a.value, b.value) letters from l1 a join l1 b")
  // define temp view
  letters.createOrReplaceTempView("letters")

  // define an RDD for the houses
  val houses = spark.sparkContext.makeRDD(1 to 399).toDS
  // create temp view
  houses.createOrReplaceTempView("houses")

  // join letters and houses
  val lettersWithHouseNr = spark.sql(
    """
      |SELECT CONCAT(letters, '-', nr) letterswithhousenr FROM letters
      |JOIN
      |(SELECT format_string("%03d", value) nr FROM houses)
    """.stripMargin
  )
  // create temp view
  lettersWithHouseNr.createOrReplaceTempView("lwh")

  // join the districts with the house numbers
  val tickets = spark.sql("SELECT concat(value, letterswithhousenr) value FROM districts JOIN lwh LIMIT 5000000")
  println("==> Writing to parquet")
  tickets.write.parquet("/tmp/tickets_spark.parquet")
  println("==> Done, took: " + (Platform.currentTime - start) + " millis")
  spark.stop()
}
