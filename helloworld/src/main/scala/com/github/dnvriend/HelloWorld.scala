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
import com.github.dnvriend.spark.CalculatePi
import org.apache.spark.sql.SparkSession

import scala.concurrent.{ ExecutionContext, Future }

/**
 * Package this application and copy to $SPARK_HOME/jars dir.
 * Also read: http://spark.apache.org/docs/latest/configuration.html
 */
object HelloWorld extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val log: LoggingAdapter = Logging(system, this.getClass)

  val n = 10000000

  // The first thing a Spark program must do is to create a SparkSession object,
  // which tells Spark how to access a cluster, or to run in local mode
  val spark = SparkSession.builder()
    .config("spark.sql.warehouse.dir", "file:/tmp/spark-warehouse")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.sql.crossJoin.enabled", "true")
    .master("local[*]") // use as many threads as cores
    .appName("Hello World") // The appName parameter is a name for your application to show on the cluster UI.
    .getOrCreate()

  for {
    count <- Future(CalculatePi(spark.sparkContext, n))
    _ <- system.terminate()
  } yield {
    val pi = 2.0 * count / (n - 1)
    println(s"Hello World, Pi = $pi")
    spark.stop()
  }

  sys.addShutdownHook {
    spark.stop()
    system.terminate()
  }
}