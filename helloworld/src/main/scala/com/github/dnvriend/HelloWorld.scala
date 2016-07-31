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
import org.apache.spark.{ SparkConf, SparkContext }

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

  // The first thing a Spark program must do is to create a SparkContext object, which tells Spark how to access a cluster.
  // To create a SparkContext you first need to build a SparkConf object that contains information about your application.
  // Only one SparkContext may be active per JVM. You must stop() the active SparkContext before creating a new one.
  val conf = new SparkConf()
    .setAppName("Hello World") // The appName parameter is a name for your application to show on the cluster UI.
    .setMaster(s"spark://localhost:7077")
  val sc = new SparkContext(conf)

  for {
    pi <- Future(CalculatePi(sc))
    _ <- system.terminate()
  } yield {
    println(s"Hello World, Pi = $pi")
    sc.stop()
  }

  sys.addShutdownHook {
    sc.stop()
    system.terminate()
  }
}