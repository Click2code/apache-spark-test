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
import akka.http.scaladsl._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.{ Directives, Route }
import akka.stream.{ ActorMaterializer, Materializer }
import com.github.dnvriend.spark.CalculatePi
import org.apache.spark.{ SparkConf, SparkContext }
import spray.json.DefaultJsonProtocol

import scala.concurrent.{ ExecutionContext, Future }

object RestPi extends App with Directives with SprayJsonSupport with DefaultJsonProtocol {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val log: LoggingAdapter = Logging(system, this.getClass)

  val conf = new SparkConf()
    .setAppName("Hello World")
    .setMaster(s"spark://localhost:7077")
  val sc = new SparkContext(conf)

  final case class Pi(pi: Long)

  implicit val piJsonFormat = jsonFormat1(Pi)

  def calculatePi(num: Long = 1000000, slices: Int = 10): Future[Long] = Future(CalculatePi(sc, num, slices))

  val route: Route =
    pathEndOrSingleSlash {
      complete(calculatePi().map(Pi))
    } ~ path("pi" / LongNumber / IntNumber) { (num, slices) =>
      complete(calculatePi(num, slices).map(Pi))
    }

  Http().bindAndHandle(route, "0.0.0.0", 8008)

  sys.addShutdownHook {
    sc.stop()
    system.terminate()
  }
}
