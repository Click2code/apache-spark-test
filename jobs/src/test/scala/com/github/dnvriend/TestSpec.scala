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
import akka.util.Timeout
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

abstract class TestSpec extends FlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val pc: PatienceConfig = PatienceConfig(timeout = 5.minutes)
  implicit val timeout = Timeout(5.minutes)

  private val _spark = SparkSession.builder()
    .config("spark.sql.warehouse.dir", "file:/tmp/spark-warehouse")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.ui.enabled", "false")
    .master("local")
    .appName("CreateZipcodesSpark").getOrCreate()

  def sc: SparkContext = _spark.newSession().sparkContext
  def spark: SparkSession = _spark.newSession()

  override protected def afterAll(): Unit = {
    _spark.stop()
    system.terminate()
  }
}
