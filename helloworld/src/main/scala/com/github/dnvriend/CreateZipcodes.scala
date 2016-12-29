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

import java.nio.file.Paths
import java.nio.file.StandardOpenOption._

import akka.NotUsed
import akka.actor.{ ActorSystem, Terminated }
import akka.stream.scaladsl.{ FileIO, Source }
import akka.stream.{ ActorMaterializer, Materializer }
import akka.util.ByteString
import play.api.libs.json.Json

import scala.concurrent.{ ExecutionContext, Future }

object CreateZipcodes extends App {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  sys.addShutdownHook {
    terminate
  }

  object Zipcode {
    implicit val format = Json.format[Zipcode]
  }
  final case class Zipcode(value: String)

  val numZips = 50000000

  def zips(range: Range): Source[ByteString, NotUsed] =
    Source(range).flatMapConcat { district =>
      Source('A' to 'Z').flatMapConcat { l1 =>
        Source('A' to 'Z').flatMapConcat { l2 =>
          Source(1 to 399).map(num => f"$district$l1$l2-$num%03d")
        }
      }
    }.map(Zipcode.apply).map(Json.toJson(_).toString).map(json => ByteString(json + "\n"))

  zips(1000 until 2000)
    .merge(zips(2000 until 3000))
    .merge(zips(3000 until 4000))
    .merge(zips(4000 until 5000))
    .merge(zips(5000 until 6000))
    .merge(zips(6000 until 7000))
    .merge(zips(7000 until 8000))
    .merge(zips(8000 until 9000))
    .take(numZips)
    .via(LogProgress.flow(each = 250000))
    .runWith(FileIO.toPath(Paths.get("/tmp/zips.json"), Set(WRITE, TRUNCATE_EXISTING, CREATE)))
    .flatMap { done =>
      println(done)
      terminate
    }.recoverWith {
      case cause: Throwable =>
        cause.printStackTrace()
        terminate
    }

  def terminate: Future[Terminated] =
    system.terminate()
}
