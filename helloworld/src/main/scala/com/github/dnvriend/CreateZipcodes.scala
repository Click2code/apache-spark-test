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

import akka.actor.ActorSystem
import akka.stream.scaladsl.{FileIO, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.ByteString
import spray.json.{DefaultJsonProtocol, _}

import scala.concurrent.ExecutionContext

object CreateZipcodes extends App with DefaultJsonProtocol {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  final case class Zipcode(value: String)

  implicit val zipcodeJsonFormat = jsonFormat1(Zipcode)

  val f = Source(1000 to 9999).flatMapConcat { district =>
    Source('A' to 'Z').flatMapConcat { l1 =>
      Source('A' to 'Z').flatMapConcat { l2 =>
        Source(1 to 399).flatMapConcat { num =>
          Source.single(f"$district$l1$l2-$num%03d")
        }
      }
    }
  }.map(Zipcode)
    .map(_.toJson.compactPrint)
    .map(json => ByteString(json + "\n"))
    .take(5000000)
    .runWith(FileIO.toPath(Paths.get("/tmp/zips.json"), Set(WRITE, TRUNCATE_EXISTING, CREATE)))
    .flatMap { done =>
      println(done)
      system.terminate
    }

  sys.addShutdownHook {
    system.terminate()
  }
}
