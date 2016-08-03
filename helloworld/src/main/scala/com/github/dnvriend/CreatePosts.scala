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
import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.{ActorSystem, Terminated}
import akka.stream.scaladsl.{FileIO, Source}
import akka.stream.{ActorMaterializer, Materializer}
import akka.util.ByteString
import spray.json.{DefaultJsonProtocol, _}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

object CreatePosts extends App with DefaultJsonProtocol {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  def terminate: Future[Terminated] =
    system.terminate()

  sys.addShutdownHook {
    terminate
  }

  implicit val postJsonFormat = jsonFormat13(Post)

  final case class Post(
    commentCount: Int,
    lastActivityDate: String,
    ownerUserId: Long,
    body: String,
    score: Int,
    creationDate: String,
    viewCount: Int,
    title: String,
    tags: String,
    answerCount: Int,
    acceptedAnswerId: Long,
    postTypeId: Long,
    id: Long
  )

  def rng = Random.nextInt(20000)

  def now: String = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").format(new Date())

  val lorem = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nam fringilla magna et pharetra vestibulum."
  val title = " Ut id placerat sapien. Aliquam vel metus orci."
  Source.fromIterator(() => Iterator from 0).map { id =>
    Post(rng, now, rng, List.fill(Random.nextInt(5))(lorem).mkString("\n"), rng, now, rng, s"$rng - $title", title, rng, rng, rng, id)
  }.map(_.toJson.compactPrint)
    .map(json => ByteString(json + "\n"))
    .take(1000000)
    .via(LogProgress.flow())
    .runWith(FileIO.toPath(Paths.get("/tmp/posts.json"), Set(WRITE, TRUNCATE_EXISTING, CREATE)))
    .flatMap { done =>
      println(done)
      terminate
    }.recoverWith {
      case cause: Throwable =>
        cause.printStackTrace()
        terminate
    }

}
