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

import akka.Done
import akka.stream.scaladsl.Tcp._
import akka.stream.scaladsl.{Flow, Sink, Source, Tcp}
import akka.util.ByteString
import com.github.dnvriend.TestSpec
import org.scalatest.Ignore

import scala.collection.immutable._
import scala.concurrent.Future
import scala.concurrent.duration._

@Ignore
class StructuredStreamingWordCountTest extends TestSpec {
  def withSocketServer(xs: Seq[String])(f: Future[Done] => Unit): Unit = {
    val connections: Source[IncomingConnection, Future[ServerBinding]] = Tcp().bind("127.0.0.1", 9999)
    val socketServer = connections.runForeach { connection =>
      println(s"New connection from: ${connection.remoteAddress}")
      val src = Source.cycle(() => xs.iterator).map(txt => ByteString(txt) ++ ByteString("\n"))
        .flatMapConcat(msg => Source.tick(0.seconds, 200.millis, msg))
      val echo = Flow.fromSinkAndSource(Sink.ignore, src)
      connection.handleWith(echo)
    }
    f(socketServer)
  }

  it should "a running word count of text data received via a TCP server" in withSparkSession { spark =>
    withSocketServer(List("apache spark")) { socketServer =>
      import spark.implicits._

      val lines = spark.readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()

      // Split the lines into words
      val words = lines.as[String].flatMap(_.split(" "))

      // Generate running word count
      val wordCounts = words.groupBy("value").count()

      // Start running the query that prints the running counts to the console
      val query = wordCounts.writeStream
        .outputMode("complete")
        .format("console")
        .start()

      query.awaitTermination(10.seconds)
    }
  }
}
