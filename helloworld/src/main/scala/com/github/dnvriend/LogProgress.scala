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

import akka.NotUsed
import akka.event.LoggingAdapter
import akka.stream.FlowShape
import akka.stream.scaladsl.{ Broadcast, Flow, GraphDSL, Sink }

import scala.compat.Platform
import scala.collection.immutable._

object LogProgress {
  def flow[A](each: Long = 1000)(implicit log: LoggingAdapter = null): Flow[A, A, NotUsed] = Flow.fromGraph[A, A, NotUsed](GraphDSL.create() { implicit b =>
    import GraphDSL.Implicits._
    val logFlow = Flow[A].statefulMapConcat { () =>
      var last = Platform.currentTime
      var num = 0L
      (x: A) =>
        num += 1
        if (num % each == 0) {
          val duration = Platform.currentTime - last
          val logOpt = Option(log)
          Option(log).foreach(_.info("[{} ms / {}]: {}", duration, each, num))
          if (logOpt.isEmpty) println(s"[$duration ms / $each]: $num")
          last = Platform.currentTime
        }
        Iterable(x)
    }
    val bcast = b.add(Broadcast[A](2, eagerCancel = false))
    bcast ~> logFlow ~> Sink.ignore
    FlowShape.of(bcast.in, bcast.out(1))
  })
}
