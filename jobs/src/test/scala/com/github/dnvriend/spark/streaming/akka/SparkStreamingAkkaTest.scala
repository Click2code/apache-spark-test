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

package com.github.dnvriend.spark.streaming.akka

import java.text.SimpleDateFormat

import akka.actor.Props
import com.github.dnvriend.TestSpec
import org.apache.spark.streaming.akka.{ ActorReceiver, AkkaUtils }

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class CustomActor extends ActorReceiver {
  import context.dispatcher
  def ping() = context.system.scheduler.scheduleOnce(200.millis, self, "foo")
  def today: String = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(new java.util.Date)
  def receive = counter(0)
  def counter(x: Long): Receive = {
    case _ =>
      store(s"counter: $x, msg: Today is $today, have a nice day!")
      context.become(counter(x + 1))
      ping()
  }
  ping()
}

class SparkStreamingAkkaTest extends TestSpec {
  it should "stream from an actor" in withStreamingContext() { spark => ssc =>
    import spark.implicits._
    val lines = AkkaUtils.createStream[String](ssc, Props[CustomActor](), "CustomReceiver")
    lines.foreachRDD { rdd =>
      rdd.toDF.show(truncate = false)
    }

    ssc.start()
    advanceClockOneBatch(ssc)
    sleep()
    advanceClockOneBatch(ssc)
    sleep()
    advanceClockOneBatch(ssc)
    sleep()
    advanceClockOneBatch(ssc)
    sleep()
    advanceClockOneBatch(ssc)
    sleep()
  }
}
