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

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.actor.{ ActorRef, Props }
import akka.persistence.PersistentActor
import akka.testkit.TestProbe
import com.github.dnvriend.TestSpec
import com.github.dnvriend.spark.datasources.SparkImplicits._
import com.github.dnvriend.spark.datasources.person.Person
import org.apache.spark.sql.streaming.{ OutputMode, ProcessingTime }
import org.scalatest.Ignore

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.implicitConversions

object PersonActor {
  final case class BlogPost(id: Long, text: String)
}
class PersonActor(val persistenceId: String, schedule: Boolean)(implicit ec: ExecutionContext) extends PersistentActor {
  val counter = new AtomicLong()
  def ping() = context.system.scheduler.scheduleOnce(200.millis, self, "persist")
  def randomId: String = UUID.randomUUID.toString
  override val receiveRecover: Receive = PartialFunction.empty
  override val receiveCommand: Receive = {
    case "persist" =>
      persist(Person(counter.incrementAndGet(), s"foo-$randomId", 20)) { _ =>
        sender() ! "ack"
      }
      if (schedule) ping()
  }
  if (schedule) ping()
}

@Ignore
class CurrentPersistenceIdsQuerySourceTest extends TestSpec {
  def withPersistentActor(pid: String = randomId, schedule: Boolean = false)(f: ActorRef => TestProbe => Unit): Unit = {
    val tp = TestProbe()
    val ref = system.actorOf(Props(new PersonActor(pid, schedule)))
    try f(ref)(tp) finally killActors(ref)
  }

  it should "query read journal" in withSparkSession { spark =>
    withPersistentActor() { ref => tp =>
      tp.send(ref, "persist")
      tp.expectMsg("ack")

      val jdbcReadJournal = spark.readStream
        .currentPersistenceIds("jdbc-read-journal")

      jdbcReadJournal.printSchema()

      println("Is the query streaming: " + jdbcReadJournal.isStreaming)
      println("Are there any streaming queries? " + spark.streams.active.isEmpty)

      val query = jdbcReadJournal
        .writeStream
        .format("console")
        .trigger(ProcessingTime(1.seconds))
        .queryName("consoleStream")
        .outputMode(OutputMode.Append())
        .start()

      query.awaitTermination(10.seconds)
    }
  }
}
