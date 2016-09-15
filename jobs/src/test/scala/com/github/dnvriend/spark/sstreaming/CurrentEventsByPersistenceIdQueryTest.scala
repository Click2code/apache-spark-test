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

import akka.actor.{ ActorRef, Props }
import akka.testkit.TestProbe
import com.github.dnvriend.TestSpec
import com.github.dnvriend.spark.datasources.SparkImplicits._
import com.github.dnvriend.spark.mapper.PersonEventMapper
import org.apache.spark.sql.streaming.{ OutputMode, ProcessingTime }
import org.apache.spark.sql.functions._
import org.scalatest.Ignore

import scala.concurrent.duration._

@Ignore
class CurrentEventsByPersistenceIdQueryTest extends TestSpec {
  def withPersistentActor(pid: String = randomId, schedule: Boolean = false)(f: ActorRef => TestProbe => Unit): Unit = {
    val tp = TestProbe()
    val ref = system.actorOf(Props(new PersonActor(pid, schedule)))
    try f(ref)(tp) finally killActors(ref)
  }

  it should "read events for pid" in withSparkSession { spark =>
    import spark.implicits._
    withPersistentActor("person", schedule = true) { ref => tp =>

      tp.send(ref, "persist")
      tp.expectMsg("ack")

      val jdbcReadJournal = spark.readStream
        .schema(PersonEventMapper.schema)
        .option("pid", "person")
        .option("event-mapper", "com.github.dnvriend.spark.mapper.PersonEventMapper")
        .eventsByPersistenceId("jdbc-read-journal")

      jdbcReadJournal.printSchema()

      //      val numOfEvents = jdbcReadJournal
      //        .groupBy('persistence_id)
      //        .agg(count('sequence_number).as("number_of_events"))

      val query = jdbcReadJournal
        .writeStream
        .format("console")
        .trigger(ProcessingTime(1.seconds))
        .queryName("consoleStream")
        //        .outputMode(OutputMode.Complete())
        .outputMode(OutputMode.Append())
        .start()

      query.awaitTermination(20.seconds)
    }
  }
}
