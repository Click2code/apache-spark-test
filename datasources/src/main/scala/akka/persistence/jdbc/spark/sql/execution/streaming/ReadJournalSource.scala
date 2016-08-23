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

package akka.persistence.jdbc.spark.sql.execution.streaming

import akka.actor.{ ActorSystem, ExtendedActorSystem }
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.{ CurrentEventsByPersistenceIdQuery, CurrentEventsByTagQuery, CurrentPersistenceIdsQuery, ReadJournal }
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.extension.{ Sink => Snk }
import akka.stream.{ ActorMaterializer, Materializer }
import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.{ LongOffset, Offset, Source }
import org.apache.spark.sql.types.StructType

import scala.collection.immutable._
import scala.concurrent.duration.{ FiniteDuration, _ }
import scala.concurrent.{ Await, ExecutionContext, Future }

trait ReadJournalSource {
  _: Source =>
  def readJournalPluginId: String
  def sqlContext: SQLContext

  // some machinery
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  // read journal, only interested in the Current queries, as Spark isn't asynchronous
  lazy val readJournal = PersistenceQuery(system).readJournalFor(readJournalPluginId)
    .asInstanceOf[ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery with CurrentEventsByTagQuery]

  implicit class FutureOps[A](f: Future[A])(implicit ec: ExecutionContext, timeout: FiniteDuration = null) {
    def futureValue: A = Await.result(f, Option(timeout).getOrElse(10.seconds))
  }

  def maxPersistenceIds: Long =
    readJournal.currentPersistenceIds().runWith(Snk.count).futureValue

  def persistenceIds(start: Long, end: Long) =
    readJournal.currentPersistenceIds().drop(start).take(end).runWith(Sink.seq).futureValue

  def maxEventsByPersistenceId(pid: String): Long =
    readJournal.currentEventsByPersistenceId(pid, 0, Long.MaxValue).runWith(Snk.count).futureValue

  def eventsByPersistenceId(pid: String, start: Long, end: Long, eventMapperFQCN: String): Seq[Row] = {
    readJournal.currentEventsByPersistenceId(pid, start, end)
      .map(env => getMapper(eventMapperFQCN).get.row(env, sqlContext)).runWith(Sink.seq).futureValue
  }

  implicit def mapToDataFrame(rows: Seq[Row]): DataFrame = {
    import scala.collection.JavaConversions._
    sqlContext.createDataFrame(rows, schema)
  }

  def getStartEnd(_start: Option[Offset], _end: Offset): (Long, Long) = (_start, _end) match {
    case (Some(LongOffset(start)), LongOffset(end)) => (start, end)
    case (None, LongOffset(end))                    => (0L, end)
  }

  def getMapper(eventMapperFQCN: String): Option[EventMapper] =
    system.asInstanceOf[ExtendedActorSystem].dynamicAccess.createInstanceFor[EventMapper](eventMapperFQCN, List.empty)
      .recover { case cause => cause.printStackTrace(); null }.toOption

  override def stop(): Unit = {
    println("Stopping jdbc read journal")
    system.terminate()
  }
}
