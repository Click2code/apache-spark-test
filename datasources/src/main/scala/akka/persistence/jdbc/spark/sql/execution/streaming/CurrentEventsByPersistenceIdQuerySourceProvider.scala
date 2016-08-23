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

import org.apache.spark.sql.execution.streaming.{ LongOffset, Offset, Source }
import org.apache.spark.sql.sources.{ DataSourceRegister, StreamSourceProvider }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ SQLContext, _ }

object CurrentEventsByPersistenceIdQuerySourceProvider {
  val name = "current-events-by-persistence-id"
}

class CurrentEventsByPersistenceIdQuerySourceProvider extends StreamSourceProvider with DataSourceRegister with Serializable {
  override def sourceSchema(
    sqlContext: SQLContext,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]
  ): (String, StructType) = {
    println(s"[CurrentEventsByPersistenceIdQuerySourceProvider.sourceSchema]: schema: $schema, providerName: $providerName, parameters: $parameters")
    CurrentEventsByPersistenceIdQuerySourceProvider.name -> schema.get
  }

  override def createSource(
    sqlContext: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]
  ): Source = {

    val eventMapperFQCN: String = parameters.get("event-mapper") match {
      case Some(_eventMapper) => _eventMapper
      case _                  => throw new RuntimeException("No event mapper FQCN")
    }

    val pid = (parameters.get("pid"), parameters.get("persistence-id")) match {
      case (Some(pid), _) => pid
      case (_, Some(pid)) => pid
      case _              => throw new RuntimeException("No persistence_id")
    }

    new CurrentEventsByPersistenceIdQuerySourceImpl(sqlContext, parameters("path"), eventMapperFQCN, pid, schema.get)
  }
  override def shortName(): String = CurrentEventsByPersistenceIdQuerySourceProvider.name
}

class CurrentEventsByPersistenceIdQuerySourceImpl(val sqlContext: SQLContext, val readJournalPluginId: String, eventMapperFQCN: String, persistenceId: String, override val schema: StructType) extends Source with ReadJournalSource {
  override def getOffset: Option[Offset] = {
    val offset = maxEventsByPersistenceId(persistenceId)
    println("[CurrentEventsByPersistenceIdQuery]: Returning maximum offset: " + offset)
    Some(LongOffset(offset))
  }

  override def getBatch(_start: Option[Offset], _end: Offset): DataFrame = {
    val (start, end) = getStartEnd(_start, _end)
    val df: DataFrame = eventsByPersistenceId(persistenceId, start, end, eventMapperFQCN)
    println(s"[CurrentEventsByPersistenceIdQuery]: Getting currentPersistenceIds from start: $start, end: $end, DataFrame.count: ${df.count}")
    df
  }
}