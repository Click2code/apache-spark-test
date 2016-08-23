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
import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import org.apache.spark.sql.{ SQLContext, _ }

object CurrentPersistenceIdsQuerySourceProvider {
  val name = "current-persistence-id"
  val schema: StructType = StructType(Array(
    StructField("persistence_id", StringType, nullable = false)
  ))
}

class CurrentPersistenceIdsQuerySourceProvider extends StreamSourceProvider with DataSourceRegister with Serializable {
  override def sourceSchema(
    sqlContext: SQLContext,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]
  ): (String, StructType) = {
    CurrentPersistenceIdsQuerySourceProvider.name -> CurrentPersistenceIdsQuerySourceProvider.schema
  }

  override def createSource(
    sqlContext: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]
  ): Source = {
    new CurrentPersistenceIdsQuerySourceImpl(sqlContext, parameters("path"))
  }
  override def shortName(): String = CurrentPersistenceIdsQuerySourceProvider.name
}

class CurrentPersistenceIdsQuerySourceImpl(val sqlContext: SQLContext, val readJournalPluginId: String) extends Source with ReadJournalSource {
  override def schema: StructType = CurrentPersistenceIdsQuerySourceProvider.schema

  override def getOffset: Option[Offset] = {
    val offset = maxPersistenceIds
    println("[CurrentPersistenceIdsQuery]: Returning maximum offset: " + offset)
    Some(LongOffset(offset))
  }

  override def getBatch(_start: Option[Offset], _end: Offset): DataFrame = {
    val (start, end) = getStartEnd(_start, _end)
    println(s"[CurrentPersistenceIdsQuery]: Getting currentPersistenceIds from start: $start, end: $end")
    import sqlContext.implicits._
    persistenceIds(start, end).toDF()
  }
}
