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

package com.github.dnvriend.spark.mapper

import akka.persistence.jdbc.spark.sql.execution.streaming.EventMapper
import akka.persistence.query.EventEnvelope
import com.github.dnvriend.spark.datasources.person.Person
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.types._

class PersonEventMapper extends EventMapper {
  override def row(envelope: EventEnvelope, sqlContext: SQLContext): Row = envelope match {
    case EventEnvelope(offset, persistenceId, sequenceNr, Person(id, name, age)) =>
      Row(offset, persistenceId, sequenceNr, id, name, age)
  }

  override def schema: StructType =
    PersonEventMapper.schema
}

object PersonEventMapper {
  val schema = StructType(Array(
    StructField("offset", LongType, nullable = false),
    StructField("persistence_id", StringType, nullable = false),
    StructField("sequence_number", LongType, nullable = false),
    StructField("id", LongType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("age", IntegerType, nullable = true)
  ))
}
