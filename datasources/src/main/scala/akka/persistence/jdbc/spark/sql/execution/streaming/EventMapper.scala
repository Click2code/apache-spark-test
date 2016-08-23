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

import akka.persistence.query.EventEnvelope
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ Row, SQLContext }

trait EventMapper {
  def row(envelope: EventEnvelope, sqlContext: SQLContext): Row
  def schema: StructType
}
