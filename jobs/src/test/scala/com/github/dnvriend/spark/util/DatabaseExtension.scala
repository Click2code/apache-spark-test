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

package com.github.dnvriend.spark.util

import akka.actor.{ ExtendedActorSystem, Extension, ExtensionId, ExtensionIdProvider }
import slick.jdbc.JdbcBackend._

object DatabaseExtension extends ExtensionId[DatabaseExtensionImpl] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): DatabaseExtensionImpl = new DatabaseExtensionImpl()(system)

  override def lookup(): ExtensionId[_ <: Extension] = DatabaseExtension
}

class DatabaseExtensionImpl()(implicit val system: ExtendedActorSystem) extends Extension {
  val db: Database = Database.forConfig("slick.db", system.settings.config)
  sys.addShutdownHook(db.close()) // close db connections
}
