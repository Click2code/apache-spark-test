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

package com.github.dnvriend.spark.datasources.person

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.sources.{ BaseRelation, DataSourceRegister, RelationProvider, TableScan }
import org.apache.spark.sql.types._

class PersonDataSource extends RelationProvider with DataSourceRegister with Serializable {
  override def shortName(): String = "person"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: scala.Any): Boolean = other.isInstanceOf[PersonDataSource]

  override def toString: String = "PersonDataSource"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.get("path")
    path match {
      case Some(p) => new PersonRelationProvider(sqlContext, p)
      case _       => throw new IllegalArgumentException("Path is required for Tickets datasets")
    }
  }
}

object PersonRelationProvider {
  val regex = """(id="[\d]+)|(name="[\s\w]+)|(age="[\d]+)""".r
}

class PersonRelationProvider(val sqlContext: SQLContext, path: String) extends BaseRelation with TableScan with Serializable {
  import PersonRelationProvider._

  override def schema: StructType = StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("age", IntegerType, nullable = true)
  ))

  override def buildScan(): RDD[Row] =
    sqlContext.sparkContext.textFile(path)
      .filter(_.contains("person"))
      .map(line => regex.findAllIn(line).toList)
      .map { xs =>
        val id = xs.head.replace("id=\"", "").toLong
        val name = xs.drop(1).map(str => str.replace("name=\"", "")).headOption
        val age = xs.drop(2).map(str => str.replace("age=\"", "")).headOption.map(_.toInt)
        Row(id, name, age)
      }
}
