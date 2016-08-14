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

package com.github.dnvriend.spark.datasources.helloworld

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{ BaseRelation, DataSourceRegister, RelationProvider, TableScan }
import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import org.apache.spark.sql.{ Row, SQLContext }

class HelloWorldDataSource extends RelationProvider with DataSourceRegister with Serializable {
  override def shortName(): String = "helloworld"

  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: scala.Any): Boolean = other.isInstanceOf[HelloWorldDataSource]

  override def toString: String = "HelloWorldDataSource"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.get("path")
    path match {
      case Some(p) => new HelloWorldRelationProvider(sqlContext, p, parameters)
      case _       => throw new IllegalArgumentException("Path is required for Tickets datasets")
    }
  }
}

class HelloWorldRelationProvider(val sqlContext: SQLContext, path: String, parameters: Map[String, String]) extends BaseRelation with TableScan {
  import sqlContext.implicits._

  override def schema: StructType = StructType(Array(
    StructField("key", StringType, nullable = false),
    StructField("value", StringType, nullable = true)
  ))

  override def buildScan(): RDD[Row] =
    Seq(
      "path" -> path,
      "message" -> parameters.getOrElse("message", ""),
      "name" -> s"Hello ${parameters.getOrElse("name", "")}",
      "hello_world" -> "Hello World!"
    ).toDF.rdd
}