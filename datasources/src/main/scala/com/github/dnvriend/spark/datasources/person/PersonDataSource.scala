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

import akka.stream.scaladsl.{ Source => AkkaStreamSource }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.{ LongOffset, Offset, Source }
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import com.github.dnvriend.spark._

final case class Person(id: Long, name: String, age: Int)

// see: org.apache.spark.sql.execution.streaming.TextSocketSource
object PersonStreamingDataSource {
  val name = "streaming-person"
}

class PersonStreamingDataSource extends StreamSourceProvider with DataSourceRegister {
  override def sourceSchema(
    sqlContext: SQLContext,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]
  ): (String, StructType) = {
    println(s"=> [PersonStreamingDataSource.sourceSchema]: providerName: '$providerName', schema: '$schema', parameters: $parameters")
    PersonStreamingDataSource.name -> PersonRelationProvider.schema
  }

  override def createSource(
    sqlContext: SQLContext,
    metadataPath: String,
    schema: Option[StructType],
    providerName: String,
    parameters: Map[String, String]
  ): Source = {
    println(s"=> [PersonStreamingDataSource.createSource]: metadataPath: '$metadataPath', schema: '$schema', providerName: '$providerName', parameters: '$parameters'")
    val take = parameters.get("take")
    val takeMax = take match {
      case Some(nr) => nr.toInt
      case _        => 250
    }
    new PersonStreamingSource(sqlContext, takeMax)
  }
  override def shortName(): String = PersonStreamingDataSource.name
}

class PersonStreamingSource(sqlContext: SQLContext, takeMax: Int) extends Source {
  override def schema: StructType = PersonRelationProvider.schema

  override def getOffset: Option[Offset] = Some(LongOffset(Long.MaxValue))

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = ???

  override def stop(): Unit = ()
}

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
  val schema = StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("age", IntegerType, nullable = true)
  ))
}

class PersonRelationProvider(val sqlContext: SQLContext, path: String) extends BaseRelation with TableScan with Serializable {
  override def schema: StructType = PersonRelationProvider.schema

  override def buildScan(): RDD[Row] =
    sqlContext.sparkContext.textFile(path)
      .filter(_.contains("person"))
      .map(line => PersonRelationProvider.regex.findAllIn(line).toList)
      .map { xs =>
        val id = xs.head.replace("id=\"", "").toLong
        val name = xs.drop(1).map(str => str.replace("name=\"", "")).headOption
        val age = xs.drop(2).map(str => str.replace("age=\"", "")).headOption.map(_.toInt)
        Row(id, name, age)
      }
}
