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

package com.github.dnvriend.spark.dataframe

import com.github.dnvriend.TestSpec
import com.github.dnvriend.spark.datasources.person.Person
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Dataset, Row }

class RddToDataFrameTest extends TestSpec {
  val people: Seq[Person] = Seq(
    Person(1, "Jonathan Archer", 41),
    Person(2, "Reginald Barclay", 45),
    Person(3, "Julian Bashir", 28),
    Person(4, "Pavel Chekov", 52),
    Person(5, "Beverly Crusher", 32),
    Person(6, "Jadzia Dax", 21),
    Person(7, "Geordi La Forge", 35)
  )

  val schema: StructType = StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("name", StringType, nullable = true),
    StructField("age", IntegerType, nullable = true)
  ))

  it should "convert an RDD[A <: Product] ie. 'case class' to a DataFrame" in withSparkSession { spark =>
    import spark.implicits._

    // a Product, or case class, contains the individual data types of the fields
    // so spark can use these types as the schema.
    // Note that the fields of the case class will become the column names.
    val peopleRDD: RDD[Person] = spark.sparkContext.parallelize(people)
    val peopleDF: DataFrame = peopleRDD.toDF()
    val peopleDs: Dataset[Person] = peopleRDD.toDS()
    // or
    val peopleDataset: Dataset[Person] = peopleDF.as[Person]
    peopleDs.collect() shouldBe people
    peopleDataset.collect() shouldBe people
  }

  it should "convert an RDD[Row] to a DataFrame with help of a schema" in withSparkSession { spark =>
    import spark.implicits._

    // a org.apache.spark.sql.Row cannot be used by spark to deduce the type.
    // To convert an RDD[Row], spark needs a schema to convert an RDD[Row] to
    // a DataFrame. Note that the schema defines the field names and types
    //
    // Notice the 'Long' type for the id field. The schema defines that
    // id should be a LongType, so we must use a Long literal.
    //
    val peopleRows: Seq[Row] = Seq(
      Row(1L, "Jonathan Archer", 41),
      Row(2L, "Reginald Barclay", 45),
      Row(3L, "Julian Bashir", 28),
      Row(4L, "Pavel Chekov", 52),
      Row(5L, "Beverly Crusher", 32),
      Row(6L, "Jadzia Dax", 21),
      Row(7L, "Geordi La Forge", 35)
    )

    val rowRDD: RDD[Row] = spark.sparkContext.parallelize(peopleRows)
    val peopleDF: DataFrame = spark.createDataFrame(rowRDD, schema)
    val peopleDs: Dataset[Person] = peopleDF.as[Person]

    peopleDs.collect() shouldBe people
  }

  it should "convert a text file to a Dataset" in withSparkSession { spark =>
    import spark.implicits._
    val peopleDF: DataFrame = spark.read.textFile(TestSpec.PeopleCsv)
      .map(_.split(";"))
      .map(xs => (xs(0).toLong, xs(1), xs(2).toInt)).toDF("id", "name", "age")

    val peopleDs = peopleDF.as[Person]
    peopleDs.collect() shouldBe people
  }

  it should "convert a csv file to a Dataset" in withSparkSession { spark =>
    import spark.implicits._

    val peopleDF: DataFrame = spark.read
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .option("delimiter", ";")
      .schema(schema)
      .csv(TestSpec.PeopleCsv)

    val peopleDs = peopleDF.as[Person]
    peopleDs.collect() shouldBe people
  }
}
