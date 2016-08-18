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

package com.github.dnvriend.spark.datasources

import com.github.dnvriend.TestSpec
import com.github.dnvriend.spark.ElectionCandidate
import com.github.dnvriend.spark.datasources.SparkImplicits._

class CsvDatasourceTest extends TestSpec {

  it should "read a CSV" in withSparkSession { spark =>
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val aangifte = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", ";")
      .load(TestSpec.AangifteGroningenCSV)

    aangifte.count() shouldBe 420

    val afvalContainers = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", ";")
      .load(TestSpec.AfvalContainersGroningenCSV)

    afvalContainers.count() shouldBe 961

    val candidates = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .option("delimiter", ",")
      .load(TestSpec.FederalElectionCandidatesCSV).as[ElectionCandidate]

    candidates.count() shouldBe 1626

    candidates
      .select('occupation)
      .distinct
      .count() shouldBe 790

    candidates
      .filter(lower('occupation) like "%it%")
      .count shouldBe 131

    // top 10 families that are candidate
    candidates
      .groupBy('surname)
      .agg(count('surname).as("count_surname"))
      .orderBy($"count_surname".desc)
      .limit(10)
      .as[(String, Long)].collect shouldBe Seq(
        ("SMITH", 15),
        ("RYAN", 9),
        ("JONES", 8),
        ("ANDERSON", 8),
        ("HALL", 6),
        ("BAKER", 6),
        ("MARTIN", 6),
        ("BUCKLEY", 5),
        ("O'BRIEN", 5),
        ("KELLY", 5)
      )
  }

  it should "read compressed CSV" in withSparkSession { spark =>
    import spark.implicits._
    spark.read
      .format("com.databricks.spark.csv")
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .option("delimiter", ",")
      .load(TestSpec.ScrabbleDictionaryCSV)
      .toDF("word")
      .as[String]
      .count() shouldBe 172820
  }

  it should "read compressed CSV from inplicit" in withSparkSession { spark =>
    import spark.implicits._
    spark.read
      .option("header", "false") // Use first line of all files as header
      .option("inferSchema", "false") // Automatically infer data types
      .option("delimiter", ",")
      .databricksCsv(TestSpec.ScrabbleDictionaryCSV)
      .toDF("word")
      .as[String]
      .count() shouldBe 172820
  }
}
