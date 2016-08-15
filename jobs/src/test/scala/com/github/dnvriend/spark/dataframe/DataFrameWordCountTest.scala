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
import org.apache.spark.sql.{ DataFrame, Dataset }

class DataFrameWordCountTest extends TestSpec {
  it should "wordcount alice in wonderland" in withSparkSession { spark =>
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val lines: Dataset[String] = spark.read.text(TestSpec.AliceInWonderlandText).as[String]
    lines.count shouldBe 3599 // alice in wonderland contains 3599 lines
    val words: DataFrame = lines.flatMap((line: String) => line.split(" ")).map(_.trim).filter(_.nonEmpty).toDF("word")
    words.count() shouldBe 26467 // there are 26,467 words in the book, excluding spaces
    val wordCount: Dataset[(String, Long)] =
      words.groupBy('word).agg(count('word).as("count")).orderBy('count.desc).as[(String, Long)].cache

    wordCount.take(1).head shouldBe ("the", 1505) // the word 'the' is used 1505 times
    wordCount.filter(lower('word) === "alice").take(1).head shouldBe ("Alice", 221)
    wordCount.filter(lower('word) === "queen").take(1).head shouldBe ("Queen", 34)
    wordCount.filter(lower('word) === "rabbit").take(1).head shouldBe ("Rabbit", 29)
    wordCount.filter(lower('word) === "cheshire").take(1).head shouldBe ("Cheshire", 6)
  }
}
