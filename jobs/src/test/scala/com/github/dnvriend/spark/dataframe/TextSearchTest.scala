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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class TextSearchTest extends TestSpec {
  it should "search for error messages in a log file" in withSparkSession { spark =>
    import spark.implicits._
    // creates a DataFrame having a single column named "line"
    val df: DataFrame = spark.read.textFile(TestSpec.ApacheErrorLog).toDF("line")
    val errors = df.filter(lower('line).like("%error%"))
    // counts all the errors
    errors.count() shouldBe 5
  }
}
