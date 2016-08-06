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

package com.github.dnvriend.spark

import com.github.dnvriend.TestSpec
import com.github.dnvriend.TestSpec.Transaction

class ItemsParserTest extends TestSpec {
  it should "parse data_transactions" in withSpark { spark =>
    import spark.implicits._
    val tx = spark.sparkContext.textFile(TestSpec.TranscationsCSV)
      .map(_.split("#")).map(TestSpec.mapToTransaction).toDS
    tx.count shouldBe 1000
  }

  it should "load items parquet" in withSpark { spark =>
    import spark.implicits._
    val tx = spark.read.parquet(TestSpec.Transactions).as[Transaction]
    tx.count shouldBe 1000
  }
}
