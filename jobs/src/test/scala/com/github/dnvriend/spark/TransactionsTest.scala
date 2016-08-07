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
import org.apache.spark.rdd.RDD

class TransactionsTest extends TestSpec {

  it should "parse data_transactions" in withSpark { spark =>
    import spark.implicits._
    val tx = spark.sparkContext.textFile(TestSpec.TranscationsCSV)
      .map(_.split("#")).map(TestSpec.mapToTransaction).toDS
    tx.count shouldBe 1000
  }

  it should "load transactions parquet" in withTx { spark => tx =>
    tx.count shouldBe 1000
  }

  it should "count distinct customers" in withTx { spark => tx =>
    import spark.implicits._
    tx.map(_.customer_id).distinct().count shouldBe 100 // 1,66s
  }

  it should "count distinct customers using groupByKey (avoid)" in withTx { spark => tx =>
    import spark.implicits._
    tx.groupByKey(_.customer_id).keys.distinct().count shouldBe 100 // 1.32s
  }

  it should "Create pair rdd and count distinct customers" in withTx { spark => tx =>
    import spark.implicits._
    val pair: RDD[(Int, Int)] = tx.map(tx => (tx.customer_id, 1)).rdd
    pair.keys.distinct().count shouldBe 100 // 0,218609s
  }

  it should "Create pair rdd and count total transactions" in withTx { spark => tx =>
    import spark.implicits._
    val pair: RDD[(Int, Int)] = tx.map(tx => (tx.customer_id, 1)).rdd
    pair.reduceByKey(_ + _).values.sum shouldBe 1000 // 0,051471s
  }
}
