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

package com.github.dnvriend.spark.streaming

import com.github.dnvriend.TestSpec
import org.apache.spark.rdd.RDD

import scala.collection.mutable._

class HelloWorldStreamingTest extends TestSpec {

  it should "stream words from a dictionary" in withStreamingContext() { spark => ssc =>
    import spark.implicits._
    var xs = List.empty[String]
    val dictRdd: RDD[String] = spark.read.text(TestSpec.ScrabbleDictionaryCSV).limit(1).as[String].rdd
    val queue: Queue[RDD[String]] = Queue(List.fill(10)(dictRdd): _*)
    val stream = ssc.queueStream(queue, true, dictRdd)

    stream.foreachRDD { rdd =>
      xs = xs ++ rdd.collect()
    }

    ssc.start()
    advanceClockOneBatch(ssc)
    advanceClockOneBatch(ssc)
    advanceClockOneBatch(ssc)
    advanceClockOneBatch(ssc)
    advanceClockOneBatch(ssc)
    eventually {
      Thread.sleep(200)
      xs.size shouldBe 5
    }
  }
}
