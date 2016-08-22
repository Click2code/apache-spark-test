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

package com.github.dnvriend.spark.rdd

import com.github.dnvriend.TestSpec
import org.apache.spark.rdd.RDD

class RddWordCountTest extends TestSpec {
  it should "wordcount alice in wonderland" in withSparkContext { sc =>
    val lines: RDD[String] = sc.textFile(TestSpec.AliceInWonderlandText)
    lines.count shouldBe 3599 // alice in wonderland contains 3599 lines
    val words: RDD[String] = lines.flatMap((line: String) => line.split(" ")).map(_.trim).filter(_.nonEmpty)
    words.count() shouldBe 26467 // there are 26,467 words in the book, excluding spaces
    val wordCount: List[(String, Int)] = words.map((_, 1)).reduceByKey(_ + _).sortBy(_._2, ascending = false).take(10).toList

    wordCount.take(1).head shouldBe ("the", 1505) // the word 'the' is used 1505 times
  }
}
