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

class RDDTest extends TestSpec {
  val data = Array(1, 2, 3, 4, 5)
  val rdd = sc.parallelize(data)

  it should "collect sync" in {
    rdd.map(_ * 2).collect shouldBe Array(2, 4, 6, 8, 10)
  }

  it should "collect async" in {
    rdd.map(_ * 2).collectAsync.futureValue shouldBe Array(2, 4, 6, 8, 10)
  }

  it should "reduce" in {
    rdd.reduce(_ + _) shouldBe 15
  }

  it should "count sync" in {
    rdd.count shouldBe 5
  }

  it should "count async" in {
    rdd.countAsync.futureValue shouldBe 5
  }

  it should "zip" in {
    rdd.zip(sc.parallelize(('A' to 'Z').take(rdd.count.toInt)))
      .map { case (int, char) => s"$char-$int" }
      .collect shouldBe Seq("A-1", "B-2", "C-3", "D-4", "E-5")
  }
}
