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
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class RDDTest extends TestSpec {
  def withRDD(f: SparkContext => RDD[Int] => Unit): Unit = withSparkContext { sc =>
    val data: Array[Int] = Array(1, 2, 3, 4, 5)
    val rdd: RDD[Int] = sc.parallelize(data)
    f(sc)(rdd)
  }

  it should "collect sync" in withRDD { sc => rdd =>
    rdd.map(_ * 2).collect shouldBe Array(2, 4, 6, 8, 10)
  }

  it should "collect async" in withRDD { sc => rdd =>
    rdd.map(_ * 2).collectAsync.futureValue shouldBe Array(2, 4, 6, 8, 10)
  }

  it should "reduce" in withRDD { sc => rdd =>
    rdd.reduce(_ + _) shouldBe 15
  }

  it should "count sync" in withRDD { sc => rdd =>
    rdd.count shouldBe 5
  }

  it should "count async" in withRDD { sc => rdd =>
    rdd.countAsync.futureValue shouldBe 5
  }

  it should "zip" in withRDD { sc => rdd =>
    rdd.zip(sc.parallelize(('A' to 'Z').take(rdd.count.toInt)))
      .map { case (int, char) => s"$char-$int" }
      .collect shouldBe Seq("A-1", "B-2", "C-3", "D-4", "E-5")
  }
}
