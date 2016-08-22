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

// see: https://blog.knoldus.com/2015/10/21/demystifying-asynchronous-actions-in-spark/
class AsyncActionsTest extends TestSpec {
  /**
   * What if we want to execute 2 actions concurrently on different RDD’s,
   * Spark actions are always `synchronous`. Like if we perform two actions
   * one after other they always execute in sequentially like one after other.
   */

  /**
   * In the example 2 actions are perform one after other collect and count,
   * both are execute synchronous. So count will always execute after collect will
   * finish.
   */

  it should "execute synchronously" in withSparkContext { sc =>
    val rdd = sc.parallelize(List(32, 34, 2, 3, 4, 54, 3), 4)
    rdd.collect().foreach(x => println("Items in the lists: " + x))
    val rddCount = sc.parallelize(List(434, 3, 2, 43, 45, 3, 2), 4)
    println("Number of items in the list: " + rddCount.count())
  }

  /**
   * What if we want to execute actions concurrently in async fashion.
   *
   * Apache spark also provide a (few) asynchronous actions for concurrent execution of jobs:
   *
   * - collectAsync(): Returns a future for retrieving all elements of this RDD.
   * - countAsync(): Returns a future for counting the number of elements in the RDD.
   * - foreachAsync(f): Applies a function f to all elements of this RDD.
   * - foreachPartitionAsync(f): Applies a function f to each partition of this RDD.
   * - takeAsync(num):  Returns a future for retrieving the first num elements of the RDD.
   */

  it should "execute asynchronously" in withSparkContext { sc =>
    val rdd = sc.parallelize(List(32, 34, 2, 3, 4, 54, 3), 4)
    rdd.collectAsync().map(x => x.foreach(x => println("Items in the list:  " + x))).futureValue
    val rddCount = sc.parallelize(List(434, 3, 2, 43, 45, 3, 2), 4)
    rddCount.countAsync().map(x => println("Number of items in the list: " + x)).futureValue
  }

  /**
   * The Async operations however will use all cluster resources. To take full advantage of
   * Asynchronous jobs we need to configure the job scheduler.
   */

  /**
   * By default spark scheduler run spark jobs in FIFO (First In First Out) fashion. In FIFO scheduler
   * the priority is given to the first job and then second and so on. If the jobs is not using whole
   * cluster then second job will also run parallel but if first job is too big then second job will wait
   * so long even it take less to execute it. The solution that spark provides is the `FAIR` scheduler.
   * FAIR scheduler will execute jobs in `round robin` fashion.
   *
   * The job scheduler can be configured, like everything in Spark by setting configuration items.
   * The scheduler mode can be set using the configuration: `spark.scheduler.mode`. You can
   * set this option from `FIFO` to `FAIR`. `
   */
}
