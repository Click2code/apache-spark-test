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

class HelloWorldDataSourceTest extends TestSpec {
  it should "read from helloworld" in withSpark { spark =>
    import spark.implicits._
    val result = spark.read
      .format("com.github.dnvriend.spark.datasources.helloworld.HelloWorldDataSource")
      .option("message", "hi there!")
      .option("name", "Dennis")
      .load("/tmp/foobar.csv")

    result.as[(String, String)].collect shouldBe Array(
      ("path", "/tmp/foobar.csv"),
      ("message", "hi there!"),
      ("name", "Hello Dennis"),
      ("hello_world", "Hello World!")
    )
  }

  it should "read from helloworld using shortname" in withSpark { spark =>
    import spark.implicits._
    val result = spark.read
      .format("helloworld")
      .option("message", "hi there!")
      .option("name", "Dennis")
      .load("/tmp/foobar.csv")

    result.as[(String, String)].collect shouldBe Array(
      ("path", "/tmp/foobar.csv"),
      ("message", "hi there!"),
      ("name", "Hello Dennis"),
      ("hello_world", "Hello World!")
    )
  }
}
