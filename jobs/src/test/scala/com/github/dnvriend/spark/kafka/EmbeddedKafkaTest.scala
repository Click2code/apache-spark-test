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

package com.github.dnvriend.spark.kafka

import com.github.dnvriend.TestSpec
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.{ Deserializer, Serializer }

class EmbeddedKafkaTest extends TestSpec with EmbeddedKafka {
  final val TopicName = "MyTopic"

  def publish[T: Serializer](msg: T): Unit = publishToKafka(TopicName, msg)
  def consume[T: Deserializer]: T = consumeFirstMessageFrom(TopicName)

  import net.manub.embeddedkafka.Codecs._
  it should "setup and embedded kafka, create a topic, send a message and receive a message from the same topic" in withRunningKafka {
    publish("foo")
    consume[String] shouldBe "foo"
    publish("bar".getBytes)
    consume[Array[Byte]] shouldBe "bar".getBytes()

  }
}
