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

package com.github.dnvriend.spark.streaming.twitter

import com.github.dnvriend.TestSpec
import com.github.dnvriend.spark.Tweet
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.{ DStream, ReceiverInputDStream }
import org.apache.spark.streaming.twitter.TwitterUtils
import org.scalatest.Ignore
import pprint.Config.Colors.PPrintConfig
import pprint._
import twitter4j.Status

// see: https://dev.twitter.com/streaming/overview
// see: https://dev.twitter.com/streaming/public
// see: https://support.twitter.com/articles/20174643
// see: https://github.com/apache/bahir/blob/master/streaming-twitter/examples/src/main/scala/org/apache/spark/examples/streaming/twitter/TwitterPopularTags.scala
// see: http://blog.originate.com/blog/2014/06/15/idiomatic-scala-your-options-do-not-match/

@Ignore
class TwitterPopularTagsTest extends TestSpec {
  it should "find popular tags" in withStreamingContext(2, await = true) { spark => ssc =>

    //    val filters = Array("#scala", "#akka", "#spark", "@scala", "@akka", "@spark")
    val filters = Array("#summercamp", "#akka", "#scala", "#fastdata", "#spark", "#hadoop")
    val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None, filters)

    val msgs: DStream[Tweet] =
      stream
        .map(Tweet(_))

    msgs.foreachRDD { rdd =>
      rdd.take(10).foreach(pprint.pprintln)
    }

    val hashTags: DStream[String] =
      stream
        .filter(_.getLang == "en")
        .flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 =
      hashTags
        .map((_, 1))
        .reduceByKeyAndWindow(_ + _, Seconds(60))
        .map { case (topic, count) => (count, topic) }
        .transform(_.sortByKey(ascending = false))

    val topCounts10 =
      hashTags
        .map((_, 1))
        .reduceByKeyAndWindow(_ + _, Seconds(10))
        .map { case (topic, count) => (count, topic) }
        .transform(_.sortByKey(false))

    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      pprint.pprintln("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      pprint.pprintln("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    ssc.start()
  }
}
