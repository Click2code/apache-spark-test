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

package com.github.dnvriend.spark.streaming.akka

import java.io.{ File, FilenameFilter }

import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
import com.github.dnvriend.TestSpec
import com.github.dnvriend.spark.streaming.akka.FeederActor.{ SubscribeReceiver, UnsubscribeReceiver }
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.akka.{ ActorReceiver, AkkaUtils }
import org.scalatest.Ignore

import scala.concurrent.duration._
import scala.util.Random

/**
 * Generates random content and sends it to its
 * parent (the feeder) every 1/2 second
 */
class RandomMessageGenerator extends Actor {
  import context.dispatcher
  val rand = new Random()
  val strings: Array[String] = Array("words ", "may ", "count ")

  def ping() = context.system.scheduler.scheduleOnce(500.millis, self, "foo")
  def makeMessage(): String = {
    val x = rand.nextInt(3)
    strings(x) + strings(2 - x)
  }

  override def receive: Receive = {
    case _ =>
      context.parent ! makeMessage()
      ping()
  }

  ping()
}

/**
 *  Sends the received random content to every receiver
 */
object FeederActor {
  case class SubscribeReceiver(receiverActor: ActorRef)
  case class UnsubscribeReceiver(receiverActor: ActorRef)
}

class FeederActor extends Actor {
  val rmg = context.actorOf(Props[RandomMessageGenerator], "RandomMessageGenerator")
  def receive: Receive = active(Set.empty[ActorRef])
  def active(receivers: Set[ActorRef]): Receive = {
    case SubscribeReceiver(receiverActor: ActorRef) =>
      println(s"received subscribe from $receiverActor")
      context.become(active(receivers + receiverActor))

    case UnsubscribeReceiver(receiverActor: ActorRef) =>
      println(s"received unsubscribe from $receiverActor")
      context.become(active(receivers - receiverActor))

    case msg: String =>
      println(s"sending message: '$msg' to receivers: [${receivers.size}]")
      receivers.foreach(_ ! msg)
  }
}

/**
 * A sample actor as receiver, is also simplest. This receiver actor
 * goes and subscribe to a typical publisher/feeder actor and receives
 * data.
 */
class SampleActorReceiver[T] extends ActorReceiver {
  lazy private val remotePublisher =
    context.actorSelection(s"akka.tcp://test@localhost:2552/user/FeederActor")

  override def preStart(): Unit = {
    println("==> Launching: SampleActorReceiver")
    remotePublisher ! SubscribeReceiver(context.self)
  }

  def receive: PartialFunction[Any, Unit] = {
    case msg => store(msg.asInstanceOf[T])
  }

  override def postStop(): Unit = {
    println("==> Stopping: SampleActorReceiver")
    remotePublisher ! UnsubscribeReceiver(context.self)
  }
}

@Ignore
class ActorWordCountTest extends TestSpec {
  /**
   * Creates an ActorSystem that has remoting enabled,
   * creates the Feeder actor
   * manages the life cycle of both the feeder actor and the
   * actor system
   */
  def withFeederActor(f: ActorRef => Unit) = {
    val host = "localhost"
    val port = "2552"
    val akkaConf = ConfigFactory.parseString(
      s"""akka.actor.provider = "akka.remote.RemoteActorRefProvider"
          |akka.remote.enabled-transports = ["akka.remote.netty.tcp"]
          |akka.remote.netty.tcp.hostname = "$host"
          |akka.remote.netty.tcp.port = $port
          |""".stripMargin
    )
    val actorSystem = ActorSystem("test", akkaConf)
    val ref = actorSystem.actorOf(Props[FeederActor], "FeederActor")
    try f(ref) finally {
      killActors(ref)(actorSystem)
      actorSystem.terminate().toTry should be a 'success
    }
  }

  def delete(): Unit =
    new File("/tmp").listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean =
        name.contains("received.csv")
    }).foreach(FileUtils.deleteDirectory)

  // note: there are two (2) actor systems at play here
  // 1. The one from SparkStreamingAkka that spawns one
  // 2. One from this test
  //
  // The basic idea is that there is already a running ActorSystem (the system of this test),
  // and that ActorSystem has remoting enabled
  // The actor that has been created by SparkStreamingAkka will look up the remote Actor by actorSelection
  // and subscribe/unsubscribe so it can receive  whwatever messages the test will send to the Actor
  //
  // The ActorReceiver can then store these (typed) messages in data blocks (in Spark's memory).
  //
  it should "receive random messages" in withStreamingContext() { spark => ssc =>
    withFeederActor { feeder =>
      import spark.implicits._
      val lines = AkkaUtils.createStream[String](ssc, Props[SampleActorReceiver[String]](), "CustomReceiver")

      // compute wordcount
      //      lines.flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _).print()
      lines.foreachRDD { rdd =>
        rdd.toDF("received").show()
      }

      ssc.start()
      (0 to 5).foreach { _ =>
        advanceClockOneBatch(ssc)
        sleep()
      }
    }
  }

  it should "store receive random messages in /tmp/received.csv" in {
    withStreamingContext() { spark => ssc =>
      withFeederActor { feeder =>
        import spark.implicits._
        delete()

        val lines = AkkaUtils.createStream[String](ssc, Props[SampleActorReceiver[String]](), "CustomReceiver")

        // compute wordcount
        //      lines.flatMap(_.split("\\s+")).map(x => (x, 1)).reduceByKey(_ + _).print()
        lines.foreachRDD { rdd =>
          rdd.toDF("received").write.mode(SaveMode.Append).csv("/tmp/received.csv")
        }

        ssc.start()
        (0 to 5).foreach { _ =>
          advanceClockOneBatch(ssc)
          sleep()
        }
      }
    }
    withSparkSession { spark =>
      spark.read.csv("/tmp/received.csv").show()
    }
  }
}
