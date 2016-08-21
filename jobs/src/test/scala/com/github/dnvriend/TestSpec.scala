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

package com.github.dnvriend

import akka.actor.{ ActorRef, ActorSystem, PoisonPill }
import akka.event.{ Logging, LoggingAdapter }
import akka.stream.{ ActorMaterializer, Materializer }
import akka.testkit.TestProbe
import akka.util.Timeout
import com.github.dnvriend.spark._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ Dataset, SparkSession }
import org.apache.spark.streaming.{ ClockWrapper, Seconds, StreamingContext }
import org.scalatest.concurrent.{ Eventually, ScalaFutures }
import org.scalatest.{ BeforeAndAfterAll, FlatSpec, Matchers }

import scala.concurrent.{ ExecutionContext, Future }
import scala.concurrent.duration._
import scala.util.Try

object TestSpec {

  def mapToTransaction(xs: Array[String]) =
    Transaction(xs(2).toInt, xs(3).toInt, xs(4).toInt, xs(5).toDouble, strToSqlTime(xs(0).trim + " " + xs(1).trim))

  implicit def strToSqlDate(str: String): java.sql.Date =
    new java.sql.Date(new java.text.SimpleDateFormat("yyyy-MM-dd").parse(str).getTime)

  implicit def strToSqlTime(str: String): java.sql.Timestamp =
    new java.sql.Timestamp(new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm a").parse(str).getTime)

  val purchaseItems = Seq(
    PurchaseItem(1, "McLendon's", "Hardware", 2121.09, "2014-01-12"),
    PurchaseItem(2, "Bond", "Electrical", 12347.87, "2014-01-18"),
    PurchaseItem(3, "Craftsman", "Hardware", 999.99, "2014-01-22"),
    PurchaseItem(4, "Stanley", "Hardware", 6532.09, "2014-01-31"),
    PurchaseItem(5, "RubberMaid", "Kitchenware", 3421.10, "2014-02-03"),
    PurchaseItem(6, "RubberMaid", "KitchenWare", 1290.90, "2014-02-07"),
    PurchaseItem(7, "Glidden", "Paint", 12987.01, "2014-02-10"),
    PurchaseItem(8, "Dunn's", "Lumber", 43235.67, "2014-02-21"),
    PurchaseItem(9, "Maytag", "Appliances", 89320.19, "2014-03-10"),
    PurchaseItem(10, "Amana", "Appliances", 53821.19, "2014-03-12"),
    PurchaseItem(11, "Lumber Surplus", "Lumber", 3245.59, "2014-03-14"),
    PurchaseItem(12, "Global Source", "Outdoor", 3331.59, "2014-03-19"),
    PurchaseItem(13, "Scott's", "Garden", 2321.01, "2014-03-21"),
    PurchaseItem(14, "Platt", "Electrical", 3456.01, "2014-04-03"),
    PurchaseItem(15, "Platt", "Electrical", 1253.87, "2014-04-21"),
    PurchaseItem(16, "RubberMaid", "Kitchenware", 3332.89, "2014-04-20"),
    PurchaseItem(17, "Cresent", "Lighting", 345.11, "2014-04-22"),
    PurchaseItem(18, "Snap-on", "Hardware", 2347.09, "2014-05-03"),
    PurchaseItem(19, "Dunn's", "Lumber", 1243.78, "2014-05-08"),
    PurchaseItem(20, "Maytag", "Appliances", 89876.90, "2014-05-10"),
    PurchaseItem(21, "Parker", "Paint", 1231.22, "2014-05-10"),
    PurchaseItem(22, "Scotts's", "Garden", 3246.98, "2014-05-12"),
    PurchaseItem(23, "Jasper", "Outdoor", 2325.98, "2014-05-14"),
    PurchaseItem(24, "Global Source", "Outdoor", 8786.99, "2014-05-21"),
    PurchaseItem(25, "Craftsman", "Hardware", 12341.09, "2014-05-22")
  )

  final val AliceInWonderlandText = "src/test/resources/alice_in_wonderland.txt"
  final val TreesParquet = "src/test/resources/bomen.parquet"
  final val PersonsParquet = "src/test/resources/people.parquet"
  final val PurchaseItems = "src/test/resources/purchase_items.parquet"
  final val Transactions = "src/test/resources/transactions.parquet"
  final val OrdersParquet = "src/test/resources/orders.parquet"
  final val CustomersParquet = "src/test/resources/customers.parquet"
  final val TranscationsCSV = "src/test/resources/data_transactions.csv"
  final val FederalElectionCandidatesCSV = "src/test/resources/2016federalelection-all-candidates-nat-30-06-924.csv"
  final val AangifteGroningenCSV = "src/test/resources/aangifte_groningen.csv"
  final val AfvalContainersGroningenCSV = "src/test/resources/afvalcontainers_groningen.csv"
  final val ScrabbleDictionaryCSV = "src/test/resources/scrabble_dictionary.csv.gz"
}

abstract class TestSpec extends FlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll with Eventually {
  implicit val system: ActorSystem = ActorSystem()
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val log: LoggingAdapter = Logging(system, this.getClass)
  implicit val pc: PatienceConfig = PatienceConfig(timeout = 5.minutes, 1.second)
  implicit val timeout = Timeout(5.minutes)

  private val _spark = SparkSession.builder()
    .config("spark.sql.warehouse.dir", "file:/tmp/spark-warehouse")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.ui.enabled", "true") // better to enable this to see what is going on
    .config("spark.sql.autoBroadcastJoinThreshold", 1)
    .config("spark.default.parallelism", 4) // number of cores
    .config("spark.sql.shuffle.partitions", 1) // default 200
    .config("spark.memory.offHeap.enabled", "true") // If true, Spark will attempt to use off-heap memory for certain operations.
    .config("spark.memory.offHeap.size", "536870912") // The absolute amount of memory in bytes which can be used for off-heap allocation.
    .config("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
    .config("spark.streaming.stopSparkContextByDefault", "false")
    .config("spark.debug.maxToStringFields", 50) // default 25 see org.apache.spark.util.Utils
    // see: https://spark.apache.org/docs/latest/sql-programming-guide.html#caching-data-in-memory
    //    .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
    //    .config("spark.sql.inMemoryColumnarStorage.batchSize", "10000")
    .master("local[2]") // better not to set this to 2 for spark-streaming
    .appName("spark-sql-test").getOrCreate()

  def killActors(actors: ActorRef*)(implicit system: ActorSystem): Unit = {
    val tp = TestProbe()
    actors.foreach { (actor: ActorRef) =>
      tp watch actor
      actor ! PoisonPill
      tp.expectTerminated(actor)
    }
  }

  implicit class PimpedFuture[T](self: Future[T]) {
    def toTry: Try[T] = Try(self.futureValue)
  }

  def sleep(duration: Duration = 1.second): Unit =
    Thread.sleep(duration.toMillis)

  def withSparkContext[A](f: SparkContext => A): A =
    f(_spark.newSession().sparkContext)

  def withSparkSession[A](f: SparkSession => A): A =
    f(_spark.newSession())

  def withStreamingContext[A](seconds: Long = 1, await: Boolean = false)(f: SparkSession => StreamingContext => A): A = withSparkSession { spark =>
    val ssc = new StreamingContext(spark.sparkContext, Seconds(seconds))
    try f(spark)(ssc) finally if (await) ssc.awaitTermination() else stopStreamingContext(ssc)
  }

  def advanceClock(ssc: StreamingContext, timeToAdd: FiniteDuration): Unit = {
    ClockWrapper.advance(ssc, timeToAdd)
  }

  def advanceClockOneBatch(ssc: StreamingContext): Unit = {
    advanceClock(ssc, 1.second)
  }

  def stopStreamingContext(ssc: StreamingContext): Unit =
    ssc.stop()

  def withTx(f: SparkSession => Dataset[Transaction] => Unit): Unit = withSparkSession { spark =>
    import spark.implicits._
    f(spark)(spark.read
      .option("mergeSchema", "false")
      .parquet(TestSpec.Transactions).as[Transaction])
  }

  def withTrees(f: SparkSession => Dataset[Tree] => Unit): Unit = withSparkSession { spark =>
    import spark.implicits._
    f(spark)(spark.read
      .option("mergeSchema", "false")
      .parquet(TestSpec.TreesParquet).as[Tree])
  }

  // for twitter api
  val cfg = system.settings.config.getConfig("twitter")
  val consumerKey = cfg.getString("consumerKey")
  val consumerSecret = cfg.getString("consumerSecret")
  val accessToken = cfg.getString("accessToken")
  val accessTokenSecret = cfg.getString("accessTokenSecret")
  System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
  System.setProperty("twitter4j.oauth.accessToken", accessToken)
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
  println(s"==> Twitter API: consumerKey: $consumerKey, consumerSecret: $consumerSecret, accessToken: $accessToken, accessTokenSecret: $accessTokenSecret")

  override protected def afterAll(): Unit = {
    _spark.stop()
    system.terminate()
  }
}
