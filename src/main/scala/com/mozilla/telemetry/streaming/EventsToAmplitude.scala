/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import com.github.fge.jsonschema.main.JsonSchemaFactory
import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings._
import com.mozilla.telemetry.streaming.StreamingJobBase.TelemetryKafkaTopic
import com.mozilla.telemetry.streaming.sinks.HttpSink
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.rogach.scallop.ScallopOption

import scala.io.Source
// TODO:
// - incorporate event schema - DEPENDS ON EVENT SCHEMA
// - Profile json4s/JValue/JsonNode schema validation
// - Include per-doctype events
// - Include per-doctype user properties

/**
 * Stream events to amplitude. More generally, stream
 * events to an HTTP Endpoint.
 *
 * Backfill is done per-day. There are two options given
 * for limiting backfill: Max Parallelism and Min Delay.
 * Max Parallelism is the max number of parallel requests,
 * and Min Delay is the minimum delay per request, in ms.
 *
 * Max requests/second = 10^3 * MaxParallelism / MinDelay
 *
 * Sampling is limited to the hundredths place; anything
 * more will be truncated (e.g. .355 will be .35, so 35%).
 */
object EventsToAmplitude extends StreamingJobBase {

  val AMPLITUDE_API_KEY_KEY = "AMPLITUDE_API_KEY"
  val TOP_LEVEL_PING_FIELDS =
    "appBuildId" ::
    "appName" ::
    "appUpdateChannel" ::
    "appVersion" ::
    "clientId" ::
    "docType" ::
    "geoCity" ::
    "geoCountry" ::
    "normalizedChannel" ::
    "submissionDate" ::
    Nil

  val MetaJsonFile = "schemaFileSchema.json"

  val allowedDocTypes = List("focus-event")
  val allowedAppNames = List("Focus")
  val kafkaCacheMaxCapacity = 1000
  val writeMode = "error"

  private[streaming] class Opts(args: Array[String]) extends BaseOpts(args) {
    val configFilePath: ScallopOption[String] = opt[String](
      descr = "JSON file with the configuration",
      required = true)
    val raiseOnError:ScallopOption[Boolean] = opt[Boolean](
      descr = "Whether the program should exit on a data processing error or not.")
    val failOnDataLoss:ScallopOption[Boolean] = opt[Boolean](
      descr = "Whether to fail the query when it’s possible that data is lost.")
    val url: ScallopOption[String] = opt[String](
      descr = "Endpoint to send data to",
      required = true)
    val sample: ScallopOption[Double] = opt[Double](
      descr = "Fraction of clients to use",
      required = false,
      default = Some(1.0))
    val minDelay: ScallopOption[Long] = opt[Long](
      descr = "Amount of delay between requesets in batch mode, in ms",
      required = false,
      default = Some(0))
    val maxParallelRequests: ScallopOption[Int] = opt[Int](
      descr = "Max number of parallel requests in batch mode",
      required = false,
      default = Some(100))

    conflicts(kafkaBroker, List(from, to, fileLimit, minDelay, maxParallelRequests))
    validateOpt (sample) {
      case Some(s) if 0.0 < s && s <= 1.0 => Right(Unit)
      case Some(o) => Left(s"Sample out of range. Expected 0.0 < Sample <= 1.0, Found $o")
    }

    verify()
  }

  case class AmplitudeEvent(
    name: String,
    description: String,
    sessionIdOffset: Option[String],
    amplitudeProperties: Option[Map[String, String]],
    userProperties: Option[Map[String, String]],
    schema: JValue)

  case class AmplitudeEventGroup(eventGroupName: String, events: List[AmplitudeEvent])

  case class Config(
    filters: Map[String, List[String]],
    eventGroups: Seq[AmplitudeEventGroup]) {
    def getBatchFilters: Map[String, List[String]] = {
      filters.map{ case(k, v) => k ->
        (k match {
          case "docType" => v.map(_.replace("-", "_"))
          case _ => v
        })
      }
    }

    val topLevelFilters = filters.filter {
      case(name, _) => TOP_LEVEL_PING_FIELDS.contains(name)
    }

    val nonTopLevelFilters = filters.filter {
      case(name, _) => !TOP_LEVEL_PING_FIELDS.contains(name)
    }
  }

  def parsePing(message: Message, sample: Double, config: Config): Array[String] = {
    val emptyReturn = Array[String]()
    val fields = message.fieldsAsMap

    config.topLevelFilters
      .map{ case(name, allowedVals) =>
        allowedVals.contains(fields.getOrElse(name, "").asInstanceOf[String])
      }.reduce(_ & _) match {
        case false => emptyReturn
        case true =>
          SendsToAmplitude(message) match {
            case p if !p.includePing(sample, config) => emptyReturn
            case p => p.getAmplitudeEvents(config).map(Array(_)).getOrElse(emptyReturn)
          }
      }
  }

  def getEvents(config: Config, pings: DataFrame, sample: Double, raiseOnError: Boolean): Dataset[String] = {
    import pings.sparkSession.implicits._

    pings.flatMap( v => {
        try {
          parsePing(Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]]), sample, config)
        } catch {
          case _: Throwable if !raiseOnError => Array[String]()
        }
      }).as[String]
  }

  private def getMetaSchema: JValue = {
    parse(
      Source.fromURL(
        getClass.getResource(s"/schemas/$MetaJsonFile")
      ).reader()
    )
  }

  def readConfigFile(filePath: String): Config = {
    val source = Source.fromFile(filePath)
    val json = parse(source.reader())

    // validate config json
    val factory = JsonSchemaFactory.byDefault
    val schema = factory.getJsonSchema(asJsonNode(getMetaSchema))
    schema.validate(asJsonNode(json))

    // get pieces of config
    implicit val formats = DefaultFormats
    json.extract[Config]
  }

  def sendStreamingEvents(spark: SparkSession, opts: Opts, apiKey: String): Unit = {
    val config = readConfigFile(opts.configFilePath())
    val httpSink = new HttpSink(opts.url(), Map("api_key" -> apiKey))()

    val pings = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", opts.kafkaBroker())
      .option("failOnDataLoss", opts.failOnDataLoss())
      .option("kafka.max.partition.fetch.bytes", 8 * 1024 * 1024) // 8MB
      .option("spark.streaming.kafka.consumer.cache.maxCapacity", kafkaCacheMaxCapacity)
      .option("subscribe", TelemetryKafkaTopic)
      .option("startingOffsets", opts.startingOffsets())
      .load()

    getEvents(config, pings.select("value"), opts.sample(), opts.raiseOnError())
      .writeStream
      .queryName(QueryName)
      .foreach(httpSink)
      .start()
      .awaitTermination()
  }

  def sendBatchEvents(spark: SparkSession, opts: Opts): Unit = {
    val config = readConfigFile(opts.configFilePath())

    val maxParallelRequests = opts.maxParallelRequests()

    implicit val sc = spark.sparkContext

    datesBetween(opts.from(), opts.to.get).foreach { currentDate =>
      val dataset = com.mozilla.telemetry.heka.Dataset("telemetry")

      val pings = config.getBatchFilters.filter{
          case(name, _) => TOP_LEVEL_PING_FIELDS.contains(name)
        }.foldLeft(dataset){
          case(d, (key, values)) => d.where(key) {
            case v if values.contains(v) => true
          }
        }.where("submissionDate") {
          case date if date == currentDate => true
        }.records(opts.fileLimit.get, Some(maxParallelRequests))
         .map(m => Row(m.toByteArray))

      val schema = StructType(List(
          StructField("value", BinaryType, true)
      ))

      val pingsDataFrame = spark.createDataFrame(pings, schema)
      val apiKey = sys.env(AMPLITUDE_API_KEY_KEY)
      val minDelay = opts.minDelay()
      val url = opts.url()

      getEvents(config, pingsDataFrame, opts.sample(), opts.raiseOnError())
        .repartition(maxParallelRequests)
        .foreachPartition{ it: Iterator[String] =>
          val httpSink = new HttpSink(url, Map("api_key" -> apiKey))()
          it.foreach{ event =>
            httpSink.process(event)
            java.lang.Thread.sleep(minDelay)
          }
        }
    }

    spark.stop()
  }

  def process(opts: Opts, apiKey: String): Unit = {
    val spark = SparkSession.builder()
      .appName("EventsToAmplitude")
      .getOrCreate()

    opts.kafkaBroker.get match {
      case Some(_) => sendStreamingEvents(spark, opts, apiKey)
      case None => sendBatchEvents(spark, opts)
    }
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    val apiKey = sys.env(AMPLITUDE_API_KEY_KEY)

    process(opts, apiKey)
  }
}
