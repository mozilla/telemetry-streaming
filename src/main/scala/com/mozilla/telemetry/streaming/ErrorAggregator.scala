// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry.streaming

import java.sql.{Date, Timestamp}

import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.pings._
import com.mozilla.telemetry.timeseries._
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.sql.functions.{sum, window}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.json4s._
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.joda.time.DateTime


object ErrorAggregator {

  private val allowedDocTypes = List("main", "crash")
  private val outputPrefix = "error_aggregates/v1"

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val kafkaBroker: ScallopOption[String] = opt[String](
      "kafkaBroker",
      descr = "Kafka broker (streaming mode only)",
      required = false)
    val from: ScallopOption[String] = opt[String](
      "from",
      descr = "Start submission date (batch mode only). Format: YYYYMMDD",
      required = false)
    val to: ScallopOption[String] = opt[String](
      "to",
      descr = "End submission date (batch mode only). Default: yesterday. Format: YYYYMMDD",
      required = false)
    val fileLimit: ScallopOption[Int] = opt[Int](
      "fileLimit",
      descr = "Max number of files to retrieve (batch mode only). Default: All files",
      required = false)
    val outputPath:ScallopOption[String] = opt[String](
      "outputPath",
      descr = "Output path",
      required = false,
      default = Some("/tmp/parquet"))
    val raiseOnError:ScallopOption[Boolean] = opt[Boolean](
      "raiseOnError",
      descr = "Whether to program should exit on a data processing error or not.")
    val failOnDataLoss:ScallopOption[Boolean] = opt[Boolean](
      "failOnDataLoss",
      descr = "Whether to fail the query when it’s possible that data is lost.")
    requireOne(kafkaBroker, from)
    conflicts(kafkaBroker, List(from, to, fileLimit))
    verify()
  }

  private val countHistogramErrorsSchema = new SchemaBuilder()
    .add[Int]("BROWSER_SHIM_USAGE_BLOCKED")
    .add[Int]("PERMISSIONS_SQL_CORRUPTED")
    .add[Int]("DEFECTIVE_PERMISSIONS_SQL_REMOVED")
    .add[Int]("SLOW_SCRIPT_NOTICE_COUNT")
    .add[Int]("SLOW_SCRIPT_PAGE_COUNT")
    .build

  private val dimensionsSchema = new SchemaBuilder()
    .add[Timestamp]("timestamp")  // Windowed
    .add[Date]("submission_date")
    .add[String]("channel")
    .add[String]("version")
    .add[String]("build_id")
    .add[String]("application")
    .add[String]("os_name")
    .add[String]("os_version")
    .add[String]("architecture")
    .add[String]("country")
    .add[String]("experiment_id")
    .add[String]("experiment_branch")
    .add[Boolean]("e10s_enabled")
    .add[String]("e10s_cohort")
    .add[String]("gfx_compositor")
    .build

  private val metricsSchema = new SchemaBuilder()
    .add[Float]("usage_hours")
    .add[Int]("count")
    .add[Int]("main_crashes")
    .add[Int]("content_crashes")
    .add[Int]("gpu_crashes")
    .add[Int]("plugin_crashes")
    .add[Int]("gmplugin_crashes")
    .add[Int]("content_shutdown_crashes")
    .build

  private val statsSchema = SchemaBuilder.merge(metricsSchema, countHistogramErrorsSchema)

  private[streaming] def aggregate(pings: DataFrame, raiseOnError: Boolean = false, online: Boolean = true): DataFrame = {
    import pings.sparkSession.implicits._

    // A custom row encoder is needed to use Rows within a Spark Dataset
    val mergedSchema = SchemaBuilder.merge(dimensionsSchema, statsSchema)
    implicit val rowEncoder = RowEncoder(mergedSchema).resolveAndBind()
    implicit val optEncoder = ExpressionEncoder.tuple(rowEncoder)

    var parsedPings = pings
      .flatMap( v => {
        try {
          parsePing(Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]]))
        } catch {
          case _: Throwable if !raiseOnError => Array[Row]()
        }
      })

    val dimensions = List(window($"timestamp", "5 minute")) ++ dimensionsSchema.fieldNames
      .filter(_ != "timestamp")
      .map(new ColumnName(_))

    val stats = for {
      fieldName <- statsSchema.fieldNames
      normFieldName = fieldName.toLowerCase
    } yield {
      sum(normFieldName).alias(normFieldName)
    }

    if (online) {
      parsedPings = parsedPings.withWatermark("timestamp", "1 minute")
    }

    parsedPings
      .groupBy(dimensions:_*)
      .agg(stats(0), stats.drop(1):_*)
      .coalesce(1)
  }
  private def buildDimensions(meta: Meta): Row = {
    val dimensions = new RowBuilder(dimensionsSchema)
    dimensions("timestamp") = Some(meta.normalizedTimestamp())
    dimensions("submission_date") = Some(new Date(meta.normalizedTimestamp().getTime))
    dimensions("channel") = Some(meta.normalizedChannel)
    dimensions("version") = meta.`environment.build`.flatMap(_.version)
    dimensions("build_id") = meta.`environment.build`.flatMap(_.buildId)
    dimensions("application") = Some(meta.appName)
    dimensions("os_name") = meta.`environment.system`.map(_.os.name)
    dimensions("os_version") = meta.`environment.system`.map(_.os.version)
    dimensions("architecture") = meta.`environment.build`.flatMap(_.architecture)
    dimensions("country") = Some(meta.geoCountry)
    dimensions("experiment_id") = for {
      addons <- meta.`environment.addons`
      experiment <- addons.activeExperiment
    } yield experiment.id
    dimensions("experiment_branch") = for {
      addons <- meta.`environment.addons`
      experiment <- addons.activeExperiment
    } yield experiment.branch
    dimensions("e10s_enabled") = meta.`environment.settings`.flatMap(_.e10sEnabled)
    dimensions("e10s_cohort") = meta.`environment.settings`.flatMap(_.e10sCohort)
    dimensions("gfx_compositor") = for {
      system <- meta.`environment.system`
      gfx <- system.gfx
      features <- gfx.features
      compositor <- features.compositor
    } yield compositor
    dimensions.build
  }

  class ErrorAggregatorCrashPing(ping: CrashPing) {
    def parse(): Array[Row] = {
      // Non-main crashes are already retrieved from main pings
      if(!ping.isMainCrash) throw new Exception("Only Crash pings of type `main` are allowed")
      val dimensions = buildDimensions(ping.meta)
      val stats = new RowBuilder(statsSchema)
      stats("count") = Some(1)
      stats("main_crashes") = Some(1)
      Array(RowBuilder.merge(dimensions, stats.build))
    }
  }
  implicit def errorAggregatorCrashPing(ping: CrashPing): ErrorAggregatorCrashPing = new ErrorAggregatorCrashPing(ping)

  class ErrorAggregatorMainPing(ping: MainPing) {
    def parse(): Array[Row] = {
      // If a main ping has no usage hours discard it.
      val usageHours = ping.usageHours
      if (usageHours.isEmpty) throw new Exception("Main pings should have a  number of usage hours != 0")

      val dimensions = buildDimensions(ping.meta)
      val stats = new RowBuilder(statsSchema)
      stats("count") = Some(1)
      stats("usage_hours") = usageHours
      countHistogramErrorsSchema.fieldNames.foreach(stats_name => {
        stats(stats_name) = ping.getCountHistogramValue(stats_name)
      })
      stats("content_crashes") = ping.getCountKeyedHistogramValue("SUBPROCESS_CRASHES_WITH_DUMP", "content")
      stats("gpu_crashes") = ping.getCountKeyedHistogramValue("SUBPROCESS_CRASHES_WITH_DUMP", "gpu")
      stats("plugin_crashes") = ping.getCountKeyedHistogramValue("SUBPROCESS_CRASHES_WITH_DUMP", "plugin")
      stats("gmplugin_crashes") = ping.getCountKeyedHistogramValue("SUBPROCESS_CRASHES_WITH_DUMP", "gmplugin")
      stats("content_shutdown_crashes") = ping.getCountKeyedHistogramValue("SUBPROCESS_KILL_HARD", "ShutDownKill")

      Array(RowBuilder.merge(dimensions, stats.build))
    }
  }
  implicit def errorAggregatorMainPing(ping: MainPing): ErrorAggregatorMainPing = new ErrorAggregatorMainPing(ping)

  /*
   * We can't use an Option[Row] because entire rows cannot be null in Spark SQL.
   * The best we can do is to resort to use a container, e.g. Array.
   * This will also give us the ability to parse more than one row from the same ping.
   */
  def parsePing(message: Message): Array[Row] = {
    implicit val formats = DefaultFormats

    val fields = message.fieldsAsMap
    val docType = fields.getOrElse("docType", "").asInstanceOf[String]
    if (!allowedDocTypes.contains(docType)) {
      throw new Exception("Doctype should be one of " + allowedDocTypes.mkString(sep = ","))
    }
    if(docType == "crash") {
      messageToCrashPing(message).parse()
    } else {
      messageToMainPing(message).parse()
    }
  }

  def writeStreamingAggregates(spark: SparkSession, opts: Opts): Unit = {
    val pings = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", opts.kafkaBroker())
      .option("failOnDataLoss", opts.failOnDataLoss())
      .option("kafka.max.partition.fetch.bytes", 8 * 1024 * 1024) // 8MB
      .option("spark.streaming.kafka.consumer.cache.maxCapacity", 1000)
      .option("subscribe", "telemetry")
      .option("startingOffsets", "latest")
      .load()

    val outputPath = opts.outputPath()

    aggregate(pings.select("value"), raiseOnError = opts.raiseOnError())
      .writeStream
      .format("parquet")
      .option("path", s"${outputPath}/${outputPrefix}")
      .option("checkpointLocation", "/tmp/checkpoint")
      .partitionBy("submission_date")
      .start()
      .awaitTermination()
  }

  def writeBatchAggregates(spark: SparkSession, opts: Opts): Unit = {
    val from = opts.from()
    val to = opts.to.get match {
      case Some(t) => t
      case _ => DateTime.now.minusDays(1).toString("yyyyMMdd")
    }

    implicit val sc = spark.sparkContext
    val pings = Dataset("telemetry")
      .where("sourceName") {
        case "telemetry" => true
      }.where("sourceVersion") {
        case "4" => true
      }.where("docType") {
        case docType if allowedDocTypes.contains(docType) => true
      }.where("appName") {
        case "Firefox" => true
      }.where("submissionDate") {
        case date if from <= date && date <= to => true
      }.records(opts.fileLimit.get)
      .map(m => Row(m.toByteArray))

    val schema = StructType(List(
        StructField("value", BinaryType, true)
    ))

    val pingsDataframe = spark.createDataFrame(pings, schema)
    val outputPath = opts.outputPath()

    aggregate(pingsDataframe, raiseOnError = opts.raiseOnError(), online = false)
      .write
      .partitionBy("submission_date")
      .parquet(s"${outputPath}/${outputPrefix}")
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val spark = SparkSession.builder()
      .appName("Error Aggregates")
      .master("local[*]")
      .getOrCreate()

    opts.kafkaBroker.get match {
      case Some(broker) => writeStreamingAggregates(spark, opts)
      case None => writeBatchAggregates(spark, opts)
    }
  }
}
