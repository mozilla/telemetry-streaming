/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */
package com.mozilla.telemetry.streaming

import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.pings.{CorePing, CrashPing, MainPing, Ping}
import com.mozilla.telemetry.streaming.StreamingJobBase.TelemetryKafkaTopic
import com.mozilla.telemetry.timeseries.{RowBuilder, SchemaBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, sum, window}
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.rogach.scallop.ScallopOption

abstract class ErrorAggregatorBase extends StreamingJobBase {

  val countHistogramErrorsSchema: StructType
  val dimensionsSchema: StructType
  val metricsSchema: StructType

  val defaultNumFiles = 60

  //TODO: make relationships visible here - we're adding crash/Fennec and core/Fennec/Android
  private val allowedDocTypes = List("main")
  private val allowedAppNames = List("Firefox", "Fennec")
  private val coreFennecPingAllowedOses = List("Android")
  private val disallowedChannels = List("Other")
  private val kafkaCacheMaxCapacity = 1000

  def parse(ping: CrashPing, dimensionsSchema: StructType, statsSchema: StructType): Array[Row] = {
    if (ping.isMainCrash || ping.isContentCrash) {

      val dimensions = buildDimensions(dimensionsSchema, ping)
      val stats = new RowBuilder(SchemaBuilder.merge(statsSchema))

      stats("count") = Some(1)
      stats("client_id") = ping.meta.clientId

      if (ping.isMainCrash) {
        stats("main_crashes") = Some(1)
        stats("startup_crashes") = if (ping.isStartupCrash) Some(1) else None
      } else {
        if (ping.isContentShutdownCrash) {
          stats("content_shutdown_crashes") = Some(1)
        } else {
          stats("content_crashes") = Some(1)
        }
      }

      dimensions.map(RowBuilder.merge(_, stats.build))
    } else {
      // Non- main and content crashes are already retrieved from main pings
      throw new Exception("Only Crash pings of type `main` and `content` are allowed")
    }
  }

  def parse(ping: MainPing, dimensionsSchema: StructType, statsSchema: StructType, countHistograms: StructType): Array[Row] = {
    // If a main ping has no usage hours discard it.
    val usageHours = ping.usageHours
    if (usageHours.isEmpty) throw new Exception("Main pings should have a  number of usage hours != 0")

    val dimensions = buildDimensions(dimensionsSchema, ping)
    val stats = new RowBuilder(SchemaBuilder.merge(statsSchema))
    stats("count") = Some(1)
    stats("client_id") = ping.meta.clientId
    stats("usage_hours") = usageHours
    countHistograms.fieldNames.foreach(stats_name => {
      stats(stats_name) = ping.getCountHistogramValue(stats_name)
    })
    stats("gpu_crashes") = ping.getCountKeyedHistogramValue("SUBPROCESS_CRASHES_WITH_DUMP", "gpu")
    stats("plugin_crashes") = ping.getCountKeyedHistogramValue("SUBPROCESS_CRASHES_WITH_DUMP", "plugin")
    stats("gmplugin_crashes") = ping.getCountKeyedHistogramValue("SUBPROCESS_CRASHES_WITH_DUMP", "gmplugin")

    dimensions.map(RowBuilder.merge(_, stats.build))
  }

  def parse(ping: CorePing, dimensionsSchema: StructType, statsSchema: StructType): Array[Row] = {
    val dimensions = buildDimensions(dimensionsSchema, ping)
    val stats = new RowBuilder(SchemaBuilder.merge(statsSchema))
    stats("count") = Some(1)
    stats("client_id") = ping.meta.clientId
    stats("usage_hours") = ping.usageHours

    dimensions.map(RowBuilder.merge(_, stats.build))
  }

  def parsePing(dimensions: StructType, statsSchema: StructType, countHistograms: StructType)(message: Message): Array[Row] = {

    val fields = message.fieldsAsMap
    val docType = fields.getOrElse("docType", "").asInstanceOf[String]
    if (!allowedDocTypes.contains(docType)) {
      throw new Exception("Doctype should be one of " + allowedDocTypes.mkString(sep = ","))
    }

    val appName = fields.getOrElse("appName", "").asInstanceOf[String]
    if (!allowedAppNames.contains(appName)) {
      throw new Exception("AppName should be one of " + allowedAppNames.mkString(sep = ","))
    }

    val channel = fields.getOrElse("normalizedChannel", "").asInstanceOf[String]
    if (disallowedChannels.contains(channel)) {
      throw new Exception("Channel can't be one of " + disallowedChannels.mkString(sep = ","))
    }

    if (docType == "crash") {
      val crashPing = CrashPing(message)
      if (crashPing.getNormalizedBuildId.isEmpty) {
        throw new Exception("Empty buildId")
      }
      parse(crashPing, dimensions, statsSchema)
    } else if (docType == "core") {
      val corePing = CorePing(message)
      if (!coreFennecPingAllowedOses.contains(corePing.os)) {
        throw new Exception("OS for core/Fennec pings should be one of: " + coreFennecPingAllowedOses.mkString(sep = ","))
      }
      if (corePing.getNormalizedBuildId.isEmpty) {
        throw new Exception("Empty buildId")
      }
      parse(corePing, dimensions, statsSchema)
    } else {
      val mainPing = MainPing(message)
      if (mainPing.getNormalizedBuildId.isEmpty) {
        throw new Exception("Empty buildId")
      }
      parse(mainPing, dimensions, statsSchema, countHistograms)
    }
  }

  def writeStreamingAggregates(spark: SparkSession, opts: Opts): Unit = {
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

    val outputPath = opts.outputPath()

    aggregate(pings.select("value"), raiseOnError = opts.raiseOnError())
      .repartition(1)
      .writeStream
      .queryName(QueryName)
      .format("parquet")
      .option("path", s"${outputPath}/${outputPrefix}")
      .option("checkpointLocation", opts.checkpointPath())
      .partitionBy("submission_date_s3")
      .start()
      .awaitTermination()
  }

  def writeBatchAggregates(spark: SparkSession, opts: Opts): Unit = {
    datesBetween(opts.from(), opts.to.get).foreach { currentDate =>
      val pings = getPingsForDate(spark, opts.fileLimit.get, opts.channel.get, currentDate)

      val schema = StructType(List(
        StructField("value", BinaryType, true)
      ))

      val pingsDataframe = spark.createDataFrame(pings, schema)
      val outputPath = opts.outputPath()

      aggregate(pingsDataframe, raiseOnError = opts.raiseOnError())
        .repartition(opts.numParquetFiles())
        .write
        .mode("overwrite")
        .partitionBy("submission_date_s3")
        .parquet(s"${outputPath}/${outputPrefix}")
    }

    if (shouldStopContextAtEnd(spark)) {
      spark.stop()
    }
  }

  private def getPingsForDate(spark: SparkSession, fileLimit: Option[Int], filterChannel: Option[String], currentDate: String): RDD[Row] = {
    implicit val sc = spark.sparkContext

    Dataset("telemetry")
      .where("sourceName") {
        case "telemetry" => true
      }.where("docType") {
      case docType if allowedDocTypes.contains(docType) => true
      }.where("appName") {
        case appName if allowedAppNames.contains(appName) => true
      }.where("submissionDate") {
        case date if date == currentDate => true
      }.where("appUpdateChannel") {
        case channel => filterChannel.isEmpty || channel == filterChannel.get
      }.records(fileLimit)
      .map(m => Row(m.toByteArray))
  }

  def run(args: Array[String]): Unit = {
    val opts = new Opts(args)

    val spark = SparkSession.builder()
      .appName("Error Aggregates")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    require(spark.version >= "2.3", "Spark 2.3 is required due to dynamic partition overwrite mode")

    opts.kafkaBroker.get match {
      case Some(_) => writeStreamingAggregates(spark, opts)
      case None => writeBatchAggregates(spark, opts)
    }
  }

  def main(args: Array[String]): Unit = run(args)

  private def buildStatsSchema(metrics: StructType, countHistograms: StructType): StructType = SchemaBuilder.merge(metrics, countHistograms)

  private[streaming] def aggregate(pings: DataFrame, raiseOnError: Boolean = false): DataFrame = {
    import pings.sparkSession.implicits._

    // A custom row encoder is needed to use Rows within a Spark Dataset
    val statsSchema = buildStatsSchema(metricsSchema, countHistogramErrorsSchema)
    val mergedSchema = SchemaBuilder.merge(dimensionsSchema, statsSchema)
    implicit val rowEncoder = RowEncoder(mergedSchema).resolveAndBind()

    val parseMessage = parsePing(dimensionsSchema, statsSchema, countHistogramErrorsSchema) _
    val parsedPings = pings
      .flatMap(v => {
        try {
          parseMessage(Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]]))
        } catch {
          case _: Throwable if !raiseOnError => Array[Row]()
        }
      })

    val dimensionsCols = List(
      window($"timestamp", "5 minute").as("window"),
      col("window.start").as("window_start"),
      col("window.end").as("window_end")
    ) ++ dimensionsSchema.fieldNames.filter(_ != "timestamp").map(col(_))

    val stats = statsSchema.fieldNames.map(_.toLowerCase)
    val aggCols = stats.map(s => sum(s).as(s))

    /*
    * The resulting DataFrame will contain the grouping columns + the columns aggregated.
    * Everything else gets dropped by .agg()
    * */
    parsedPings
      .withWatermark("timestamp", "1 minute")
      .groupBy(dimensionsCols: _*)
      .agg(aggCols.head, aggCols.tail: _*)
      .drop("window")
  }

  private def buildDimensions(dimensionsSchema: StructType, ping: Ping): Array[Row] = {
    val meta = ping.meta

    val experiments = ping.getExperiments

    experiments.map { case (experiment_id, experiment_branch) =>
      val dimensions = new RowBuilder(dimensionsSchema)
      dimensions("timestamp") = Some(meta.normalizedTimestamp())
      dimensions("submission_date_s3") = Some(timestampToDateString(meta.normalizedTimestamp))
      dimensions("channel") = Some(meta.normalizedChannel)
      dimensions("version") = ping.getVersion
      dimensions("display_version") = ping.getDisplayVersion
      dimensions("build_id") = ping.getNormalizedBuildId
      dimensions("application") = Some(meta.appName)
      dimensions("os_name") = ping.getOsName
      dimensions("os_version") = ping.getOsVersion
      dimensions("architecture") = ping.getArchitecture
      dimensions("country") = Some(meta.geoCountry)
      dimensions("experiment_id") = experiment_id
      dimensions("experiment_branch") = experiment_branch
      dimensions.build
    }
  }

  private class Opts(args: Array[String]) extends BaseOpts(args) {
    val outputPath: ScallopOption[String] = opt[String](
      "outputPath",
      descr = "Output path",
      required = false,
      default = Some("/tmp/parquet"))
    val raiseOnError: ScallopOption[Boolean] = opt[Boolean](
      "raiseOnError",
      descr = "Whether the program should exit on a data processing error or not.")
    val failOnDataLoss: ScallopOption[Boolean] = opt[Boolean](
      "failOnDataLoss",
      descr = "Whether to fail the query when it’s possible that data is lost.")
    val numParquetFiles: ScallopOption[Int] = opt[Int](
      "numParquetFiles",
      descr = "Number of parquet files per submission_date_s3 (batch mode only)",
      required = false,
      default = Some(defaultNumFiles))
    val channel: ScallopOption[String] = opt[String](
      "channel",
      descr = "Only process data from the given channel (batch mode only)",
      required = false)

    requireOne(kafkaBroker, from)
    conflicts(kafkaBroker, List(from, to, fileLimit, channel, numParquetFiles))
    verify()
  }

}
