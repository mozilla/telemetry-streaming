package com.mozilla.telemetry.streaming

import java.sql.{Date, Timestamp}

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.pings.{CrashPing, MainPing}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.window
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.rogach.scallop.{ScallopConf, ScallopOption}


case class AggregateDimensions(
                                submission_date: Date,
                                channel: String,
                                version: Double,
                                build_id: String,
                                application: String,
                                os_name: String,
                                os_version: String,
                                architecture: String,
                                country: String
                              )

case class CrashStats(main_crashes: Long=0){

  def +(that: CrashStats): CrashStats =
    new CrashStats(this.main_crashes + that.main_crashes)
}

case class ErrorStats(
                       browser_shim_usage_blocked: Long=0,
                       permissions_sql_corrupted: Long=0,
                       defective_permissions_sql_removed: Long=0,
                       slow_script_notice_count: Long=0,
                       slow_script_page_count :Long=0
                     ){
  def this(ping: MainPing) = this(
    ping.getCountHistogramValue("browser_shim_usage_blocked"),
    ping.getCountHistogramValue("permissions_sql_corrupted"),
    ping.getCountHistogramValue("defective_permissions_sql_removed"),
    ping.getCountHistogramValue("slow_script_notice_count"),
    ping.getCountHistogramValue("slow_script_page_count")
  )

  def +(that: ErrorStats): ErrorStats =
    new ErrorStats(
      this.browser_shim_usage_blocked + that.browser_shim_usage_blocked,
      this.permissions_sql_corrupted + that.permissions_sql_corrupted,
      this.defective_permissions_sql_removed + that.defective_permissions_sql_removed,
      this.slow_script_notice_count + that.slow_script_notice_count,
      this.slow_script_page_count + that.slow_script_page_count
    )
}

case class PingAggregate(
                          dimensions: AggregateDimensions,
                          crashes: CrashStats,
                          errors: ErrorStats,
                          usage_hours: Float = 0,
                          size: Long = 1,
                          timestamp: Timestamp,
                          window_start: Timestamp,
                          window_end: Timestamp
                        ){
  def this(ping: CrashPing) = this(
    AggregateDimensions(
      new Date(ping.meta.Timestamp / 1000000),
      ping.meta.normalizedChannel,
      ping.meta.appVersion,
      ping.meta.appBuildId,
      ping.meta.appName,
      ping.meta.os,
      ping.meta.`environment.system`.os.version,
      ping.application.architecture,
      ping.meta.geoCountry
    ),
    new CrashStats(1),
    new ErrorStats(),
    timestamp = ping.timestamp,
    window_start = ping.timestamp,
    window_end = ping.timestamp

  )

  def this(ping: MainPing) = this(
    AggregateDimensions(
      new Date(ping.meta.Timestamp / 1000000),
      ping.meta.normalizedChannel,
      ping.meta.appVersion,
      ping.meta.appBuildId,
      ping.meta.appName,
      ping.meta.os,
      ping.meta.`environment.system`.os.version,
      ping.application.architecture,
      ping.meta.geoCountry
    ),
    new CrashStats(),
    new ErrorStats(ping),
    ping.usageHours,
    timestamp = ping.timestamp,
    window_start = ping.timestamp,
    window_end = ping.timestamp
  )

  def +(that: PingAggregate): PingAggregate = {
    // When summing pings, they should always belong to the same window
    assert(
      this.dimensions == that.dimensions &&
      this.window_start == that.window_start &&
      this.window_end == that.window_end
    )
    new PingAggregate(
      this.dimensions,
      this.crashes + that.crashes,
      this.errors + that.errors,
      this.usage_hours + that.usage_hours,
      this.size + that.size,
      // if an aggregate represents more than a ping,
      // timestamp = window_start for consistency
      this.window_start,
      this.window_start,
      this.window_end
    )
  }
}

object ErrorAggregator {
  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val kafkaBroker: ScallopOption[String] = opt[String](
      "kafkaBroker",
      descr = "From submission date",
      required = true)
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
    verify()
  }

  def messageToCrashPing(message: Message): CrashPing = {
    implicit val formats = DefaultFormats
    val jsonFieldNames = List(
      "environment.build",
      "environment.settings",
      "environment.system",
      "environment.profile",
      "environment.addons"
    )
    val ping = messageToPing(message, jsonFieldNames)
    ping.extract[CrashPing]
  }

  def messageToMainPing(message: Message): MainPing = {
    implicit val formats = DefaultFormats
    val jsonFieldNames = List(
      "environment.build",
      "environment.settings",
      "environment.system",
      "environment.profile",
      "environment.addons",
      "payload.simpleMeasurements",
      "payload.keyedHistograms",
      "payload.histograms",
      "payload.info"
    )
    val ping = messageToPing(message, jsonFieldNames)
    ping.extract[MainPing]
  }

  def messageToPing(message:Message, jsonFieldNames: List[String]): JValue = {
    implicit val formats = DefaultFormats
    val fields = message.fieldsAsMap
    val jsonObj = Extraction.decompose(fields)
    // Transform json fields into JValues
    val meta = jsonObj transformField {
      case JField(key, JString(s)) if jsonFieldNames contains key => (key, parse(s))
    }
    val submission = if(message.payload.isDefined) message.payload else fields.get("submission")
    submission match {
      case Some(value: String) => parse(value) ++ JObject(List(JField("meta", meta)))
      case _ => JObject()
    }
  }

  private[streaming] def aggregate(pings: DataFrame, raiseOnError: Boolean = false, online: Boolean = true): DataFrame = {
    import pings.sparkSession.implicits._
    var parsedPings = pings
      .flatMap { case v =>
        try {
          val message = Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]])
          message.fieldsAsMap.get("docType") match {
            case Some("main") => Some(new PingAggregate(messageToMainPing(message)))
            case Some("crash") => {
             val ping = messageToCrashPing(message)
              // Non-main crashes are reported both in main pings and in crash pings.
              // To keep compatibility with old clients discard non-main crash pings
              if(ping.isMain) Some(new PingAggregate(ping)) else None
            }
            case _ => None
          }
        } catch {
          case _: Throwable if !raiseOnError => None
        }
      }

    // Transform timestamp into a windowed time
    parsedPings = parsedPings.toDF()
      .withColumn("window", window($"timestamp", "5 minute"))
      .withColumn("window_start", new Column("window.start"))
      .withColumn("window_end", new Column("window.end"))
      .drop("window")
      .as[PingAggregate]

    if (online) {
      parsedPings = parsedPings.withWatermark("timestamp", "1 minute")
    }
    parsedPings.groupByKey(p => {
      p.dimensions.productIterator.mkString("-")+s"${p.window_start}-${p.window_end}"
    })
      .reduceGroups(_+_)
      .map(_._2)
      .coalesce(1)
      .select("dimensions.*", "crashes.*", "errors.*", "usage_hours", "size", "window_start", "window_end")
  }

  def main(args: Array[String]): Unit = {
    val opts = new Opts(args)
    val outputPath = opts.outputPath()
    val prefix = s"error_aggregates/v1"

    val spark = SparkSession.builder()
      .appName("Error Aggregates")
      .master("local[*]")
      .getOrCreate()

    val rawPings = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", opts.kafkaBroker())
      .option("failOnDataLoss", opts.failOnDataLoss())
      .option("kafka.max.partition.fetch.bytes", 8 * 1024 * 1024) // 8MB
      .option("spark.streaming.kafka.consumer.cache.maxCapacity", 1000)
      .option("subscribe", "telemetry")
      .option("startingOffsets", "latest")
      .load()

    aggregate(rawPings.select("value"), raiseOnError = opts.raiseOnError())
      .writeStream
      .format("parquet")
      .option("path", s"${outputPath}/${prefix}")
      .option("checkpointLocation", "/tmp/checkpoint")
      .partitionBy("submission_date")
      .start()
      .awaitTermination()
  }
}