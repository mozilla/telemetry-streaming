package com.mozilla.telemetry.streaming

import java.sql.{Date, Timestamp}

import com.mozilla.telemetry.heka.{Dataset, Message}
import com.mozilla.telemetry.pings.{CrashPing, MainPing, Meta}
import com.mozilla.telemetry.timeseries._
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.sql.functions.{sum, window}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.json4s._
import org.json4s.jackson.JsonMethods._
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
    .build

  private val metricsSchema = new SchemaBuilder()
    .add[Float]("usage_hours")
    .add[Int]("count")
    .add[Int]("main_crashes")
    .build

  private val statsSchema = SchemaBuilder.merge(metricsSchema, countHistogramErrorsSchema)

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

    // A custom row encoder is needed to use Rows within a Spark Dataset
    val mergedSchema = SchemaBuilder.merge(dimensionsSchema, statsSchema)
    implicit val rowEncoder = RowEncoder(mergedSchema).resolveAndBind()
    implicit val optEncoder = ExpressionEncoder.tuple(rowEncoder)

    var parsedPings = pings
      .map { case v =>
        try {
          parsePing(Message.parseFrom(v.get(0).asInstanceOf[Array[Byte]]))
        } catch {
          case _: Throwable if !raiseOnError => Tuple1(null)
        }
      }
      .filter(_._1 != null)
      .map(_._1)

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
    dimensions("timestamp") = Some(new Timestamp(meta.Timestamp / 1000000))
    dimensions("submission_date") = Some(new Date(meta.Timestamp / 1000000))
    dimensions("channel") = Some(meta.normalizedChannel)
    dimensions("version") = meta.`environment.build`.flatMap(_.version)
    dimensions("build_id") = meta.`environment.build`.flatMap(_.buildId)
    dimensions("application") = Some(meta.appName)
    dimensions("os_name") = meta.`environment.system`.map(_.os.name)
    dimensions("os_version") = meta.`environment.system`.map(_.os.version)
    dimensions("architecture") = meta.`environment.build`.flatMap(_.architecture)
    dimensions("country") = Some(meta.geoCountry)
    dimensions.build
  }

  private def parseCrashPing(ping: CrashPing): Tuple1[Row] = {
    // Non-main crashes are already retrieved from main pings
    if(!ping.isMain()) return Tuple1(null)

    val dimensions = buildDimensions(ping.meta)
    val stats = new RowBuilder(statsSchema)
    stats("count") = Some(1)
    stats("main_crashes") = Some(1)
    Tuple1(RowBuilder.merge(dimensions, stats.build))
  }

  private def parseMainPing(ping: MainPing): Tuple1[Row] = {
    // If a main ping has no usage hours discard it.
    val usageHours = ping.usageHours()
    if (usageHours.isEmpty) return Tuple1(null)

    val dimensions = buildDimensions(ping.meta)
    val stats = new RowBuilder(statsSchema)
    stats("count") = Some(1)
    stats("usage_hours") = usageHours
    countHistogramErrorsSchema.fieldNames.foreach(stats_name => {
      stats(stats_name) = Some(ping.getCountHistogramValue(stats_name))
    })
    Tuple1(RowBuilder.merge(dimensions, stats.build))
  }
  /*
   We can't use an Option[Row] because entire rows cannot be null in Spark SQL. The best we can do is to resort to Tuple1[Row].
   See https://github.com/apache/spark/blob/38b9e69623c14a675b14639e8291f5d29d2a0bc3/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/encoders/ExpressionEncoder.scala#L53
   */
  def parsePing(message: Message): Tuple1[Row] = {
    implicit val formats = DefaultFormats

    val fields = message.fieldsAsMap
    val docType = fields.getOrElse("docType", "").asInstanceOf[String]
    if (!allowedDocTypes.contains(docType)) {
      return Tuple1(null)
    }
    if(docType == "crash") {
      parseCrashPing(messageToCrashPing(message))
    } else {
      parseMainPing(messageToMainPing(message))
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
