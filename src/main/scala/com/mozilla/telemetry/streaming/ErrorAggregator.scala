package com.mozilla.telemetry.streaming

import java.sql.{Date, Timestamp}

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.timeseries._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.rogach.scallop.{ScallopConf, ScallopOption}


private case class EnvironmentBuild(version: Option[String],
                            buildId: Option[String],
                            architecture: Option[String])

private case class EnvironmentSystem(os: OS)

private case class OS(name: Option[String],
              version: Option[String])

private case class PayloadInfo(subsessionLength: Option[Int])

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
    verify()
  }

  private val countHistogramErrorsSchema = new SchemaBuilder()
    .add[Int]("BROWSER_SHIM_USAGE_BLOCKED")
    .add[Int]("PERMISSIONS_SQL_CORRUPTED")
    .add[Int]("DEFECTIVE_PERMISSIONS_SQL_REMOVED")
    .add[Int]("SLOW_SCRIPT_NOTICE_COUNT")
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
    .add[Float]("usageHours")
    .add[Int]("count")
    .add[Int]("crashes")
    .build

  private val statsSchema = SchemaBuilder.merge(metricsSchema, countHistogramErrorsSchema)

  private def getCountHistogramValue(histogram: JValue): Int = {
    try {
      histogram \ "values" \ "0" match {
        case JInt(count) => count.toInt
        case _ => 0
      }
    } catch { case _: Throwable => 0 }
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

  /*
   We can't use an Option[Row] because entire rows cannot be null in Spark SQL. The best we can do is to resort to Tuple1[Row].
   See https://github.com/apache/spark/blob/38b9e69623c14a675b14639e8291f5d29d2a0bc3/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/encoders/ExpressionEncoder.scala#L53
   */
  def parsePing(message: Message): Tuple1[Row] = {
    implicit val formats = DefaultFormats

    val ping = message.fieldsAsMap

    val docType = ping.getOrElse("docType", "").asInstanceOf[String]
    if (!List("main", "crash").contains(docType)) {
      return Tuple1(null)
    }

    val environmentBuild = parse(ping.getOrElse("environment.build", "{}")
      .asInstanceOf[String])
      .extract[EnvironmentBuild]

    val environmentSystem = parse(ping.getOrElse("environment.system", "{}")
      .asInstanceOf[String])
      .extract[EnvironmentSystem]

    val payloadInfo = parse(ping.getOrElse("payload.info", "{}")
      .asInstanceOf[String])
      .extract[PayloadInfo]

    val keyedHistograms = parse(ping.getOrElse("payload.histograms", "{}")
      .asInstanceOf[String]
    )

    val application = ping.get("appName").asInstanceOf[Option[String]]
    val channel = ping.get("normalizedChannel").asInstanceOf[Option[String]]
    val country = ping.get("geoCountry").asInstanceOf[Option[String]]

    val dimensions = new RowBuilder(dimensionsSchema)
    dimensions("timestamp") = Some(new Timestamp(message.timestamp / 1000000))
    dimensions("submission_date") = Some(new Date(message.timestamp / 1000000))
    dimensions("channel") = channel
    dimensions("version") = environmentBuild.version
    dimensions("build_id") = environmentBuild.buildId
    dimensions("application") = application
    dimensions("os_name") = environmentSystem.os.name
    dimensions("os_version") = environmentSystem.os.version
    dimensions("architecture") = environmentBuild.architecture
    dimensions("country") = country

    val stats = new RowBuilder(statsSchema)
    if (docType == "main") {
      assert(payloadInfo.subsessionLength.isDefined)

      val sessionLength = payloadInfo.subsessionLength.get.toFloat
      stats("usageHours") = Some(Math.min(25, Math.max(0, sessionLength / 3600)))
      stats("count") = Some(1)

      countHistogramErrorsSchema.fieldNames.foreach(key => {
        stats(key) = Some(getCountHistogramValue(keyedHistograms \ key))
      })
    } else {
      stats("crashes") = Some(1)
    }

    Tuple1(RowBuilder.merge(dimensions.build, stats.build))
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
      .option("kafka.max.partition.fetch.bytes", 8 * 1024 * 1024) // 8MB
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