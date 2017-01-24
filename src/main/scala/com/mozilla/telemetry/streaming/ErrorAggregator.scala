package com.mozilla.telemetry.streaming

import java.sql.Timestamp

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.timeseries._
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.rogach.scallop.{ScallopConf, ScallopOption}

case class EnvironmentBuild(version: Option[String],
                            buildId: Option[String],
                            architecture: Option[String])

case class EnvironmentSystem(os: OS)

case class OS(name: Option[String],
              version: Option[String])

case class PayloadInfo(subsessionLength: Option[Int])


object ErrorAggregator {

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val kafkaBroker: ScallopOption[String] = opt[String](
      "kafkaBroker",
      descr = "From submission date",
      required = true)
    val kafkaGroupId: ScallopOption[String] = opt[String](
      "kafkaGroupId",
      descr = "A unique string that identifies the consumer group this consumer belongs to",
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

  private val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingAggregator")

  private def getCountHistogramValue(histogram: JValue): Int = {
    try {
      histogram \ "values" \ "0" match {
        case JInt(count) => count.toInt
        case _ => 0
      }
    } catch { case _: Throwable => 0 }
  }

  private def parsePing(fields: Map[String, Any]): Option[(Row, Row)] = {
    implicit val formats = DefaultFormats

    val environmentBuild = parse(fields.getOrElse("environment.build", "{}")
      .asInstanceOf[String])
      .extract[EnvironmentBuild]

    val environmentSystem = parse(fields.getOrElse("environment.system", "{}")
      .asInstanceOf[String])
      .extract[EnvironmentSystem]

    val payloadInfo = parse(fields.getOrElse("payload.info", "{}")
      .asInstanceOf[String])
      .extract[PayloadInfo]

    val keyedHistograms = parse(fields.getOrElse("payload.histograms", "{}")
      .asInstanceOf[String]
    )

    val application = fields.get("appName").asInstanceOf[Option[String]]
    val channel = fields.get("normalizedChannel").asInstanceOf[Option[String]]
    val country = fields.get("geoCountry").asInstanceOf[Option[String]]
    val docType = fields.get("docType").get.asInstanceOf[String]

    val dimensions = new RowBuilder(dimensionsSchema)
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

    Some((dimensions.build, stats.build))
  }

  private[streaming] def process(
                                  datasetPath: String,
                                  raiseOnError: Boolean
                                )(stream: DStream[(String, Message)]): Unit = {
    stream
      .map(_._2.fieldsAsMap)
      .filter{ fields =>
        val docType = fields.getOrElse("docType", "")
        docType == "main" || docType == "crash"
      }.foreachRDD { (rdd, time) =>
      val rows = rdd
        .flatMap(ping => {
          try {
            parsePing(ping)
          } catch {
            // TODO: count number of failures
            case _: Throwable if !raiseOnError  => None
          }
        })
        .reduceByKey((x, y) => RowBuilder.add(x, y, statsSchema))
        .map(RowBuilder.merge)

      val mergedSchema = SchemaBuilder.merge(dimensionsSchema, statsSchema)
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      spark
        .createDataFrame(rows, mergedSchema)
        .withColumn("timestamp", lit(new Timestamp(time.milliseconds)))
        .withColumn("date", lit(new java.sql.Date(time.milliseconds)))
        .coalesce(1)
        .write
        .mode("append")
        .partitionBy("date", "timestamp")
        .parquet(datasetPath)
    }
  }

  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext(conf, Seconds(300))
    val opts = new Opts(args)
    val outputPath = opts.outputPath()
    val prefix = s"error_aggregates/v1"

    val kafkaParams = Map(
      "metadata.broker.list" -> opts.kafkaBroker(),
      "group.id" -> opts.kafkaGroupId(),
      "auto.offset.reset" -> "largest"
    )

    val stream = KafkaUtils.createDirectStream[String, Message, StringDecoder, HekaMessageDecoder](
      ssc,
      kafkaParams,
      Set("telemetry"))

    process(s"${outputPath}/${prefix}/", opts.raiseOnError())(stream)

    ssc.start()
    ssc.awaitTermination()
  }
}
