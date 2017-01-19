package com.mozilla.telemetry.streaming

import java.sql.Timestamp

import com.mozilla.telemetry.heka.Message
import com.mozilla.telemetry.timeseries._
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.rogach.scallop.ScallopConf

case class EnvironmentBuild(version: Option[String],
                            buildId: Option[String],
                            architecture: Option[String])

case class EnvironmentSystem(os: OS)

case class OS(name: Option[String],
              version: Option[String])

case class PayloadInfo(subsessionLength: Option[Int])


object ErrorAggregator {

  private class Opts(args: Array[String]) extends ScallopConf(args) {
    val kafkaBroker = opt[String](
      "kafkaBroker",
      descr = "From submission date",
      required = true)
    val kafkaGroupId = opt[String](
      "kafkaGroupId",
      descr = "A unique string that identifies the consumer group this consumer belongs to",
      required = true)
    val outputBucket = opt[String](
      "outputBucket",
      descr = "Bucket in which to save data",
      required = false,
      default = Some("telemetry-test-bucket"))
    val saveLocally = opt[Boolean](
      "saveLocally",
      descr = "Whether to save the processing output locally or not.")
    verify()
  }

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

  private val conf = new SparkConf().setMaster("local[*]").setAppName("StreamingAggregator")

  private def parsePing(fields: Map[String, Any]): Option[(Row, Row)] = {
    try {
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

      val metrics = new RowBuilder(metricsSchema)
      if (docType == "main") {
        assert(payloadInfo.subsessionLength.isDefined)
        val sessionLength = payloadInfo.subsessionLength.get.toFloat
        metrics("usageHours") = Some(Math.min(25, Math.max(0, sessionLength / 3600)))
        metrics("count") = Some(1)
      } else {
        metrics("crashes") = Some(1)
      }

      Some((dimensions.build, metrics.build))
    } catch {
      // TODO: count number of failures
      case _: Throwable => None
    }
  }

  private[streaming] def process(datasetPath: String)(stream: DStream[(String, Message)]): Unit = {
    stream
      .map(_._2.fieldsAsMap)
      .filter{ fields =>
        val docType = fields.getOrElse("docType", "")
        docType == "main" || docType == "crash"
      }.foreachRDD { (rdd, time) =>
      val rows = rdd
        .flatMap(parsePing)
        .reduceByKey((x, y) => RowBuilder.add(x, y, metricsSchema))
        .map(RowBuilder.merge)

      val mergedSchema = SchemaBuilder.merge(dimensionsSchema, metricsSchema)
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

    val kafkaParams = Map(
      "metadata.broker.list" -> opts.kafkaBroker(),
      "group.id" -> opts.kafkaGroupId(),
      "auto.offset.reset" -> "largest"
    )

    val stream = KafkaUtils.createDirectStream[String, Message, StringDecoder, HekaMessageDecoder](
      ssc,
      kafkaParams,
      Set("telemetry"))


    val prefix = s"error_aggregates/v1"
    val outputBucket = opts.outputBucket()

    val path = if(opts.saveLocally()) "/tmp/parquet" else s"s3://${outputBucket}/${prefix}/"

    process(path)(stream)
    ssc.start()
    ssc.awaitTermination()
  }
}
