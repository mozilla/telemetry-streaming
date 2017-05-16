// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
package com.mozilla.telemetry

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, _}

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._

package object timeseries {
  class SchemaBuilder(private val fields: ListBuffer[StructField] = ListBuffer()){
    def add[T: TypeTag](name: String): SchemaBuilder = {
      typeOf[T] match {
        case t if t =:= typeOf[String] =>
          fields += StructField (name, StringType, true)
        case t if t =:= typeOf[Boolean] =>
          fields += StructField (name, BooleanType, true)
        case t if t =:= typeOf[Float] =>
          fields += StructField (name, FloatType, true)
        case t if t =:= typeOf[Double] =>
          fields += StructField (name, DoubleType, true)
        case t if t =:= typeOf[Long] =>
          fields += StructField (name, LongType, true)
        case t if t =:= typeOf[Int] =>
          fields += StructField (name, IntegerType, true)
        case t if t =:= typeOf[Timestamp] =>
          fields += StructField (name, TimestampType, true)
        case t if t =:= typeOf[Date] =>
          fields += StructField (name, DateType, true)
        case _ =>
          throw new Exception (s"Unsupported type for field $name")
      }

      this
    }

    def build: StructType = StructType(fields)
  }

  object SchemaBuilder {
    def merge(x: StructType, y: StructType): StructType = StructType(x.fields ++ y.fields)
  }

  class RowBuilder(schema: StructType) extends Serializable {
    val container = Array.fill[Any](schema.length)(null)

    def update(name: String, value: Option[Any]): Unit = {
      val idx = schema.fieldIndex(name)
      value match {
        case Some(v: Any) =>
          container(idx) = v

        case _ =>
      }
    }

    def build: Row = Row.fromSeq(container)
  }

  object RowBuilder {
    def merge(x: (Row, Row)): Row = {
      Row.fromSeq(x._1.toSeq ++ x._2.toSeq)
    }
  }
}
