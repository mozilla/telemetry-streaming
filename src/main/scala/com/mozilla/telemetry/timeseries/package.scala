package com.mozilla.telemetry

import java.sql.Timestamp

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
    def add(x: Row, y: Row, schema: StructType): Row = {
    val result = ListBuffer[Any]()

      for ((field, i) <- schema.fields.zipWithIndex) {
        def fieldAdder[T]()(implicit num: Numeric[T]): T = {
          if (x.isNullAt(i) && y.isNullAt(i)) {
            null.asInstanceOf[T]
          } else if (x.isNullAt(i)) {
            y.getAs[T](i)
          } else if (y.isNullAt(i)) {
            x.getAs[T](i)
          } else {
            val a = x.getAs[T](i)
            val b = y.getAs[T](i)
            num.plus(a, b)
          }
        }

        field.dataType match {
          case FloatType =>
            result += fieldAdder[Float]

          case DoubleType =>
            result += fieldAdder[Double]

          case IntegerType =>
            result += fieldAdder[Int]

          case LongType =>
            result += fieldAdder[Long]

          case _ =>
            throw new Exception(s"RowBuilder.add operation is not defined for field ${field.name}")
        }
      }

      Row.fromSeq(result)
    }

    def merge(x: (Row, Row)): Row = {
      Row.fromSeq(x._1.toSeq ++ x._2.toSeq)
    }
  }
}
