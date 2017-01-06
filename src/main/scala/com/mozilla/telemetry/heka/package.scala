package com.mozilla.telemetry

package object heka {
  class RichMessage(m: Message) {
    def fieldsAsMap: Map[String, Any] = {
      val fields = m.fields
      Map(fields.map(_.name).zip(fields.map(field)): _*)
    }
  }

  private def field(f: Field): Any = {
    // I am assuming there is only one value
    f.getValueType match {
      case Field.ValueType.BYTES => f.valueBytes(0).toStringUtf8
      case Field.ValueType.STRING => f.valueString(0)
      case Field.ValueType.BOOL => f.valueBool(0)
      case Field.ValueType.DOUBLE => f.valueDouble(0)
      case Field.ValueType.INTEGER => f.valueInteger(0)
      case _ => ""
    }
  }

  implicit def messageToRichMessage(m: Message): RichMessage = new RichMessage(m)
}