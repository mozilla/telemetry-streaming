package com.mozilla.telemetry

import com.mozilla.telemetry.heka.Message
import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties

package object streaming {
  class HekaMessageDecoder(props: VerifiableProperties = null) extends Decoder[Message] {
    def fromBytes(bytes: Array[Byte]): Message = {
      Message.parseFrom(bytes)
    }
  }

}
