/**
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.el.impl.feel

import io.zeebe.el.impl.Loggers.LOGGER
import io.zeebe.msgpack.spec.MsgPackWriter
import org.agrona.concurrent.UnsafeBuffer
import org.agrona.{DirectBuffer, ExpandableArrayBuffer}
import org.camunda.feel.syntaxtree.{Val, _}

class FeelToMessagePackTransformer {

  private val writer = new MsgPackWriter

  private val writeBuffer = new ExpandableArrayBuffer
  private val resultView = new UnsafeBuffer
  private val stringWrapper = new UnsafeBuffer

  def toMessagePack(value: Val): DirectBuffer = {
    writer.wrap(writeBuffer, 0)

    writeValue(value)

    resultView.wrap(writeBuffer, 0, writer.getOffset)
    resultView
  }

  private def writeValue(value: Val) {
    value match {
      case ValNull => writer.writeNil()
      case ValNumber(number) if number.isWhole => writer.writeInteger(number.longValue)
      case ValNumber(number) => writer.writeFloat(number.doubleValue)
      case ValBoolean(boolean) => writer.writeBoolean(boolean)
      case ValString(string) => {
        stringWrapper.wrap(string.getBytes)
        writer.writeString(stringWrapper)
      }
      case ValList(items) => {
        writer.writeArrayHeader(items.size)
        items.foreach(writeValue)
      }
      case ValContext(context: MessagePackContext) => writer.writeRaw(context.messagePackMap)
      case ValContext(context) => {
        val variables = context.variableProvider.getVariables
        writer.writeMapHeader(variables.size)

        variables.foreach { case (key, value) =>
          stringWrapper.wrap(key.getBytes)
          writer.writeString(stringWrapper)

          value match {
            case entryVal: Val => writeValue(entryVal)
            case entryBuffer: DirectBuffer => writer.writeRaw(entryBuffer)
            case other => {
              writer.writeNil()
              LOGGER.warn("No FEEL to MessagePack transformation for '{}'. Using 'null' for context entry with key '{}'.", other, key)
            }
          }
        }
      }
      case other => {
        writer.writeNil()
        LOGGER.warn("No FEEL to MessagePack transformation for '{}'. Using 'null' instead.", other)
      }
    }
  }


}

