/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.deployment.model.element;

import static io.zeebe.util.buffer.BufferUtil.bufferAsString;
import static io.zeebe.util.buffer.BufferUtil.wrapString;

import io.zeebe.protocol.record.value.BpmnElementType;
import java.util.HashMap;
import java.util.Map;
import org.agrona.DirectBuffer;

/** Executable* prefix in order to avoid confusion with model API classes. */
public class ExecutableWorkflow extends ExecutableFlowElementContainer {

  private final Map<DirectBuffer, AbstractFlowElement> flowElements = new HashMap<>();

  public ExecutableWorkflow(final String id) {
    super(id);
    addFlowElement(this);
  }

  public void addFlowElement(final AbstractFlowElement element) {
    flowElements.put(element.getId(), element);
  }

  public AbstractFlowElement getElementById(final DirectBuffer id) {
    return flowElements.get(id);
  }

  /** convenience function for transformation */
  public <T extends ExecutableFlowElement> T getElementById(
      final String id, final Class<T> expectedType) {
    return getElementById(wrapString(id), expectedType);
  }

  public <T extends ExecutableFlowElement> T getElementById(
      final DirectBuffer id, final Class<T> expectedClass) {

    final var elementType =
        ExecutableMultiInstanceBody.class.isAssignableFrom(expectedClass)
            ? BpmnElementType.MULTI_INSTANCE_BODY
            : BpmnElementType.UNSPECIFIED;

    return getElementById(id, elementType, expectedClass);
  }

  public <T extends ExecutableFlowElement> T getElementById(
      final DirectBuffer id, final BpmnElementType elementType, final Class<T> expectedClass) {

    var element = flowElements.get(id);
    if (element == null) {
      return null;
    }

    if (element instanceof ExecutableMultiInstanceBody
        && elementType != BpmnElementType.MULTI_INSTANCE_BODY) {
      // the multi-instance body and the inner activity have the same element id
      final var multiInstanceBody = (ExecutableMultiInstanceBody) element;
      element = multiInstanceBody.getInnerActivity();
    }

    if (element.getElementType() != elementType && elementType != BpmnElementType.UNSPECIFIED) {
      throw new RuntimeException(
          String.format(
              "Expected element with id '%s' to be of type '%s', but it is of type '%s'",
              bufferAsString(id), elementType, element.getElementType()));
    }

    if (expectedClass.isAssignableFrom(element.getClass())) {
      return (T) element;
    } else {
      throw new RuntimeException(
          String.format(
              "Expected element with id '%s' to be instance of class '%s', but it is an instance of '%s'",
              bufferAsString(id),
              expectedClass.getSimpleName(),
              element.getClass().getSimpleName()));
    }
  }
}
