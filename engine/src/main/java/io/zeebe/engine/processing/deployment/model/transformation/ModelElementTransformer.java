/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.deployment.model.transformation;

import io.zeebe.model.bpmn.instance.BpmnModelElementInstance;

public interface ModelElementTransformer<T extends BpmnModelElementInstance> {

  Class<T> getType();

  void transform(T element, TransformContext context);
}
