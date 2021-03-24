/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.deployment.model.transformer;

import io.zeebe.engine.processing.deployment.model.element.ExecutableWorkflow;
import io.zeebe.engine.processing.deployment.model.transformation.ModelElementTransformer;
import io.zeebe.engine.processing.deployment.model.transformation.TransformContext;
import io.zeebe.model.bpmn.instance.Process;

public final class ContextProcessTransformer implements ModelElementTransformer<Process> {

  @Override
  public Class<Process> getType() {
    return Process.class;
  }

  @Override
  public void transform(final Process element, final TransformContext context) {
    final ExecutableWorkflow workflow = context.getWorkflow(element.getId());
    context.setCurrentWorkflow(workflow);
  }
}
