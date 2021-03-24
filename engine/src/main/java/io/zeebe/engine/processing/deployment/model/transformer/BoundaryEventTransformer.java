/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.deployment.model.transformer;

import io.zeebe.engine.processing.deployment.model.element.ExecutableActivity;
import io.zeebe.engine.processing.deployment.model.element.ExecutableBoundaryEvent;
import io.zeebe.engine.processing.deployment.model.element.ExecutableWorkflow;
import io.zeebe.engine.processing.deployment.model.transformation.ModelElementTransformer;
import io.zeebe.engine.processing.deployment.model.transformation.TransformContext;
import io.zeebe.model.bpmn.instance.Activity;
import io.zeebe.model.bpmn.instance.BoundaryEvent;

public final class BoundaryEventTransformer implements ModelElementTransformer<BoundaryEvent> {
  @Override
  public Class<BoundaryEvent> getType() {
    return BoundaryEvent.class;
  }

  @Override
  public void transform(final BoundaryEvent event, final TransformContext context) {
    final ExecutableWorkflow workflow = context.getCurrentWorkflow();
    final ExecutableBoundaryEvent element =
        workflow.getElementById(event.getId(), ExecutableBoundaryEvent.class);

    element.setInterrupting(event.cancelActivity());

    attachToActivity(event, workflow, element);
  }

  private void attachToActivity(
      final BoundaryEvent event,
      final ExecutableWorkflow workflow,
      final ExecutableBoundaryEvent element) {
    final Activity attachedToActivity = event.getAttachedTo();
    final ExecutableActivity attachedToElement =
        workflow.getElementById(attachedToActivity.getId(), ExecutableActivity.class);

    attachedToElement.attach(element);
  }
}
