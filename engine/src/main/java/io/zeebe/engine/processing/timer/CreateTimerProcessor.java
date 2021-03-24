/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.timer;

import io.zeebe.engine.processing.streamprocessor.TypedRecord;
import io.zeebe.engine.processing.streamprocessor.TypedRecordProcessor;
import io.zeebe.engine.processing.streamprocessor.sideeffect.SideEffectProducer;
import io.zeebe.engine.processing.streamprocessor.writers.TypedResponseWriter;
import io.zeebe.engine.processing.streamprocessor.writers.TypedStreamWriter;
import io.zeebe.engine.state.KeyGenerator;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.engine.state.deployment.WorkflowState;
import io.zeebe.engine.state.instance.TimerInstance;
import io.zeebe.protocol.impl.record.value.timer.TimerRecord;
import io.zeebe.protocol.record.intent.TimerIntent;
import java.util.function.Consumer;

public final class CreateTimerProcessor implements TypedRecordProcessor<TimerRecord> {

  private final DueDateTimerChecker timerChecker;

  private final WorkflowState workflowState;
  private final TimerInstance timerInstance = new TimerInstance();
  private final KeyGenerator keyGenerator;

  public CreateTimerProcessor(final ZeebeState zeebeState, final DueDateTimerChecker timerChecker) {
    this.timerChecker = timerChecker;
    workflowState = zeebeState.getWorkflowState();
    keyGenerator = zeebeState.getKeyGenerator();
  }

  @Override
  public void processRecord(
      final TypedRecord<TimerRecord> record,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter,
      final Consumer<SideEffectProducer> sideEffect) {

    final TimerRecord timer = record.getValue();

    final long timerKey = keyGenerator.nextKey();

    timerInstance.setElementInstanceKey(timer.getElementInstanceKey());
    timerInstance.setDueDate(timer.getDueDate());
    timerInstance.setKey(timerKey);
    timerInstance.setHandlerNodeId(timer.getTargetElementIdBuffer());
    timerInstance.setRepetitions(timer.getRepetitions());
    timerInstance.setWorkflowKey(timer.getWorkflowKey());
    timerInstance.setWorkflowInstanceKey(timer.getWorkflowInstanceKey());

    sideEffect.accept(this::scheduleTimer);

    streamWriter.appendFollowUpEvent(timerKey, TimerIntent.CREATED, timer);

    workflowState.getTimerState().put(timerInstance);
  }

  private boolean scheduleTimer() {
    timerChecker.scheduleTimer(timerInstance);

    return true;
  }
}
