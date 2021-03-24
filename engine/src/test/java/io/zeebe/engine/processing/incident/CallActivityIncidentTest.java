/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.incident;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.Assertions;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.intent.IncidentIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.protocol.record.value.ErrorType;
import io.zeebe.protocol.record.value.IncidentRecordValue;
import io.zeebe.protocol.record.value.WorkflowInstanceRecordValue;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public final class CallActivityIncidentTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();

  private static final String PROCESS_ID_PARENT = "wf-parent";
  private static final String PROCESS_ID_CHILD = "wf-child";

  private static final String PROCESS_ID_VARIABLE = "wfChild";

  private static final BpmnModelInstance WORKFLOW_PARENT =
      Bpmn.createExecutableProcess(PROCESS_ID_PARENT)
          .startEvent()
          .callActivity("call", c -> c.zeebeProcessId(PROCESS_ID_CHILD))
          .done();

  private static final BpmnModelInstance WORKFLOW_PARENT_PROCESS_ID_EXPRESSION =
      Bpmn.createExecutableProcess(PROCESS_ID_PARENT)
          .startEvent()
          .callActivity("call", c -> c.zeebeProcessIdExpression(PROCESS_ID_VARIABLE))
          .done();

  private static final BpmnModelInstance WORKFLOW_CHILD =
      Bpmn.createExecutableProcess(PROCESS_ID_CHILD).startEvent().endEvent().done();

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  @Before
  public void init() {
    ENGINE.deployment().withXmlResource("wf-parent.bpmn", WORKFLOW_PARENT).deploy();
  }

  @Test
  public void shouldCreateIncidentIfWorkflowIsNotDeployed() {
    // when
    final long workflowInstanceKey =
        ENGINE.workflowInstance().ofBpmnProcessId(PROCESS_ID_PARENT).create();

    // then
    final Record<IncidentRecordValue> incident = getIncident(workflowInstanceKey);
    final Record<WorkflowInstanceRecordValue> elementInstance =
        getCallActivityInstance(workflowInstanceKey);

    Assertions.assertThat(incident.getValue())
        .hasElementInstanceKey(elementInstance.getKey())
        .hasElementId(elementInstance.getValue().getElementId())
        .hasErrorType(ErrorType.CALLED_ELEMENT_ERROR)
        .hasErrorMessage(
            "Expected workflow with BPMN process id '"
                + PROCESS_ID_CHILD
                + "' to be deployed, but not found.");
  }

  @Test
  public void shouldCreateIncidentIfWorkflowHasNoNoneStartEvent() {
    // given
    ENGINE
        .deployment()
        .withXmlResource(
            Bpmn.createExecutableProcess(PROCESS_ID_CHILD)
                .startEvent()
                .message("start")
                .endEvent()
                .done())
        .deploy();

    // when
    final long workflowInstanceKey =
        ENGINE.workflowInstance().ofBpmnProcessId(PROCESS_ID_PARENT).create();

    // then
    final Record<IncidentRecordValue> incident = getIncident(workflowInstanceKey);
    final Record<WorkflowInstanceRecordValue> elementInstance =
        getCallActivityInstance(workflowInstanceKey);

    Assertions.assertThat(incident.getValue())
        .hasElementInstanceKey(elementInstance.getKey())
        .hasElementId(elementInstance.getValue().getElementId())
        .hasErrorType(ErrorType.CALLED_ELEMENT_ERROR)
        .hasErrorMessage(
            "Expected workflow with BPMN process id '"
                + PROCESS_ID_CHILD
                + "' to have a none start event, but not found.");
  }

  @Test
  public void shouldCreateIncidentIfProcessIdVariableNotExists() {
    // given
    ENGINE.deployment().withXmlResource(WORKFLOW_PARENT_PROCESS_ID_EXPRESSION).deploy();

    // when
    final long workflowInstanceKey =
        ENGINE.workflowInstance().ofBpmnProcessId(PROCESS_ID_PARENT).create();

    // then
    final Record<IncidentRecordValue> incident = getIncident(workflowInstanceKey);
    final Record<WorkflowInstanceRecordValue> elementInstance =
        getCallActivityInstance(workflowInstanceKey);

    Assertions.assertThat(incident.getValue())
        .hasElementInstanceKey(elementInstance.getKey())
        .hasElementId(elementInstance.getValue().getElementId())
        .hasErrorType(ErrorType.EXTRACT_VALUE_ERROR)
        .hasErrorMessage(
            "failed to evaluate expression '"
                + PROCESS_ID_VARIABLE
                + "': no variable found for name '"
                + PROCESS_ID_VARIABLE
                + "'");
  }

  @Test
  public void shouldCreateIncidentIfProcessIdVariableIsNaString() {
    // given
    ENGINE.deployment().withXmlResource(WORKFLOW_PARENT_PROCESS_ID_EXPRESSION).deploy();

    // when
    final long workflowInstanceKey =
        ENGINE
            .workflowInstance()
            .ofBpmnProcessId(PROCESS_ID_PARENT)
            .withVariable(PROCESS_ID_VARIABLE, 123)
            .create();

    // then
    final Record<IncidentRecordValue> incident = getIncident(workflowInstanceKey);
    final Record<WorkflowInstanceRecordValue> elementInstance =
        getCallActivityInstance(workflowInstanceKey);

    Assertions.assertThat(incident.getValue())
        .hasElementInstanceKey(elementInstance.getKey())
        .hasElementId(elementInstance.getValue().getElementId())
        .hasErrorType(ErrorType.EXTRACT_VALUE_ERROR)
        .hasErrorMessage(
            "Expected result of the expression '"
                + PROCESS_ID_VARIABLE
                + "' to be 'STRING', but was 'NUMBER'.");
  }

  @Test
  public void shouldResolveIncident() {
    // given
    final long workflowInstanceKey =
        ENGINE.workflowInstance().ofBpmnProcessId(PROCESS_ID_PARENT).create();

    final Record<IncidentRecordValue> incident = getIncident(workflowInstanceKey);

    // when
    ENGINE.deployment().withXmlResource(WORKFLOW_CHILD).deploy();

    ENGINE.incident().ofInstance(workflowInstanceKey).withKey(incident.getKey()).resolve();

    // then
    assertThat(
            RecordingExporter.workflowInstanceRecords()
                .withRecordKey(incident.getValue().getElementInstanceKey())
                .limit(2))
        .extracting(Record::getIntent)
        .contains(WorkflowInstanceIntent.ELEMENT_ACTIVATED);
  }

  private Record<WorkflowInstanceRecordValue> getCallActivityInstance(
      final long workflowInstanceKey) {
    return RecordingExporter.workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_ACTIVATING)
        .withWorkflowInstanceKey(workflowInstanceKey)
        .withElementType(BpmnElementType.CALL_ACTIVITY)
        .getFirst();
  }

  private Record<IncidentRecordValue> getIncident(final long workflowInstanceKey) {
    return RecordingExporter.incidentRecords(IncidentIntent.CREATED)
        .withWorkflowInstanceKey(workflowInstanceKey)
        .getFirst();
  }
}
