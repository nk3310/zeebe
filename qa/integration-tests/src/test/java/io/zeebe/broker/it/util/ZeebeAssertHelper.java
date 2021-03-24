/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.util;

import static io.zeebe.protocol.record.intent.WorkflowInstanceIntent.ELEMENT_ACTIVATED;
import static io.zeebe.protocol.record.intent.WorkflowInstanceIntent.ELEMENT_ACTIVATING;
import static io.zeebe.protocol.record.intent.WorkflowInstanceIntent.ELEMENT_COMPLETED;
import static io.zeebe.protocol.record.intent.WorkflowInstanceIntent.ELEMENT_TERMINATED;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.intent.IncidentIntent;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.protocol.record.intent.VariableDocumentIntent;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.protocol.record.value.JobRecordValue;
import io.zeebe.protocol.record.value.VariableDocumentRecordValue;
import io.zeebe.protocol.record.value.WorkflowInstanceRecordValue;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.WorkflowInstanceRecordStream;
import java.util.function.Consumer;

public final class ZeebeAssertHelper {

  public static void assertWorkflowInstanceCreated() {
    assertWorkflowInstanceCreated((e) -> {});
  }

  public static void assertWorkflowInstanceCreated(final long workflowInstanceKey) {
    assertWorkflowInstanceCreated(workflowInstanceKey, w -> {});
  }

  public static void assertWorkflowInstanceCreated(
      final Consumer<WorkflowInstanceRecordValue> consumer) {
    assertWorkflowInstanceState(WorkflowInstanceIntent.ELEMENT_ACTIVATING, consumer);
  }

  public static void assertJobCreated(final String jobType) {
    assertThat(RecordingExporter.jobRecords(JobIntent.CREATED).withType(jobType).exists()).isTrue();
  }

  public static void assertJobCreated(
      final String jobType, final Consumer<JobRecordValue> consumer) {
    final JobRecordValue value =
        RecordingExporter.jobRecords(JobIntent.CREATED)
            .withType(jobType)
            .findFirst()
            .map(Record::getValue)
            .orElse(null);

    assertThat(value).isNotNull();

    consumer.accept(value);
  }

  public static void assertIncidentCreated() {
    assertThat(RecordingExporter.incidentRecords(IncidentIntent.CREATED).exists()).isTrue();
  }

  public static void assertWorkflowInstanceCompleted(
      final long workflowInstanceKey, final Consumer<WorkflowInstanceRecordValue> consumer) {
    final Record<WorkflowInstanceRecordValue> record =
        RecordingExporter.workflowInstanceRecords(ELEMENT_COMPLETED)
            .withRecordKey(workflowInstanceKey)
            .findFirst()
            .orElse(null);

    assertThat(record).isNotNull();

    if (consumer != null) {
      consumer.accept(record.getValue());
    }
  }

  public static void assertWorkflowInstanceCompleted(final long workflowInstanceKey) {
    assertWorkflowInstanceCompleted(workflowInstanceKey, r -> {});
  }

  public static void assertElementActivated(final String element) {
    assertElementInState(ELEMENT_ACTIVATED, element, (e) -> {});
  }

  public static void assertElementReady(final String element) {
    assertElementInState(ELEMENT_ACTIVATING, element, (e) -> {});
  }

  public static void assertWorkflowInstanceCanceled(final String bpmnId) {
    assertThat(
            RecordingExporter.workflowInstanceRecords(ELEMENT_TERMINATED)
                .withBpmnProcessId(bpmnId)
                .withElementId(bpmnId)
                .exists())
        .isTrue();
  }

  public static void assertWorkflowInstanceCompleted(
      final String workflow, final long workflowInstanceKey) {
    assertElementCompleted(workflowInstanceKey, workflow, (e) -> {});
  }

  public static void assertWorkflowInstanceCompleted(final String bpmnId) {
    assertWorkflowInstanceCompleted(bpmnId, (e) -> {});
  }

  public static void assertWorkflowInstanceCompleted(
      final String bpmnId, final Consumer<WorkflowInstanceRecordValue> eventConsumer) {
    assertElementCompleted(bpmnId, bpmnId, eventConsumer);
  }

  public static void assertJobCompleted() {
    assertThat(RecordingExporter.jobRecords(JobIntent.COMPLETED).exists()).isTrue();
  }

  public static void assertJobCanceled() {
    assertThat(RecordingExporter.jobRecords(JobIntent.CANCELED).exists()).isTrue();
  }

  public static void assertJobCompleted(final String jobType) {
    assertJobCompleted(jobType, (j) -> {});
  }

  public static void assertJobCompleted(
      final String jobType, final Consumer<JobRecordValue> consumer) {
    final JobRecordValue job =
        RecordingExporter.jobRecords(JobIntent.COMPLETED)
            .withType(jobType)
            .findFirst()
            .map(Record::getValue)
            .orElse(null);

    assertThat(job).isNotNull();
    consumer.accept(job);
  }

  public static void assertElementCompleted(final String bpmnId, final String activity) {
    assertElementCompleted(bpmnId, activity, (e) -> {});
  }

  public static void assertElementCompleted(
      final String bpmnId,
      final String activity,
      final Consumer<WorkflowInstanceRecordValue> eventConsumer) {
    final Record<WorkflowInstanceRecordValue> workflowInstanceRecordValueRecord =
        RecordingExporter.workflowInstanceRecords(ELEMENT_COMPLETED)
            .withBpmnProcessId(bpmnId)
            .withElementId(activity)
            .findFirst()
            .orElse(null);

    assertThat(workflowInstanceRecordValueRecord).isNotNull();

    eventConsumer.accept(workflowInstanceRecordValueRecord.getValue());
  }

  public static void assertElementCompleted(
      final long workflowInstanceKey,
      final String activity,
      final Consumer<WorkflowInstanceRecordValue> eventConsumer) {
    final Record<WorkflowInstanceRecordValue> workflowInstanceRecordValueRecord =
        RecordingExporter.workflowInstanceRecords(ELEMENT_COMPLETED)
            .withElementId(activity)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .findFirst()
            .orElse(null);

    assertThat(workflowInstanceRecordValueRecord).isNotNull();

    eventConsumer.accept(workflowInstanceRecordValueRecord.getValue());
  }

  public static void assertWorkflowInstanceState(
      final long workflowInstanceKey,
      final WorkflowInstanceIntent intent,
      final Consumer<WorkflowInstanceRecordValue> consumer) {
    consumeFirstWorkflowInstanceRecord(
        RecordingExporter.workflowInstanceRecords(intent)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .filter(r -> r.getKey() == r.getValue().getWorkflowInstanceKey()),
        consumer);
  }

  public static void assertWorkflowInstanceCreated(
      final long workflowInstanceKey, final Consumer<WorkflowInstanceRecordValue> consumer) {
    assertWorkflowInstanceState(
        workflowInstanceKey, WorkflowInstanceIntent.ELEMENT_ACTIVATING, consumer);
  }

  public static void assertWorkflowInstanceState(
      final WorkflowInstanceIntent intent, final Consumer<WorkflowInstanceRecordValue> consumer) {
    consumeFirstWorkflowInstanceRecord(
        RecordingExporter.workflowInstanceRecords(intent)
            .filter(r -> r.getKey() == r.getValue().getWorkflowInstanceKey()),
        consumer);
  }

  public static void assertElementInState(
      final long workflowInstanceKey, final String elementId, final WorkflowInstanceIntent intent) {
    final Record<WorkflowInstanceRecordValue> record =
        RecordingExporter.workflowInstanceRecords(intent)
            .withWorkflowInstanceKey(workflowInstanceKey)
            .withElementId(elementId)
            .findFirst()
            .orElse(null);

    assertThat(record).isNotNull();
  }

  public static void assertElementInState(
      final long workflowInstanceKey,
      final String elementId,
      final BpmnElementType elementType,
      final WorkflowInstanceIntent intent) {
    assertThat(
            RecordingExporter.workflowInstanceRecords(intent)
                .withWorkflowInstanceKey(workflowInstanceKey)
                .withElementType(elementType)
                .withElementId(elementId)
                .exists())
        .isTrue();
  }

  public static void assertElementInState(
      final WorkflowInstanceIntent intent,
      final String element,
      final Consumer<WorkflowInstanceRecordValue> consumer) {
    consumeFirstWorkflowInstanceRecord(
        RecordingExporter.workflowInstanceRecords(intent).withElementId(element), consumer);
  }

  private static void consumeFirstWorkflowInstanceRecord(
      final WorkflowInstanceRecordStream stream,
      final Consumer<WorkflowInstanceRecordValue> consumer) {

    final WorkflowInstanceRecordValue value = stream.findFirst().map(Record::getValue).orElse(null);

    assertThat(value).isNotNull();

    consumer.accept(value);
  }

  public static void assertIncidentResolved() {
    assertThat(RecordingExporter.incidentRecords(IncidentIntent.RESOLVED).exists()).isTrue();
  }

  public static void assertIncidentResolveFailed() {
    assertThat(RecordingExporter.incidentRecords(IncidentIntent.RESOLVED).exists()).isTrue();

    assertThat(
            RecordingExporter.incidentRecords()
                .skipUntil(e -> e.getIntent() == IncidentIntent.RESOLVED)
                .filter(e -> e.getIntent() == IncidentIntent.CREATED)
                .exists())
        .isTrue();
  }

  public static void assertVariableDocumentUpdated() {
    assertVariableDocumentUpdated(e -> {});
  }

  public static void assertVariableDocumentUpdated(
      final Consumer<VariableDocumentRecordValue> eventConsumer) {
    final Record<VariableDocumentRecordValue> record =
        RecordingExporter.variableDocumentRecords(VariableDocumentIntent.UPDATED)
            .findFirst()
            .orElse(null);

    assertThat(record).isNotNull();
    eventConsumer.accept(record.getValue());
  }
}
