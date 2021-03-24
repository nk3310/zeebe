/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.util.client;

import io.zeebe.engine.util.StreamProcessorRule;
import io.zeebe.msgpack.value.StringValue;
import io.zeebe.msgpack.value.ValueArray;
import io.zeebe.protocol.impl.record.value.job.JobBatchRecord;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.intent.JobBatchIntent;
import io.zeebe.protocol.record.value.JobBatchRecordValue;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.util.buffer.BufferUtil;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

public final class JobActivationClient {
  private static final int DEFAULT_PARTITION = 1;
  private static final long DEFAULT_TIMEOUT = 10000L;
  private static final String DEFAULT_WORKER = "defaultWorker";
  private static final int DEFAULT_MAX_ACTIVATE = 10;

  private static final BiFunction<Integer, Long, Record<JobBatchRecordValue>>
      SUCCESS_EXPECTATION_SUPPLIER =
          (partitionId, position) ->
              RecordingExporter.jobBatchRecords(JobBatchIntent.ACTIVATED)
                  .withPartitionId(partitionId)
                  .withSourceRecordPosition(position)
                  .getFirst();

  private static final BiFunction<Integer, Long, Record<JobBatchRecordValue>>
      REJECTION_EXPECTATION_SUPPLIER =
          (partitionId, position) ->
              RecordingExporter.jobBatchRecords(JobBatchIntent.ACTIVATE)
                  .onlyCommandRejections()
                  .withPartitionId(partitionId)
                  .withSourceRecordPosition(position)
                  .getFirst();

  private final StreamProcessorRule environmentRule;
  private final JobBatchRecord jobBatchRecord;

  private int partitionId;
  private BiFunction<Integer, Long, Record<JobBatchRecordValue>> expectation =
      SUCCESS_EXPECTATION_SUPPLIER;

  public JobActivationClient(final StreamProcessorRule environmentRule) {
    this.environmentRule = environmentRule;

    jobBatchRecord = new JobBatchRecord();
    jobBatchRecord
        .setTimeout(DEFAULT_TIMEOUT)
        .setWorker(DEFAULT_WORKER)
        .setMaxJobsToActivate(DEFAULT_MAX_ACTIVATE);
    partitionId = DEFAULT_PARTITION;
  }

  public JobActivationClient withType(final String type) {
    jobBatchRecord.setType(type);
    return this;
  }

  public JobActivationClient withTimeout(final long timeout) {
    jobBatchRecord.setTimeout(timeout);
    return this;
  }

  public JobActivationClient withFetchVariables(final String... fetchVariables) {
    return withFetchVariables(Arrays.asList(fetchVariables));
  }

  public JobActivationClient withFetchVariables(final List<String> fetchVariables) {
    final ValueArray<StringValue> variables = jobBatchRecord.variables();
    fetchVariables.stream()
        .map(BufferUtil::wrapString)
        .forEach(buffer -> variables.add().wrap(buffer));
    return this;
  }

  public JobActivationClient byWorker(final String name) {
    jobBatchRecord.setWorker(name);
    return this;
  }

  public JobActivationClient onPartition(final int partitionId) {
    this.partitionId = partitionId;
    return this;
  }

  public JobActivationClient withMaxJobsToActivate(final int count) {
    jobBatchRecord.setMaxJobsToActivate(count);
    return this;
  }

  public JobActivationClient expectRejection() {
    expectation = REJECTION_EXPECTATION_SUPPLIER;
    return this;
  }

  public Record<JobBatchRecordValue> activate() {
    final long position =
        environmentRule.writeCommandOnPartition(
            partitionId, JobBatchIntent.ACTIVATE, jobBatchRecord);

    return expectation.apply(partitionId, position);
  }
}
