/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.streamprocessor;

import io.zeebe.db.ZeebeDb;
import io.zeebe.engine.processing.streamprocessor.writers.CommandResponseWriter;
import io.zeebe.logstreams.log.LogStream;
import io.zeebe.logstreams.log.LoggedEvent;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.util.sched.ActorScheduler;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

public final class StreamProcessorBuilder {

  private final ProcessingContext processingContext;
  private final List<StreamProcessorLifecycleAware> lifecycleListeners = new ArrayList<>();
  private TypedRecordProcessorFactory typedRecordProcessorFactory;
  private ActorScheduler actorScheduler;
  private ZeebeDb zeebeDb;
  private int nodeId;

  public StreamProcessorBuilder() {
    processingContext = new ProcessingContext();
  }

  public StreamProcessorBuilder streamProcessorFactory(
      final TypedRecordProcessorFactory typedRecordProcessorFactory) {
    this.typedRecordProcessorFactory = typedRecordProcessorFactory;
    return this;
  }

  public StreamProcessorBuilder actorScheduler(final ActorScheduler actorScheduler) {
    this.actorScheduler = actorScheduler;
    return this;
  }

  public StreamProcessorBuilder nodeId(final int nodeId) {
    this.nodeId = nodeId;
    return this;
  }

  public StreamProcessorBuilder logStream(final LogStream stream) {
    processingContext.logStream(stream);
    return this;
  }

  public StreamProcessorBuilder commandResponseWriter(
      final CommandResponseWriter commandResponseWriter) {
    processingContext.commandResponseWriter(commandResponseWriter);
    return this;
  }

  public StreamProcessorBuilder onProcessedListener(final Consumer<TypedRecord> onProcessed) {
    processingContext.onProcessedListener(onProcessed);
    return this;
  }

  public StreamProcessorBuilder zeebeDb(final ZeebeDb zeebeDb) {
    this.zeebeDb = zeebeDb;
    return this;
  }

  public StreamProcessorBuilder detectReprocessingInconsistency(
      final boolean detectReprocessingInconsistency) {
    this.processingContext.setDetectReprocessingInconsistency(detectReprocessingInconsistency);
    return this;
  }

  public TypedRecordProcessorFactory getTypedRecordProcessorFactory() {
    return typedRecordProcessorFactory;
  }

  public ProcessingContext getProcessingContext() {
    return processingContext;
  }

  public ActorScheduler getActorScheduler() {
    return actorScheduler;
  }

  public List<StreamProcessorLifecycleAware> getLifecycleListeners() {
    return lifecycleListeners;
  }

  public ZeebeDb getZeebeDb() {
    return zeebeDb;
  }

  public int getNodeId() {
    return nodeId;
  }

  public StreamProcessor build() {
    validate();

    final MetadataFilter metadataFilter = new VersionFilter();
    final EventFilter eventFilter = new MetadataEventFilter(metadataFilter);
    processingContext.eventFilter(eventFilter);

    return new StreamProcessor(this);
  }

  private void validate() {
    Objects.requireNonNull(typedRecordProcessorFactory, "No stream processor factory provided.");
    Objects.requireNonNull(actorScheduler, "No task scheduler provided.");
    Objects.requireNonNull(processingContext.getLogStream(), "No log stream provided.");
    Objects.requireNonNull(
        processingContext.getCommandResponseWriter(), "No command response writer provided.");
    Objects.requireNonNull(zeebeDb, "No database provided.");
  }

  private static class MetadataEventFilter implements EventFilter {

    protected final RecordMetadata metadata = new RecordMetadata();
    protected final MetadataFilter metadataFilter;

    MetadataEventFilter(final MetadataFilter metadataFilter) {
      this.metadataFilter = metadataFilter;
    }

    @Override
    public boolean applies(final LoggedEvent event) {
      event.readMetadata(metadata);
      return metadataFilter.applies(metadata);
    }
  }

  private final class VersionFilter implements MetadataFilter {
    @Override
    public boolean applies(final RecordMetadata m) {
      if (m.getProtocolVersion() > Protocol.PROTOCOL_VERSION) {
        throw new RuntimeException(
            String.format(
                "Cannot handle event with version newer "
                    + "than what is implemented by broker (%d > %d)",
                m.getProtocolVersion(), Protocol.PROTOCOL_VERSION));
      }

      return true;
    }
  }
}
