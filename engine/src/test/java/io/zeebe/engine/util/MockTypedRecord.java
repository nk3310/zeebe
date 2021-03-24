/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.util;

import io.zeebe.engine.processing.streamprocessor.TypedRecord;
import io.zeebe.protocol.Protocol;
import io.zeebe.protocol.impl.record.RecordMetadata;
import io.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.RejectionType;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.Intent;

public final class MockTypedRecord<T extends UnifiedRecordValue> implements TypedRecord<T> {

  private final long timestamp;
  private long key;
  private RecordMetadata metadata;
  private T value;

  public MockTypedRecord(final long key, final RecordMetadata metadata, final T value) {
    this.key = key;
    this.metadata = metadata;
    this.value = value;
    timestamp = System.currentTimeMillis();
  }

  @Override
  public long getKey() {
    return key;
  }

  public void setKey(final long key) {
    this.key = key;
  }

  @Override
  public T getValue() {
    return value;
  }

  @Override
  public int getRequestStreamId() {
    return metadata.getRequestStreamId();
  }

  @Override
  public long getRequestId() {
    return metadata.getRequestId();
  }

  @Override
  public long getLength() {
    return metadata.getLength() + value.getLength();
  }

  public void setValue(final T value) {
    this.value = value;
  }

  public void setMetadata(final RecordMetadata metadata) {
    this.metadata = metadata;
  }

  @Override
  public long getPosition() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  @Override
  public long getSourceRecordPosition() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public Intent getIntent() {
    return metadata.getIntent();
  }

  @Override
  public int getPartitionId() {
    return Protocol.decodePartitionId(key);
  }

  @Override
  public RecordType getRecordType() {
    return metadata.getRecordType();
  }

  @Override
  public RejectionType getRejectionType() {
    return metadata.getRejectionType();
  }

  @Override
  public String getRejectionReason() {
    return metadata.getRejectionReason();
  }

  @Override
  public String getBrokerVersion() {
    return metadata.getBrokerVersion().toString();
  }

  @Override
  public ValueType getValueType() {
    return metadata.getValueType();
  }

  @Override
  public String toJson() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  @Override
  public Record<T> clone() {
    throw new UnsupportedOperationException("not yet implemented");
  }
}
