/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.configuration;

import java.util.Optional;
import org.springframework.util.unit.DataSize;

/**
 * Be aware that all configuration which are part of this class are experimental, which means they
 * are subject to change and to drop. It might be that also some of them are actually dangerous so
 * be aware when you change one of these!
 */
public class ExperimentalCfg {

  public static final int DEFAULT_MAX_APPENDS_PER_FOLLOWER = 2;
  public static final DataSize DEFAULT_MAX_APPEND_BATCH_SIZE = DataSize.ofKilobytes(32);
  public static final boolean DEFAULT_DISABLE_EXPLICIT_RAFT_FLUSH = false;
  private static final boolean DEFAULT_DETECT_REPROCESSING_INCONSISTENCY = false;

  private int maxAppendsPerFollower = DEFAULT_MAX_APPENDS_PER_FOLLOWER;
  private DataSize maxAppendBatchSize = DEFAULT_MAX_APPEND_BATCH_SIZE;
  private boolean disableExplicitRaftFlush = DEFAULT_DISABLE_EXPLICIT_RAFT_FLUSH;
  private boolean detectReprocessingInconsistency = DEFAULT_DETECT_REPROCESSING_INCONSISTENCY;

  public int getMaxAppendsPerFollower() {
    return maxAppendsPerFollower;
  }

  public void setMaxAppendsPerFollower(final int maxAppendsPerFollower) {
    this.maxAppendsPerFollower = maxAppendsPerFollower;
  }

  public DataSize getMaxAppendBatchSize() {
    return maxAppendBatchSize;
  }

  public void setMaxAppendBatchSize(final DataSize maxAppendBatchSize) {
    this.maxAppendBatchSize = maxAppendBatchSize;
  }

  public long getMaxAppendBatchSizeInBytes() {
    return Optional.ofNullable(maxAppendBatchSize).orElse(DEFAULT_MAX_APPEND_BATCH_SIZE).toBytes();
  }

  public boolean isDisableExplicitRaftFlush() {
    return disableExplicitRaftFlush;
  }

  public void setDisableExplicitRaftFlush(final boolean disableExplicitRaftFlush) {
    this.disableExplicitRaftFlush = disableExplicitRaftFlush;
  }

  public boolean isDetectReprocessingInconsistency() {
    return detectReprocessingInconsistency;
  }

  public void setDetectReprocessingInconsistency(final boolean detectReprocessingInconsistency) {
    this.detectReprocessingInconsistency = detectReprocessingInconsistency;
  }

  @Override
  public String toString() {
    return "ExperimentalCfg{"
        + "maxAppendsPerFollower="
        + maxAppendsPerFollower
        + ", maxAppendBatchSize="
        + maxAppendBatchSize
        + ", disableExplicitRaftFlush="
        + disableExplicitRaftFlush
        + ", detectReprocessingInconsistency="
        + detectReprocessingInconsistency
        + '}';
  }
}
