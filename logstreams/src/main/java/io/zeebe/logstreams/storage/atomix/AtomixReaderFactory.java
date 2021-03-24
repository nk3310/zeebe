/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.logstreams.storage.atomix;

import io.atomix.raft.storage.log.RaftLogReader;
import io.atomix.storage.journal.JournalReader.Mode;

@FunctionalInterface
public interface AtomixReaderFactory {
  RaftLogReader create(long index, Mode mode);

  default RaftLogReader create(final long index) {
    return create(index, Mode.COMMITS);
  }

  default RaftLogReader create() {
    return create(0);
  }
}
