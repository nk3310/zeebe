/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.db.impl.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.db.ZeebeDb;
import io.zeebe.db.ZeebeDbFactory;
import io.zeebe.db.impl.DefaultColumnFamily;
import io.zeebe.util.ByteValue;
import java.io.File;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionPriority;

public final class ZeebeRocksDbFactoryTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void shouldCreateNewDb() throws Exception {
    // given
    final ZeebeDbFactory<DefaultColumnFamily> dbFactory =
        ZeebeRocksDbFactory.newFactory(DefaultColumnFamily.class);

    final File pathName = temporaryFolder.newFolder();

    // when
    final ZeebeDb<DefaultColumnFamily> db = dbFactory.createDb(pathName);

    // then
    assertThat(pathName.listFiles()).isNotEmpty();
    db.close();
  }

  @Test
  public void shouldCreateTwoNewDbs() throws Exception {
    // given
    final ZeebeDbFactory<DefaultColumnFamily> dbFactory =
        ZeebeRocksDbFactory.newFactory(DefaultColumnFamily.class);
    final File firstPath = temporaryFolder.newFolder();
    final File secondPath = temporaryFolder.newFolder();

    // when
    final ZeebeDb<DefaultColumnFamily> firstDb = dbFactory.createDb(firstPath);
    final ZeebeDb<DefaultColumnFamily> secondDb = dbFactory.createDb(secondPath);

    // then
    assertThat(firstDb).isNotEqualTo(secondDb);

    assertThat(firstPath.listFiles()).isNotEmpty();
    assertThat(secondPath.listFiles()).isNotEmpty();

    firstDb.close();
    secondDb.close();
  }

  @Test
  public void shouldOverwriteDefaultColumnFamilyOptions() {
    // given
    final var customProperties = new Properties();
    customProperties.put("write_buffer_size", String.valueOf(ByteValue.ofMegabytes(16)));
    customProperties.put("compaction_pri", "kByCompensatedSize");

    final var factoryWithDefaults =
        (ZeebeRocksDbFactory<DefaultColumnFamily>)
            ZeebeRocksDbFactory.newFactory(DefaultColumnFamily.class);
    final var factoryWithCustomOptions =
        (ZeebeRocksDbFactory<DefaultColumnFamily>)
            ZeebeRocksDbFactory.newFactory(DefaultColumnFamily.class, customProperties);

    // when
    final var defaults = factoryWithDefaults.createColumnFamilyOptions();
    final var customOptions = factoryWithCustomOptions.createColumnFamilyOptions();

    // then
    assertThat(defaults)
        .extracting(ColumnFamilyOptions::writeBufferSize, ColumnFamilyOptions::compactionPriority)
        .containsExactly(ByteValue.ofMegabytes(64), CompactionPriority.OldestSmallestSeqFirst);

    assertThat(customOptions)
        .extracting(ColumnFamilyOptions::writeBufferSize, ColumnFamilyOptions::compactionPriority)
        .containsExactly(ByteValue.ofMegabytes(16), CompactionPriority.ByCompensatedSize);
  }
}
