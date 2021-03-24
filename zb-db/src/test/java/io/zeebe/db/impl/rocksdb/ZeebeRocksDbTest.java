/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.db.impl.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.ZeebeDbFactory;
import io.zeebe.db.impl.DbString;
import io.zeebe.db.impl.DefaultColumnFamily;
import java.io.File;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public final class ZeebeRocksDbTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void shouldCreateSnapshot() throws Exception {
    // given
    final ZeebeDbFactory<DefaultColumnFamily> dbFactory =
        ZeebeRocksDbFactory.newFactory(DefaultColumnFamily.class);

    final File pathName = temporaryFolder.newFolder();
    final ZeebeDb<DefaultColumnFamily> db = dbFactory.createDb(pathName);

    final DbString key = new DbString();
    key.wrapString("foo");
    final DbString value = new DbString();
    value.wrapString("bar");
    final ColumnFamily<DbString, DbString> columnFamily =
        db.createColumnFamily(DefaultColumnFamily.DEFAULT, db.createContext(), key, value);
    columnFamily.put(key, value);

    // when
    final File snapshotDir = new File(temporaryFolder.newFolder(), "snapshot");
    db.createSnapshot(snapshotDir);

    // then
    assertThat(pathName.listFiles()).isNotEmpty();
    db.close();
  }

  @Test
  public void shouldReopenDb() throws Exception {
    // given
    final ZeebeDbFactory<DefaultColumnFamily> dbFactory =
        ZeebeRocksDbFactory.newFactory(DefaultColumnFamily.class);
    final File pathName = temporaryFolder.newFolder();
    ZeebeDb<DefaultColumnFamily> db = dbFactory.createDb(pathName);

    final DbString key = new DbString();
    key.wrapString("foo");
    final DbString value = new DbString();
    value.wrapString("bar");
    ColumnFamily<DbString, DbString> columnFamily =
        db.createColumnFamily(DefaultColumnFamily.DEFAULT, db.createContext(), key, value);
    columnFamily.put(key, value);
    db.close();

    // when
    db = dbFactory.createDb(pathName);

    // then
    columnFamily =
        db.createColumnFamily(DefaultColumnFamily.DEFAULT, db.createContext(), key, value);
    final DbString zbString = columnFamily.get(key);
    assertThat(zbString).isNotNull();
    assertThat(zbString.toString()).isEqualTo("bar");

    db.close();
  }

  @Test
  public void shouldRecoverFromSnapshot() throws Exception {
    // given
    final ZeebeDbFactory<DefaultColumnFamily> dbFactory =
        ZeebeRocksDbFactory.newFactory(DefaultColumnFamily.class);
    final File pathName = temporaryFolder.newFolder();
    ZeebeDb<DefaultColumnFamily> db = dbFactory.createDb(pathName);

    final DbString key = new DbString();
    key.wrapString("foo");
    final DbString value = new DbString();
    value.wrapString("bar");
    ColumnFamily<DbString, DbString> columnFamily =
        db.createColumnFamily(DefaultColumnFamily.DEFAULT, db.createContext(), key, value);
    columnFamily.put(key, value);

    final File snapshotDir = new File(temporaryFolder.newFolder(), "snapshot");
    db.createSnapshot(snapshotDir);
    value.wrapString("otherString");
    columnFamily.put(key, value);

    // when
    assertThat(pathName.listFiles()).isNotEmpty();
    db.close();
    db = dbFactory.createDb(snapshotDir);
    columnFamily =
        db.createColumnFamily(DefaultColumnFamily.DEFAULT, db.createContext(), key, value);

    // then
    final DbString dbString = columnFamily.get(key);

    assertThat(dbString).isNotNull();
    assertThat(dbString.toString()).isEqualTo("bar");
  }
}
