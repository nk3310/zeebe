/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.snapshots.broker.impl;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.zeebe.snapshots.broker.ConstructableSnapshotStore;
import io.zeebe.snapshots.raft.PersistedSnapshotListener;
import io.zeebe.util.FileUtil;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileBasedTransientSnapshotTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private ConstructableSnapshotStore persistedSnapshotStore;
  private Path snapshotsDir;
  private Path pendingSnapshotsDir;

  @Before
  public void before() {
    final FileBasedSnapshotStoreFactory factory = new FileBasedSnapshotStoreFactory();
    final String partitionName = "1";
    final File root = temporaryFolder.getRoot();

    factory.createReceivableSnapshotStore(root.toPath(), partitionName);
    persistedSnapshotStore = factory.getConstructableSnapshotStore(partitionName);

    snapshotsDir =
        temporaryFolder
            .getRoot()
            .toPath()
            .resolve(FileBasedSnapshotStoreFactory.SNAPSHOTS_DIRECTORY);
    pendingSnapshotsDir =
        temporaryFolder.getRoot().toPath().resolve(FileBasedSnapshotStoreFactory.PENDING_DIRECTORY);
  }

  @Test
  public void shouldNotCreateDirForTakeTransientSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;

    // when
    persistedSnapshotStore.newTransientSnapshot(index, term, 1, 0);

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();
    assertThat(snapshotsDir.toFile().listFiles()).isEmpty();
  }

  @Test
  public void shouldBeAbleToAbortNotStartedSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var transientSnapshot = persistedSnapshotStore.newTransientSnapshot(index, term, 1, 0);

    // when
    transientSnapshot.orElseThrow().abort();

    // then
    assertThat(snapshotsDir.toFile().listFiles()).isEmpty();
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();
  }

  @Test
  public void shouldTakeTransientSnapshot() {
    // given
    final var index = 1L;
    final var term = 2L;
    final var transientSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index, term, 3, 4).orElseThrow();

    // when
    transientSnapshot.take(this::createSnapshotDir);

    // then
    assertThat(snapshotsDir.toFile().listFiles()).isEmpty();
    final var snapshotDirs = pendingSnapshotsDir.toFile().listFiles();
    assertThat(snapshotDirs).isNotNull().hasSize(1);

    final var pendingSnapshotDir = snapshotDirs[0];
    final FileBasedSnapshotMetadata pendingSnapshotId =
        FileBasedSnapshotMetadata.ofFileName(pendingSnapshotDir.getName()).orElseThrow();
    assertThat(pendingSnapshotId.getIndex()).isEqualTo(1);
    assertThat(pendingSnapshotId.getTerm()).isEqualTo(2);
    assertThat(pendingSnapshotId.getProcessedPosition()).isEqualTo(3);
    assertThat(pendingSnapshotId.getExportedPosition()).isEqualTo(4);
    assertThat(pendingSnapshotDir.listFiles())
        .isNotNull()
        .extracting(File::getName)
        .containsExactly("file1.txt");
  }

  @Test
  public void shouldAbortAndDeleteTransientSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var transientSnapshot = persistedSnapshotStore.newTransientSnapshot(index, term, 1, 0);
    transientSnapshot.orElseThrow().take(this::createSnapshotDir);

    // when
    transientSnapshot.get().abort();

    // then
    assertThat(snapshotsDir.toFile().listFiles()).isEmpty();
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();
  }

  @Test
  public void shouldPurgePendingOnStore() throws Exception {
    // given
    final var index = 1L;
    final var term = 0L;
    final var transientSnapshot = persistedSnapshotStore.newTransientSnapshot(index, term, 1, 0);
    transientSnapshot.orElseThrow().take(this::createSnapshotDir);

    // when
    persistedSnapshotStore.purgePendingSnapshots();

    // then
    assertThat(snapshotsDir.toFile().listFiles()).isEmpty();
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();
  }

  @Test
  public void shouldNotDeletePersistedSnapshotOnPurge() throws Exception {
    // given
    final var index = 1L;
    final var term = 0L;
    final var transientSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index, term, 1, 0).orElseThrow();
    transientSnapshot.take(this::createSnapshotDir);
    final var persistSnapshot = transientSnapshot.persist();

    // when
    persistedSnapshotStore.purgePendingSnapshots();

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();
    final var snapshotDirs = snapshotsDir.toFile().listFiles();
    assertThat(snapshotDirs).isNotNull().hasSize(1);

    final var pendingSnapshotDir = snapshotDirs[0];
    assertThat(pendingSnapshotDir.getName()).isEqualTo(persistSnapshot.getId());
    assertThat(pendingSnapshotDir.listFiles())
        .isNotNull()
        .extracting(File::getName)
        .containsExactly("file1.txt");
  }

  @Test
  public void shouldCommitTakenSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var transientSnapshot = persistedSnapshotStore.newTransientSnapshot(index, term, 1, 0);
    transientSnapshot.orElseThrow().take(this::createSnapshotDir);

    // when
    final var persistedSnapshot = transientSnapshot.get().persist();

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();

    final var snapshotDirs = snapshotsDir.toFile().listFiles();
    assertThat(snapshotDirs).isNotNull().hasSize(1);

    final var committedSnapshotDir = snapshotDirs[0];
    assertThat(committedSnapshotDir.getName()).isEqualTo(persistedSnapshot.getId());
    assertThat(committedSnapshotDir.listFiles())
        .isNotNull()
        .extracting(File::getName)
        .containsExactly("file1.txt");
  }

  @Test
  public void shouldReplaceSnapshotOnNextSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var oldTransientSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index, term, 1, 0).orElseThrow();
    oldTransientSnapshot.take(this::createSnapshotDir);
    oldTransientSnapshot.persist();

    // when
    final var newSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index + 1, term, 1, 0).orElseThrow();
    newSnapshot.take(this::createSnapshotDir);
    newSnapshot.persist();

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();

    final var snapshotDirs = snapshotsDir.toFile().listFiles();
    assertThat(snapshotDirs).isNotNull().hasSize(1);

    final var committedSnapshotDir = snapshotDirs[0];
    assertThat(
            FileBasedSnapshotMetadata.ofFileName(committedSnapshotDir.getName())
                .orElseThrow()
                .getIndex())
        .isEqualTo(2);
    assertThat(committedSnapshotDir.listFiles())
        .isNotNull()
        .extracting(File::getName)
        .containsExactly("file1.txt");
  }

  @Test
  public void shouldRemovePendingSnapshotOnCommittingSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var oldTransientSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index, term, 1, 0).orElseThrow();
    oldTransientSnapshot.take(this::createSnapshotDir);

    // when
    final var newSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index + 1, term, 1, 0).orElseThrow();
    newSnapshot.take(this::createSnapshotDir);
    newSnapshot.persist();

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();

    final var snapshotDirs = snapshotsDir.toFile().listFiles();
    assertThat(snapshotDirs).isNotNull().hasSize(1);

    final var committedSnapshotDir = snapshotDirs[0];
    assertThat(
            FileBasedSnapshotMetadata.ofFileName(committedSnapshotDir.getName())
                .orElseThrow()
                .getIndex())
        .isEqualTo(2);
    assertThat(committedSnapshotDir.listFiles())
        .isNotNull()
        .extracting(File::getName)
        .containsExactly("file1.txt");
  }

  @Test
  public void shouldNotRemovePendingSnapshotOnCommittingSnapshotWhenHigher() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var oldTransientSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index + 1, term, 1, 0).orElseThrow();
    oldTransientSnapshot.take(this::createSnapshotDir);

    // when
    final var newSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index, term, 1, 0).orElseThrow();
    newSnapshot.take(this::createSnapshotDir);
    final var newSnapshotId = newSnapshot.persist().getId();

    // then
    final var pendingSnapshotDirs = pendingSnapshotsDir.toFile().listFiles();
    assertThat(pendingSnapshotDirs).isNotNull().hasSize(1);

    final var pendingSnapshotDir = pendingSnapshotDirs[0];
    assertThat(
            FileBasedSnapshotMetadata.ofFileName(pendingSnapshotDir.getName())
                .orElseThrow()
                .getIndex())
        .isEqualTo(2);
    assertThat(pendingSnapshotDir.listFiles())
        .isNotNull()
        .extracting(File::getName)
        .containsExactly("file1.txt");

    final var snapshotDirs = snapshotsDir.toFile().listFiles();
    assertThat(snapshotDirs).isNotNull().hasSize(1);

    final var committedSnapshotDir = snapshotDirs[0];
    assertThat(committedSnapshotDir.getName()).isEqualTo(newSnapshotId);
    assertThat(committedSnapshotDir.listFiles())
        .isNotNull()
        .extracting(File::getName)
        .containsExactly("file1.txt");
  }

  @Test
  public void shouldCleanUpPendingDirOnFailingTakeSnapshot() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var oldTransientSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index, term, 1, 0).orElseThrow();

    // when
    oldTransientSnapshot.take(
        path -> {
          try {
            FileUtil.ensureDirectoryExists(path);
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
          return false;
        });

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();
    assertThat(snapshotsDir.toFile().listFiles()).isEmpty();
  }

  @Test
  public void shouldCleanUpPendingDirOnException() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var oldTransientSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index, term, 1, 0).orElseThrow();

    // when
    oldTransientSnapshot.take(
        path -> {
          try {
            FileUtil.ensureDirectoryExists(path);
            throw new RuntimeException("EXPECTED");
          } catch (final IOException e) {
            throw new UncheckedIOException(e);
          }
        });

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();
    assertThat(snapshotsDir.toFile().listFiles()).isEmpty();
  }

  @Test
  public void shouldNotifyListenersOnNewSnapshot() {
    // given
    final var listener = mock(PersistedSnapshotListener.class);
    final var index = 1L;
    final var term = 0L;
    final var transientSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index, term, 1, 0).orElseThrow();
    persistedSnapshotStore.addSnapshotListener(listener);
    transientSnapshot.take(this::createSnapshotDir);

    // when
    final var persistedSnapshot = transientSnapshot.persist();

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();

    verify(listener, times(1)).onNewSnapshot(eq(persistedSnapshot));
  }

  @Test
  public void shouldNotNotifyListenersOnNewSnapshotWhenDeregistered() {
    // given
    final var listener = mock(PersistedSnapshotListener.class);
    final var index = 1L;
    final var term = 0L;
    final var transientSnapshot =
        persistedSnapshotStore.newTransientSnapshot(index, term, 1, 0).orElseThrow();
    persistedSnapshotStore.addSnapshotListener(listener);
    persistedSnapshotStore.removeSnapshotListener(listener);
    transientSnapshot.take(this::createSnapshotDir);

    // when
    final var persistedSnapshot = transientSnapshot.persist();

    // then
    assertThat(pendingSnapshotsDir.toFile().listFiles()).isEmpty();

    verify(listener, times(0)).onNewSnapshot(eq(persistedSnapshot));
  }

  @Test
  public void shouldNotTakeSnapshotIfIdAlreadyExists() {
    // given
    final var index = 1L;
    final var term = 0L;
    final var processedPosition = 2;
    final var exporterPosition = 3;
    final var transientSnapshot =
        persistedSnapshotStore
            .newTransientSnapshot(index, term, processedPosition, exporterPosition)
            .orElseThrow();
    transientSnapshot.take(this::createSnapshotDir);
    // when
    transientSnapshot.persist();

    // then
    assertThat(
            persistedSnapshotStore.newTransientSnapshot(
                index, term, processedPosition, exporterPosition))
        .isEmpty();
  }

  private boolean createSnapshotDir(final Path path) {
    try {
      FileUtil.ensureDirectoryExists(path);
      Files.write(
          path.resolve("file1.txt"),
          "This is the content".getBytes(),
          CREATE_NEW,
          StandardOpenOption.WRITE);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    return true;
  }
}
