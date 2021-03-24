/*
 * Copyright 2015-present Open Networking Foundation
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.raft.roles;

import io.atomix.raft.RaftError;
import io.atomix.raft.RaftServer;
import io.atomix.raft.impl.RaftContext;
import io.atomix.raft.metrics.SnapshotReplicationMetrics;
import io.atomix.raft.protocol.AppendRequest;
import io.atomix.raft.protocol.AppendResponse;
import io.atomix.raft.protocol.InstallRequest;
import io.atomix.raft.protocol.InstallResponse;
import io.atomix.raft.protocol.JoinRequest;
import io.atomix.raft.protocol.JoinResponse;
import io.atomix.raft.protocol.LeaveRequest;
import io.atomix.raft.protocol.LeaveResponse;
import io.atomix.raft.protocol.PollRequest;
import io.atomix.raft.protocol.PollResponse;
import io.atomix.raft.protocol.RaftResponse;
import io.atomix.raft.protocol.ReconfigureRequest;
import io.atomix.raft.protocol.ReconfigureResponse;
import io.atomix.raft.protocol.VoteRequest;
import io.atomix.raft.protocol.VoteResponse;
import io.atomix.raft.snapshot.impl.SnapshotChunkImpl;
import io.atomix.raft.storage.log.RaftLogReader;
import io.atomix.raft.storage.log.RaftLogWriter;
import io.atomix.raft.storage.log.entry.RaftLogEntry;
import io.atomix.storage.StorageException;
import io.atomix.storage.journal.Indexed;
import io.atomix.utils.concurrent.ThreadContext;
import io.zeebe.snapshots.raft.PersistedSnapshot;
import io.zeebe.snapshots.raft.PersistedSnapshotListener;
import io.zeebe.snapshots.raft.ReceivedSnapshot;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;

/** Passive state. */
public class PassiveRole extends InactiveRole {
  private final SnapshotReplicationMetrics snapshotReplicationMetrics;

  private long pendingSnapshotStartTimestamp;
  private ReceivedSnapshot pendingSnapshot;
  private PersistedSnapshotListener snapshotListener;

  public PassiveRole(final RaftContext context) {
    super(context);

    snapshotReplicationMetrics = new SnapshotReplicationMetrics(context.getName());
    snapshotReplicationMetrics.setCount(0);
  }

  @Override
  public CompletableFuture<RaftRole> start() {
    snapshotListener = createSnapshotListener();

    return super.start()
        .thenRun(this::truncateUncommittedEntries)
        .thenRun(this::addSnapshotListener)
        .thenApply(v -> this);
  }

  @Override
  public CompletableFuture<Void> stop() {
    abortPendingSnapshots();
    if (snapshotListener != null) {
      raft.getPersistedSnapshotStore().removeSnapshotListener(snapshotListener);
    }
    return super.stop();
  }

  /** Truncates uncommitted entries from the log. */
  private void truncateUncommittedEntries() {
    if (role() == RaftServer.Role.PASSIVE) {
      final RaftLogWriter writer = raft.getLogWriter();
      writer.truncate(raft.getCommitIndex());
    }

    // to fix the edge case where we might have been stopped
    // between persisting snapshot and truncating log we need to call on restart snapshot listener
    // again, such that we truncate the log when necessary
    final var latestSnapshot = raft.getPersistedSnapshotStore().getLatestSnapshot();
    if (latestSnapshot.isPresent()) {
      final var persistedSnapshot = latestSnapshot.get();
      if (snapshotListener != null) {
        snapshotListener.onNewSnapshot(persistedSnapshot);
      }
    }
  }

  /**
   * Should be overwritten by sub classes to introduce different snapshot listener.
   *
   * <p>If null no snapshot listener will be installed
   *
   * @return the snapshot listener which will be installed
   */
  protected PersistedSnapshotListener createSnapshotListener() {
    return new ResetWriterSnapshotListener(log, raft.getThreadContext(), raft.getLogWriter());
  }

  private void addSnapshotListener() {
    if (snapshotListener != null) {
      raft.getPersistedSnapshotStore().addSnapshotListener(snapshotListener);
    }
  }

  @Override
  public RaftServer.Role role() {
    return RaftServer.Role.PASSIVE;
  }

  @Override
  public CompletableFuture<InstallResponse> onInstall(final InstallRequest request) {
    raft.checkThread();
    logRequest(request);
    updateTermAndLeader(request.currentTerm(), request.leader());

    log.debug("Received snapshot {} chunk from {}", request.index(), request.leader());

    // If the request is for a lesser term, reject the request.
    if (request.currentTerm() < raft.getTerm()) {
      return CompletableFuture.completedFuture(
          logResponse(
              InstallResponse.builder()
                  .withStatus(RaftResponse.Status.ERROR)
                  .withError(
                      RaftError.Type.ILLEGAL_MEMBER_STATE,
                      "Request term is less than the local term " + request.currentTerm())
                  .build()));
    }

    // If the index has already been applied, we have enough state to populate the state machine up
    // to this index.
    // Skip the snapshot and response successfully.
    if (raft.getCommitIndex() > request.index()) {
      return CompletableFuture.completedFuture(
          logResponse(InstallResponse.builder().withStatus(RaftResponse.Status.OK).build()));
    }

    // If a snapshot is currently being received and the snapshot versions don't match, simply
    // close the existing snapshot. This is a naive implementation that assumes that the leader
    // will be responsible in sending the correct snapshot to this server. Leaders must dictate
    // where snapshots must be sent since entries can still legitimately exist prior to the
    // snapshot,
    // and so snapshots aren't simply sent at the beginning of the follower's log, but rather the
    // leader dictates when a snapshot needs to be sent.
    if (pendingSnapshot != null && request.index() != pendingSnapshot.index()) {
      abortPendingSnapshots();
    }

    // If the snapshot already exists locally, do not overwrite it with a replicated snapshot.
    // Simply reply to the
    // request successfully.
    final var latestIndex =
        raft.getPersistedSnapshotStore()
            .getLatestSnapshot()
            .map(PersistedSnapshot::getIndex)
            .orElse(Long.MIN_VALUE);
    if (latestIndex >= request.index()) {
      abortPendingSnapshots();

      return CompletableFuture.completedFuture(
          logResponse(InstallResponse.builder().withStatus(RaftResponse.Status.OK).build()));
    }

    if (!request.complete() && request.nextChunkId() == null) {
      return CompletableFuture.completedFuture(
          logResponse(
              InstallResponse.builder()
                  .withStatus(RaftResponse.Status.ERROR)
                  .withError(
                      RaftError.Type.PROTOCOL_ERROR,
                      "Snapshot installation is not complete but did not provide any next expected chunk")
                  .build()));
    }

    final var snapshotChunk = new SnapshotChunkImpl();
    final var snapshotChunkBuffer = new UnsafeBuffer(request.data());
    if (!snapshotChunk.tryWrap(snapshotChunkBuffer)) {
      return CompletableFuture.completedFuture(
          logResponse(
              InstallResponse.builder()
                  .withStatus(RaftResponse.Status.ERROR)
                  .withError(RaftError.Type.APPLICATION_ERROR, "Failed to parse request data")
                  .build()));
    }

    // If there is no pending snapshot, create a new snapshot.
    if (pendingSnapshot == null) {
      // if we have no pending snapshot then the request must be the first chunk, otherwise we could
      // receive an old request and end up in a strange state
      if (!request.isInitial()) {
        return CompletableFuture.completedFuture(
            logResponse(
                InstallResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withError(
                        RaftError.Type.ILLEGAL_MEMBER_STATE, "Request chunk offset is invalid")
                    .build()));
      }

      pendingSnapshot =
          raft.getPersistedSnapshotStore().newReceivedSnapshot(snapshotChunk.getSnapshotId());
      log.info("Started receiving new snapshot {} from {}", pendingSnapshot, request.leader());
      pendingSnapshotStartTimestamp = System.currentTimeMillis();
      snapshotReplicationMetrics.incrementCount();
    } else {
      // skip if we already have this chunk
      if (pendingSnapshot.containsChunk(request.chunkId())) {
        return CompletableFuture.completedFuture(
            logResponse(InstallResponse.builder().withStatus(RaftResponse.Status.OK).build()));
      }

      // fail the request if this is not the expected next chunk
      if (!pendingSnapshot.isExpectedChunk(request.chunkId())) {
        return CompletableFuture.completedFuture(
            logResponse(
                InstallResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withError(
                        RaftError.Type.ILLEGAL_MEMBER_STATE,
                        "Request chunk is was received out of order")
                    .build()));
      }
    }

    boolean snapshotChunkConsumptionFailed;
    try {
      snapshotChunkConsumptionFailed = !pendingSnapshot.apply(snapshotChunk);
    } catch (final Exception e) {
      log.error("Failed to write pending snapshot chunk {}, rolling back", pendingSnapshot, e);
      snapshotChunkConsumptionFailed = true;
    }

    if (snapshotChunkConsumptionFailed) {
      abortPendingSnapshots();
      return CompletableFuture.completedFuture(
          logResponse(
              InstallResponse.builder()
                  .withStatus(RaftResponse.Status.ERROR)
                  .withError(
                      RaftError.Type.APPLICATION_ERROR, "Failed to write pending snapshot chunk")
                  .build()));
    }

    // If the snapshot is complete, store the snapshot and reset state, otherwise update the next
    // snapshot offset.
    if (request.complete()) {
      final long elapsed = System.currentTimeMillis() - pendingSnapshotStartTimestamp;
      log.debug("Committing snapshot {}", pendingSnapshot);
      try {
        final var snapshot = pendingSnapshot.persist();
        log.info("Committed snapshot {}", snapshot);
      } catch (final Exception e) {
        log.error("Failed to commit pending snapshot {}, rolling back", pendingSnapshot, e);
        abortPendingSnapshots();
        return CompletableFuture.completedFuture(
            logResponse(
                InstallResponse.builder()
                    .withStatus(RaftResponse.Status.ERROR)
                    .withError(
                        RaftError.Type.APPLICATION_ERROR, "Failed to commit pending snapshot")
                    .build()));
      }

      pendingSnapshot = null;
      pendingSnapshotStartTimestamp = 0L;
      snapshotReplicationMetrics.decrementCount();
      snapshotReplicationMetrics.observeDuration(elapsed);
    } else {
      pendingSnapshot.setNextExpected(request.nextChunkId());
    }

    return CompletableFuture.completedFuture(
        logResponse(InstallResponse.builder().withStatus(RaftResponse.Status.OK).build()));
  }

  @Override
  public CompletableFuture<JoinResponse> onJoin(final JoinRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(
          logResponse(
              JoinResponse.builder()
                  .withStatus(RaftResponse.Status.ERROR)
                  .withError(RaftError.Type.NO_LEADER)
                  .build()));
    } else {
      return forward(request, raft.getProtocol()::join)
          .exceptionally(
              error ->
                  JoinResponse.builder()
                      .withStatus(RaftResponse.Status.ERROR)
                      .withError(RaftError.Type.NO_LEADER)
                      .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<ReconfigureResponse> onReconfigure(final ReconfigureRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(
          logResponse(
              ReconfigureResponse.builder()
                  .withStatus(RaftResponse.Status.ERROR)
                  .withError(RaftError.Type.NO_LEADER)
                  .build()));
    } else {
      return forward(request, raft.getProtocol()::reconfigure)
          .exceptionally(
              error ->
                  ReconfigureResponse.builder()
                      .withStatus(RaftResponse.Status.ERROR)
                      .withError(RaftError.Type.NO_LEADER)
                      .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<LeaveResponse> onLeave(final LeaveRequest request) {
    raft.checkThread();
    logRequest(request);

    if (raft.getLeader() == null) {
      return CompletableFuture.completedFuture(
          logResponse(
              LeaveResponse.builder()
                  .withStatus(RaftResponse.Status.ERROR)
                  .withError(RaftError.Type.NO_LEADER)
                  .build()));
    } else {
      return forward(request, raft.getProtocol()::leave)
          .exceptionally(
              error ->
                  LeaveResponse.builder()
                      .withStatus(RaftResponse.Status.ERROR)
                      .withError(RaftError.Type.NO_LEADER)
                      .build())
          .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(final AppendRequest request) {
    raft.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), request.leader());
    return handleAppend(request);
  }

  @Override
  public CompletableFuture<PollResponse> onPoll(final PollRequest request) {
    raft.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(
        logResponse(
            PollResponse.builder()
                .withStatus(RaftResponse.Status.ERROR)
                .withError(RaftError.Type.ILLEGAL_MEMBER_STATE, "Cannot poll RESERVE member")
                .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> onVote(final VoteRequest request) {
    raft.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), null);

    return CompletableFuture.completedFuture(
        logResponse(
            VoteResponse.builder()
                .withStatus(RaftResponse.Status.ERROR)
                .withError(
                    RaftError.Type.ILLEGAL_MEMBER_STATE, "Cannot request vote from RESERVE member")
                .build()));
  }

  private void abortPendingSnapshots() {
    if (pendingSnapshot != null) {
      log.info("Rolling back snapshot {}", pendingSnapshot);
      try {
        pendingSnapshot.abort();
      } catch (final Exception e) {
        log.error("Failed to abort pending snapshot, clearing status anyway", e);
      }
      pendingSnapshot = null;
      pendingSnapshotStartTimestamp = 0L;

      snapshotReplicationMetrics.decrementCount();
    }

    // as a safe guard, we clean up any orphaned pending snapshots
    try {
      raft.getPersistedSnapshotStore().purgePendingSnapshots();
    } catch (final IOException e) {
      log.error(
          "Failed to purge pending snapshots, which may result in unnecessary disk usage and should be monitored",
          e);
    }
  }

  /** Handles an AppendRequest. */
  protected CompletableFuture<AppendResponse> handleAppend(final AppendRequest request) {
    final CompletableFuture<AppendResponse> future = new CompletableFuture<>();

    // Check that the term of the given request matches the local term or update the term.
    if (!checkTerm(request, future)) {
      return future;
    }

    // Check that the previous index/term matches the local log's last entry.
    if (!checkPreviousEntry(request, future)) {
      return future;
    }

    // Append the entries to the log.
    appendEntries(request, future);
    return future;
  }

  /**
   * Checks the leader's term of the given AppendRequest, returning a boolean indicating whether to
   * continue handling the request.
   */
  protected boolean checkTerm(
      final AppendRequest request, final CompletableFuture<AppendResponse> future) {
    final RaftLogWriter writer = raft.getLogWriter();
    if (request.term() < raft.getTerm()) {
      log.debug(
          "Rejected {}: request term is less than the current term ({})", request, raft.getTerm());
      return failAppend(writer.getLastIndex(), future);
    }
    return true;
  }

  /**
   * Checks the previous index of the given AppendRequest, returning a boolean indicating whether to
   * continue handling the request.
   */
  protected boolean checkPreviousEntry(
      final AppendRequest request, final CompletableFuture<AppendResponse> future) {
    final RaftLogWriter writer = raft.getLogWriter();

    // If the previous term is set, validate that it matches the local log.
    // We check the previous log term since that indicates whether any entry is present in the
    // leader's
    // log at the previous log index. prevLogTerm is 0 only when it is the first entry of the log.
    if (request.prevLogTerm() != 0) {
      // Get the last entry written to the log.
      final Indexed<RaftLogEntry> lastEntry = writer.getLastEntry();

      // If the local log is non-empty...
      if (lastEntry != null) {
        return checkPreviousEntry(request, lastEntry.index(), lastEntry.entry().term(), future);
      } else {
        final var optCurrentSnapshot = raft.getPersistedSnapshotStore().getLatestSnapshot();

        if (optCurrentSnapshot.isPresent()) {
          final var currentSnapshot = optCurrentSnapshot.get();
          return checkPreviousEntry(
              request, currentSnapshot.getIndex(), currentSnapshot.getTerm(), future);
        } else {
          // If the previous log index is set and the last entry is null and there is no snapshot,
          // fail the append.
          if (request.prevLogIndex() > 0) {
            log.debug(
                "Rejected {}: Previous index ({}) is greater than the local log's last index (0)",
                request,
                request.prevLogIndex());
            return failAppend(0, future);
          }
        }
      }
    }
    return true;
  }

  private boolean checkPreviousEntry(
      final AppendRequest request,
      final long lastEntryIndex,
      final long lastEntryTerm,
      final CompletableFuture<AppendResponse> future) {

    final RaftLogReader reader = raft.getLogReader();

    // If the previous log index is greater than the last entry index, fail the attempt.
    if (request.prevLogIndex() > lastEntryIndex) {
      log.debug(
          "Rejected {}: Previous index ({}) is greater than the local log's last index ({})",
          request,
          request.prevLogIndex(),
          lastEntryIndex);
      return failAppend(lastEntryIndex, future);
    }

    // If the previous log index is less than the last written entry index, look up the entry.
    if (request.prevLogIndex() < lastEntryIndex) {
      // Reset the reader to the previous log index.
      if (reader.getNextIndex() != request.prevLogIndex()) {
        reader.reset(request.prevLogIndex());
      }

      // The previous entry should exist in the log if we've gotten this far.
      if (!reader.hasNext()) {
        log.debug("Rejected {}: Previous entry does not exist in the local log", request);
        return failAppend(lastEntryIndex, future);
      }

      // Read the previous entry and validate that the term matches the request previous log term.
      final Indexed<RaftLogEntry> previousEntry = reader.next();
      if (request.prevLogTerm() != previousEntry.entry().term()) {
        log.debug(
            "Rejected {}: Previous entry term ({}) does not match local log's term for the same entry ({})",
            request,
            request.prevLogTerm(),
            previousEntry.entry().term());
        return failAppend(request.prevLogIndex() - 1, future);
      }
    }
    // If the previous log term doesn't equal the last entry term, fail the append, sending the
    // prior entry.
    else if (request.prevLogTerm() != lastEntryTerm) {
      log.debug(
          "Rejected {}: Previous entry term ({}) does not equal the local log's last term ({})",
          request,
          request.prevLogTerm(),
          lastEntryTerm);
      return failAppend(request.prevLogIndex() - 1, future);
    }
    return true;
  }

  /** Appends entries from the given AppendRequest. */
  protected void appendEntries(
      final AppendRequest request, final CompletableFuture<AppendResponse> future) {
    // Compute the last entry index from the previous log index and request entry count.
    final long lastEntryIndex = request.prevLogIndex() + request.entries().size();

    // Ensure the commitIndex is not increased beyond the index of the last entry in the request.
    final long commitIndex =
        Math.max(raft.getCommitIndex(), Math.min(request.commitIndex(), lastEntryIndex));

    // Track the last log index while entries are appended.
    long lastLogIndex = request.prevLogIndex();

    if (!request.entries().isEmpty()) {
      final RaftLogWriter writer = raft.getLogWriter();
      final RaftLogReader reader = raft.getLogReader();

      // If the previous term is zero, that indicates the previous index represents the beginning of
      // the log.
      // Reset the log to the previous index plus one.
      if (request.prevLogTerm() == 0) {
        log.debug("Reset first index to {}", request.prevLogIndex() + 1);
        writer.reset(request.prevLogIndex() + 1);
      }

      // Iterate through entries and append them.
      for (final RaftLogEntry entry : request.entries()) {
        final long index = ++lastLogIndex;

        // Get the last entry written to the log by the writer.
        final Indexed<RaftLogEntry> lastEntry = writer.getLastEntry();

        final boolean failedToAppend = tryToAppend(future, writer, reader, entry, index, lastEntry);
        if (failedToAppend) {
          return;
        }

        // If the last log index meets the commitIndex, break the append loop to avoid appending
        // uncommitted entries.
        if (!role().active() && index == commitIndex) {
          break;
        }
      }
    }

    // Set the first commit index.
    raft.setFirstCommitIndex(request.commitIndex());

    // Update the context commit and global indices.
    final long previousCommitIndex = raft.setCommitIndex(commitIndex);
    if (previousCommitIndex < commitIndex) {
      log.trace("Committed entries up to index {}", commitIndex);
      raft.notifyCommitListeners(commitIndex);
    }

    // Make sure all entries are flushed before ack to ensure we have persisted what we acknowledge
    if (raft.getLog().shouldFlushExplicitly()) {
      raft.getLogWriter().flush();
    }

    // Return a successful append response.
    succeedAppend(lastLogIndex, future);
  }

  private boolean tryToAppend(
      final CompletableFuture<AppendResponse> future,
      final RaftLogWriter writer,
      final RaftLogReader reader,
      final RaftLogEntry entry,
      final long index,
      final Indexed<RaftLogEntry> lastEntry) {
    boolean failedToAppend = false;
    if (lastEntry != null) {
      // If the last written entry index is greater than the next append entry index,
      // we need to validate that the entry that's already in the log matches this entry.
      if (lastEntry.index() > index) {
        failedToAppend = !replaceExistingEntry(future, writer, reader, entry, index);
      } else if (lastEntry.index() == index) {
        // If the last written entry is equal to the append entry index, we don't need
        // to read the entry from disk and can just compare the last entry in the writer.

        // If the last entry term doesn't match the leader's term for the same entry, truncate
        // the log and append the leader's entry.
        if (lastEntry.entry().term() != entry.term()) {
          writer.truncate(index - 1);
          failedToAppend = !appendEntry(index, entry, writer, future);
        }
      } else { // Otherwise, this entry is being appended at the end of the log.
        failedToAppend = !appendEntry(future, writer, entry, index, lastEntry);
      }
    } else { // Otherwise, if the last entry is null just append the entry and log a message.
      failedToAppend = !appendEntry(index, entry, writer, future);
    }
    return failedToAppend;
  }

  private boolean appendEntry(
      final CompletableFuture<AppendResponse> future,
      final RaftLogWriter writer,
      final RaftLogEntry entry,
      final long index,
      final Indexed<RaftLogEntry> lastEntry) {
    // If the last entry index isn't the previous index, throw an exception because
    // something crazy happened!
    if (lastEntry.index() != index - 1) {
      throw new IllegalStateException(
          "Log writer inconsistent with next append entry index " + index);
    }

    // Append the entry and log a message.
    return appendEntry(index, entry, writer, future);
  }

  private boolean replaceExistingEntry(
      final CompletableFuture<AppendResponse> future,
      final RaftLogWriter writer,
      final RaftLogReader reader,
      final RaftLogEntry entry,
      final long index) {
    // Reset the reader to the current entry index.
    if (reader.getNextIndex() != index) {
      reader.reset(index);
    }

    // If the reader does not have any next entry, that indicates an inconsistency between
    // the reader and writer.
    if (!reader.hasNext()) {
      throw new IllegalStateException("Log reader inconsistent with log writer");
    }

    // Read the existing entry from the log.
    final Indexed<RaftLogEntry> existingEntry = reader.next();

    // If the existing entry term doesn't match the leader's term for the same entry,
    // truncate
    // the log and append the leader's entry.
    if (existingEntry.entry().term() != entry.term()) {
      writer.truncate(index - 1);
      if (!appendEntry(index, entry, writer, future)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Attempts to append an entry, returning {@code false} if the append fails due to an {@link
   * StorageException.OutOfDiskSpace} exception.
   */
  private boolean appendEntry(
      final long index,
      final RaftLogEntry entry,
      final RaftLogWriter writer,
      final CompletableFuture<AppendResponse> future) {
    try {
      final Indexed<RaftLogEntry> indexed = writer.append(entry);
      log.trace("Appended {}", indexed);
      raft.getReplicationMetrics().setAppendIndex(indexed.index());
    } catch (final StorageException.TooLarge e) {
      log.warn(
          "Entry size exceeds maximum allowed bytes. Ensure Raft storage configuration is consistent on all nodes!");
      return false;
    } catch (final StorageException.OutOfDiskSpace e) {
      log.trace("Append failed: {}", e);
      raft.getLogCompactor().compact();
      failAppend(index - 1, future);
      return false;
    }
    return true;
  }

  /**
   * Returns a failed append response.
   *
   * @param lastLogIndex the last log index
   * @param future the append response future
   * @return the append response status
   */
  protected boolean failAppend(
      final long lastLogIndex, final CompletableFuture<AppendResponse> future) {
    return completeAppend(false, lastLogIndex, future);
  }

  /**
   * Returns a successful append response.
   *
   * @param lastLogIndex the last log index
   * @param future the append response future
   * @return the append response status
   */
  protected boolean succeedAppend(
      final long lastLogIndex, final CompletableFuture<AppendResponse> future) {
    return completeAppend(true, lastLogIndex, future);
  }

  /**
   * Returns a successful append response.
   *
   * @param succeeded whether the append succeeded
   * @param lastLogIndex the last log index
   * @param future the append response future
   * @return the append response status
   */
  protected boolean completeAppend(
      final boolean succeeded,
      final long lastLogIndex,
      final CompletableFuture<AppendResponse> future) {
    future.complete(
        logResponse(
            AppendResponse.builder()
                .withStatus(RaftResponse.Status.OK)
                .withTerm(raft.getTerm())
                .withSucceeded(succeeded)
                .withLastLogIndex(lastLogIndex)
                .withLastSnapshotIndex(
                    raft.getPersistedSnapshotStore()
                        .getLatestSnapshot()
                        .map(PersistedSnapshot::getIndex)
                        .orElse(0L))
                .build()));
    return succeeded;
  }

  private static final class ResetWriterSnapshotListener implements PersistedSnapshotListener {
    private final ThreadContext threadContext;
    private final RaftLogWriter logWriter;
    private final Logger log;

    ResetWriterSnapshotListener(
        final Logger log, final ThreadContext threadContext, final RaftLogWriter logWriter) {
      this.log = log;
      this.threadContext = threadContext;
      this.logWriter = logWriter;
    }

    @Override
    public void onNewSnapshot(final PersistedSnapshot persistedSnapshot) {
      if (threadContext.isCurrentContext()) {
        // this is called after the snapshot is commited
        // on install requests and on Zeebe snapshot replication

        final var index = persistedSnapshot.getIndex();
        // It might happen that the last index is far behind our current snapshot index.
        // E. g. on slower followers, we need to throw away the existing log,
        // otherwise we might end with an inconsistent log (gaps between last index and
        // snapshot index)
        final var lastIndex = logWriter.getLastIndex();
        if (lastIndex < index) {
          log.info(
              "Delete existing log (lastIndex '{}') and replace with received snapshot (index '{}')",
              lastIndex,
              index);
          logWriter.reset(index + 1);
        }
      } else {
        threadContext.execute(() -> onNewSnapshot(persistedSnapshot));
      }
    }
  }
}
