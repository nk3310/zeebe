/*
 * Copyright 2017-present Open Networking Foundation
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.raft.partition.impl;

import io.atomix.cluster.MemberId;
import io.atomix.raft.RaftError;
import io.atomix.raft.cluster.RaftMember;
import io.atomix.raft.cluster.impl.DefaultRaftMember;
import io.atomix.raft.protocol.AppendRequest;
import io.atomix.raft.protocol.AppendResponse;
import io.atomix.raft.protocol.ConfigureRequest;
import io.atomix.raft.protocol.ConfigureResponse;
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
import io.atomix.raft.storage.log.entry.ConfigurationEntry;
import io.atomix.raft.storage.log.entry.InitializeEntry;
import io.atomix.raft.storage.system.Configuration;
import io.atomix.raft.zeebe.ZeebeEntry;
import io.atomix.utils.serializer.FallbackNamespace;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.NamespaceImpl.Builder;
import io.atomix.utils.serializer.Namespaces;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;

/** Storage serializer namespaces. */
public final class RaftNamespaces {

  /** Raft protocol namespace. */
  public static final Namespace RAFT_PROTOCOL =
      new FallbackNamespace(
          new Builder()
              .register(Namespaces.BASIC)
              .nextId(Namespaces.BEGIN_USER_CUSTOM_ID)
              .register(Void.class) // OpenSessionRequest
              .register(Void.class) // OpenSessionResponse
              .register(Void.class) // CloseSessionRequest
              .register(Void.class) // CloseSessionResponse
              .register(Void.class) // KeepAliveRequest
              .register(Void.class) // KeepAliveResponse
              .register(Void.class) // HeartbeatRequest
              .register(Void.class) // HeartbeatResponse
              .register(Void.class) // QueryRequest
              .register(Void.class) // QueryResponse
              .register(Void.class) // CommandRequest
              .register(Void.class) // CommandResponse
              .register(Void.class) // MetadataRequest
              .register(Void.class) // MetadataResponse
              .register(JoinRequest.class)
              .register(JoinResponse.class)
              .register(LeaveRequest.class)
              .register(LeaveResponse.class)
              .register(ConfigureRequest.class)
              .register(ConfigureResponse.class)
              .register(ReconfigureRequest.class)
              .register(ReconfigureResponse.class)
              .register(InstallRequest.class)
              .register(InstallResponse.class)
              .register(PollRequest.class)
              .register(PollResponse.class)
              .register(VoteRequest.class)
              .register(VoteResponse.class)
              .register(AppendRequest.class)
              .register(AppendResponse.class)
              .register(Void.class) // PublishRequest
              .register(Void.class) // ResetRequest
              .register(RaftResponse.Status.class)
              .register(RaftError.class)
              .register(RaftError.Type.class)
              .register(Void.class) // ReadConsistency
              .register(Void.class) // SessionMetadata
              .register(Void.class) // CloseSessionEntry
              .register(Void.class) // CommandEntry
              .register(ConfigurationEntry.class)
              .register(InitializeEntry.class)
              .register(Void.class) // KeepAliveEntry
              .register(Void.class) // MetadataEntry
              .register(Void.class) // OpenSessionEntry
              .register(Void.class) // QueryEntry
              .register(Void.class) // PrimitiveOperation
              .register(Void.class) // PrimitiveEvent
              .register(Void.class) // DefaultEventType
              .register(Void.class) // DefaultOperationId
              .register(Void.class) // OperationType
              .register(Void.class) // ReadConsistency
              .register(ArrayList.class)
              .register(LinkedList.class)
              .register(Collections.emptyList().getClass())
              .register(HashSet.class)
              .register(DefaultRaftMember.class)
              .register(MemberId.class)
              .register(Void.class) // SessionId
              .register(RaftMember.Type.class)
              .register(Instant.class)
              .register(Configuration.class)
              .register(ZeebeEntry.class)
              .name("RaftProtocol"));

  /**
   * Raft storage namespace.
   *
   * <p>*Be aware* we use the Void type for replaced/removed types to keep the id's of used types,
   * otherwise we break compatibility.
   */
  public static final FallbackNamespace RAFT_STORAGE =
      new FallbackNamespace(
          new Builder()
              .register(Namespaces.BASIC)
              .nextId(Namespaces.BEGIN_USER_CUSTOM_ID + 100)
              .register(Void.class) // CloseSessionEntry
              .register(Void.class) // CommandEntry
              .register(ConfigurationEntry.class)
              .register(InitializeEntry.class)
              .register(Void.class) // KeepAliveEntry
              .register(Void.class) // MetadataEntry
              .register(Void.class) // OpenSessionEntry
              .register(Void.class) // QueryEntry
              .register(Void.class) // PrimitiveOperation
              .register(Void.class) // DefaultOperationId
              .register(Void.class) // OperationType
              .register(Void.class) // ReadConsistency
              .register(ArrayList.class)
              .register(HashSet.class)
              .register(DefaultRaftMember.class)
              .register(MemberId.class)
              .register(RaftMember.Type.class)
              .register(Instant.class)
              .register(Configuration.class)
              .register(ZeebeEntry.class)
              .name("RaftStorage"));

  private RaftNamespaces() {}
}
