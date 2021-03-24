/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.system;

import static org.assertj.core.api.Assertions.assertThat;

import io.atomix.raft.RaftServer.Role;
import io.zeebe.broker.Broker;
import io.zeebe.broker.it.clustering.ClusteringRule;
import io.zeebe.broker.it.util.GrpcClientRule;
import io.zeebe.broker.system.management.BrokerAdminService;
import io.zeebe.broker.system.management.PartitionStatus;
import io.zeebe.engine.processing.streamprocessor.StreamProcessor.Phase;
import java.time.Duration;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

public class BrokerAdminServiceClusterTest {

  private static final int PARTITION_ID = 1;
  private final Timeout testTimeout = Timeout.seconds(60);
  private final ClusteringRule clusteringRule =
      new ClusteringRule(
          1,
          3,
          3,
          cfg -> {
            cfg.getData().setLogIndexDensity(1);
            cfg.getData().setSnapshotPeriod(Duration.ofMinutes(15));
          });
  private final GrpcClientRule clientRule = new GrpcClientRule(clusteringRule);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(testTimeout).around(clusteringRule).around(clientRule);

  private BrokerAdminService leaderAdminService;
  private Broker leader;

  @Before
  public void before() {
    leader = clusteringRule.getBroker(clusteringRule.getLeaderForPartition(1).getNodeId());
    leaderAdminService = leader.getBrokerAdminService();
  }

  @Test
  public void shouldReportPartitionStatusOnFollowersAndLeader() {
    // given
    final var followers =
        clusteringRule.getOtherBrokerObjects(
            clusteringRule.getLeaderForPartition(PARTITION_ID).getNodeId());

    // when
    final var followerStatus =
        followers.stream()
            .map(Broker::getBrokerAdminService)
            .map(BrokerAdminService::getPartitionStatus)
            .map(status -> status.get(1));

    final var leaderStatus = leaderAdminService.getPartitionStatus().get(PARTITION_ID);

    // then
    followerStatus.forEach(
        partitionStatus -> {
          assertThat(partitionStatus.getRole()).isEqualTo(Role.FOLLOWER);
          assertThat(partitionStatus.getProcessedPosition()).isNull();
          assertThat(partitionStatus.getSnapshotId()).isNull();
          assertThat(partitionStatus.getProcessedPositionInSnapshot()).isNull();
          assertThat(partitionStatus.getStreamProcessorPhase()).isNull();
        });

    assertThat(leaderStatus.getRole()).isEqualTo(Role.LEADER);
    assertThat(leaderStatus.getProcessedPosition()).isEqualTo(-1);
    assertThat(leaderStatus.getSnapshotId()).isNull();
    assertThat(leaderStatus.getProcessedPositionInSnapshot()).isNull();
    assertThat(leaderStatus.getStreamProcessorPhase()).isEqualTo(Phase.PROCESSING);
  }

  @Test
  public void shouldReportPartitionStatusWithSnapshotOnFollowers() {
    // given
    clientRule.createSingleJob("test");
    leaderAdminService.takeSnapshot();

    // when
    waitForSnapshotAtBroker(leaderAdminService);

    // then
    final var leaderStatus = leaderAdminService.getPartitionStatus().get(PARTITION_ID);
    clusteringRule.getBrokers().stream()
        .map(Broker::getBrokerAdminService)
        .forEach(
            adminService ->
                Awaitility.await()
                    .untilAsserted(
                        () -> assertFollowerSnapshotEqualToLeader(adminService, leaderStatus)));
  }

  @Test
  public void shouldPauseAfterLeaderChange() {
    // given
    clusteringRule.getBrokers().stream()
        .map(Broker::getBrokerAdminService)
        .forEach(BrokerAdminService::pauseStreamProcessing);

    // when
    assertStreamProcessorPhase(leaderAdminService, Phase.PAUSED);
    clusteringRule.stopBrokerAndAwaitNewLeader(leader.getConfig().getCluster().getNodeId());

    // then
    final var newLeaderAdminService =
        clusteringRule
            .getBroker(clusteringRule.getLeaderForPartition(1).getNodeId())
            .getBrokerAdminService();
    assertStreamProcessorPhase(newLeaderAdminService, Phase.PAUSED);
  }

  private void assertFollowerSnapshotEqualToLeader(
      final BrokerAdminService followerService, final PartitionStatus leaderStatus) {
    assertThat(followerService.getPartitionStatus().get(1).getSnapshotId())
        .isEqualTo(leaderStatus.getSnapshotId());
  }

  private void waitForSnapshotAtBroker(final BrokerAdminService adminService) {
    Awaitility.await()
        .untilAsserted(
            () ->
                adminService
                    .getPartitionStatus()
                    .values()
                    .forEach(
                        status -> assertThat(status.getProcessedPositionInSnapshot()).isNotNull()));
  }

  private void assertStreamProcessorPhase(
      final BrokerAdminService brokerAdminService, final Phase expected) {
    Awaitility.await()
        .untilAsserted(
            () ->
                brokerAdminService
                    .getPartitionStatus()
                    .forEach(
                        (p, status) ->
                            assertThat(status.getStreamProcessorPhase()).isEqualTo(expected)));
  }
}
