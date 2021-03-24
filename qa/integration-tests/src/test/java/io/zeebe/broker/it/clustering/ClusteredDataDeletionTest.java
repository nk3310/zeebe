/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.clustering;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.zeebe.broker.Broker;
import io.zeebe.broker.system.configuration.BrokerCfg;
import io.zeebe.broker.system.configuration.DataCfg;
import io.zeebe.broker.system.configuration.ExporterCfg;
import io.zeebe.exporter.api.Exporter;
import io.zeebe.exporter.api.context.Controller;
import io.zeebe.protocol.record.Record;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.util.unit.DataSize;

@RunWith(Parameterized.class)
public final class ClusteredDataDeletionTest {

  private static final Duration SNAPSHOT_PERIOD = Duration.ofMinutes(1);
  private static final int SEGMENT_COUNT = 10;

  @Rule public final ClusteringRule clusteringRule;

  public ClusteredDataDeletionTest(final Consumer<BrokerCfg> configurator, final String name) {
    clusteringRule = new ClusteringRule(1, 3, 3, configurator);
  }

  @Parameters(name = "{index}: {1}")
  public static Object[][] configurators() {
    return new Object[][] {
      new Object[] {
        (Consumer<BrokerCfg>) ClusteredDataDeletionTest::configureNoExporters, "no-exporter"
      },
      new Object[] {
        (Consumer<BrokerCfg>) ClusteredDataDeletionTest::configureCustomExporter,
        "updating-exporter"
      }
    };
  }

  private static void configureNoExporters(final BrokerCfg brokerCfg) {
    final DataCfg data = brokerCfg.getData();
    data.setSnapshotPeriod(SNAPSHOT_PERIOD);
    data.setLogSegmentSize(DataSize.ofKilobytes(8));
    data.setLogIndexDensity(50);
    brokerCfg.getNetwork().setMaxMessageSize(DataSize.ofKilobytes(8));

    brokerCfg.setExporters(Collections.emptyMap());
  }

  private static void configureCustomExporter(final BrokerCfg brokerCfg) {
    final DataCfg data = brokerCfg.getData();
    data.setSnapshotPeriod(SNAPSHOT_PERIOD);
    data.setLogSegmentSize(DataSize.ofKilobytes(8));
    data.setLogIndexDensity(50);
    brokerCfg.getNetwork().setMaxMessageSize(DataSize.ofKilobytes(8));

    final ExporterCfg exporterCfg = new ExporterCfg();
    exporterCfg.setClassName(TestExporter.class.getName());

    // overwrites RecordingExporter on purpose because since it doesn't update its position
    // we wouldn't be able to delete data
    brokerCfg.setExporters(Collections.singletonMap("data-delete-test-exporter", exporterCfg));
  }

  @Test
  public void shouldDeleteDataOnLeader() {
    // given
    final int leaderNodeId = clusteringRule.getLeaderForPartition(1).getNodeId();
    final Broker leader = clusteringRule.getBroker(leaderNodeId);

    fillSegments(List.of(leader), SEGMENT_COUNT);

    // when
    final var segmentCountBeforeSnapshot = getSegmentsCount(leader);

    clusteringRule.getClock().addTime(SNAPSHOT_PERIOD);
    clusteringRule.waitForSnapshotAtBroker(leader);

    // then
    await()
        .untilAsserted(
            () ->
                assertThat(getSegmentsCount(leader))
                    .describedAs("Expected less segments after a snapshot is taken")
                    .isLessThan(segmentCountBeforeSnapshot));
  }

  @Test
  public void shouldDeleteDataOnFollowers() {
    // given
    final int leaderNodeId = clusteringRule.getLeaderForPartition(1).getNodeId();
    final List<Broker> followers =
        clusteringRule.getBrokers().stream()
            .filter(b -> b.getConfig().getCluster().getNodeId() != leaderNodeId)
            .collect(Collectors.toList());

    fillSegments(followers, SEGMENT_COUNT);

    // when
    final var followerSegmentCountsBeforeSnapshot = getSegmentCountByNodeId(followers);

    clusteringRule.getClock().addTime(SNAPSHOT_PERIOD);
    followers.forEach(clusteringRule::waitForSnapshotAtBroker);

    // then
    await()
        .untilAsserted(
            () ->
                assertThat(getSegmentCountByNodeId(followers))
                    .describedAs("Expected less segments after a snapshot is taken")
                    .allSatisfy(
                        (nodeId, segmentCount) ->
                            assertThat(segmentCount)
                                .isLessThan(followerSegmentCountsBeforeSnapshot.get(nodeId))));
  }

  private Map<Integer, Integer> getSegmentCountByNodeId(final List<Broker> brokers) {
    return brokers.stream()
        .collect(
            Collectors.toMap(
                follower -> follower.getConfig().getCluster().getNodeId(), this::getSegmentsCount));
  }

  private int getSegmentsCount(final Broker broker) {
    return getSegments(broker).size();
  }

  private Collection<Path> getSegments(final Broker broker) {
    try {
      return Files.list(clusteringRule.getSegmentsDirectory(broker))
          .filter(path -> path.toString().endsWith(".log"))
          .collect(Collectors.toList());
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void fillSegments(final List<Broker> brokers, final int segmentCount) {

    while (brokers.stream().map(this::getSegmentsCount).allMatch(count -> count <= segmentCount)) {

      writeRecord();
    }
  }

  private void writeRecord() {
    clusteringRule
        .getClient()
        .newPublishMessageCommand()
        .messageName("msg")
        .correlationKey("key")
        .send()
        .join();
  }

  public static class TestExporter implements Exporter {
    static final List<Record<?>> RECORDS = new CopyOnWriteArrayList<>();
    private Controller controller;

    @Override
    public void open(final Controller controller) {
      this.controller = controller;
    }

    @Override
    public void export(final Record<?> record) {
      RECORDS.add(record);
      controller.updateLastExportedRecordPosition(record.getPosition());
    }
  }
}
