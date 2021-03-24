/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.api.topology;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.gateway.api.util.GatewayTest;
import io.zeebe.gateway.impl.broker.cluster.BrokerClusterStateImpl;
import io.zeebe.gateway.protocol.GatewayOuterClass.Partition.PartitionBrokerHealth;
import io.zeebe.gateway.protocol.GatewayOuterClass.TopologyRequest;
import org.junit.Test;

public final class TopologyTest extends GatewayTest {

  @Test
  public void shouldResponseWithInitialUnhealthyPartitions() {
    // when
    final var response = client.topology(TopologyRequest.newBuilder().build());

    // then
    assertThat(response.getBrokersList())
        .isNotEmpty()
        .allSatisfy(
            brokerInfo ->
                assertThat(brokerInfo.getPartitionsList())
                    .isNotEmpty()
                    .allSatisfy(
                        partitionPerBroker ->
                            assertThat(partitionPerBroker.getHealth())
                                .isEqualTo(PartitionBrokerHealth.UNHEALTHY)));
  }

  @Test
  public void shouldUpdatePartitionHealthHealthy() {
    // given
    final var topology = (BrokerClusterStateImpl) brokerClient.getTopologyManager().getTopology();
    topology.setPartitionHealthy(0, 1);

    // when
    final var response = client.topology(TopologyRequest.newBuilder().build());

    // then
    final var health = response.getBrokers(0).getPartitions(0).getHealth();
    assertThat(health).isEqualTo(PartitionBrokerHealth.HEALTHY);
  }

  @Test
  public void shouldUpdatePartitionHealth() {
    // given
    final var topology = (BrokerClusterStateImpl) brokerClient.getTopologyManager().getTopology();
    topology.setPartitionUnhealthy(0, 1);

    // when
    final var response = client.topology(TopologyRequest.newBuilder().build());

    // then
    final var health = response.getBrokers(0).getPartitions(0).getHealth();
    assertThat(health).isEqualTo(PartitionBrokerHealth.UNHEALTHY);
  }

  @Test
  public void shouldUpdateMultiplePartitionHealths() {
    // given
    final var topology = (BrokerClusterStateImpl) brokerClient.getTopologyManager().getTopology();
    topology.setPartitionUnhealthy(0, 1);
    topology.setPartitionHealthy(0, 6);

    // when
    final var response = client.topology(TopologyRequest.newBuilder().build());

    // then
    final var brokerInfo = response.getBrokers(0);
    assertThat(brokerInfo.getPartitions(0).getHealth()).isEqualTo(PartitionBrokerHealth.UNHEALTHY);
    assertThat(brokerInfo.getPartitions(5).getHealth()).isEqualTo(PartitionBrokerHealth.HEALTHY);
  }
}
