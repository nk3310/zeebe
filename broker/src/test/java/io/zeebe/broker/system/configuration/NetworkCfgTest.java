/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.gateway.impl.configuration.NetworkCfg;
import java.net.InetSocketAddress;
import org.assertj.core.internal.bytebuddy.utility.RandomString;
import org.junit.Test;

public class NetworkCfgTest {

  @Test
  public void shouldNotFailOnLongHostName() {
    // given
    final var port = 1000;
    final var hostName = RandomString.make(139);
    final var networkCfg = new NetworkCfg();
    networkCfg.setPort(port);

    // when
    networkCfg.setHost(hostName);
    final var socketAddress = networkCfg.toSocketAddress();

    // then
    assertThat(socketAddress).isEqualTo(new InetSocketAddress(hostName, port));
  }
}
