/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.api.job;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.gateway.api.util.GatewayTest;
import io.zeebe.gateway.impl.broker.request.BrokerUpdateJobRetriesRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.UpdateJobRetriesRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.UpdateJobRetriesResponse;
import io.zeebe.protocol.impl.record.value.job.JobRecord;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.JobIntent;
import org.junit.Test;

public final class UpdateJobRetriesTest extends GatewayTest {

  @Test
  public void shouldMapRequestAndResponse() {
    // given
    final UpdateJobRetriesStub stub = new UpdateJobRetriesStub();
    stub.registerWith(brokerClient);

    final int retries = 123;

    final UpdateJobRetriesRequest request =
        UpdateJobRetriesRequest.newBuilder().setJobKey(stub.getKey()).setRetries(retries).build();

    // when
    final UpdateJobRetriesResponse response = client.updateJobRetries(request);

    // then
    assertThat(response).isNotNull();

    final BrokerUpdateJobRetriesRequest brokerRequest = brokerClient.getSingleBrokerRequest();
    assertThat(brokerRequest.getKey()).isEqualTo(stub.getKey());
    assertThat(brokerRequest.getIntent()).isEqualTo(JobIntent.UPDATE_RETRIES);
    assertThat(brokerRequest.getValueType()).isEqualTo(ValueType.JOB);

    final JobRecord brokerRequestValue = brokerRequest.getRequestWriter();
    assertThat(brokerRequestValue.getRetries()).isEqualTo(retries);
  }
}
