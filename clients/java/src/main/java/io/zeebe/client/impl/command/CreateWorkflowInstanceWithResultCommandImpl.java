/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
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
package io.zeebe.client.impl.command;

import io.grpc.stub.StreamObserver;
import io.zeebe.client.api.ZeebeFuture;
import io.zeebe.client.api.command.CreateWorkflowInstanceCommandStep1.CreateWorkflowInstanceWithResultCommandStep1;
import io.zeebe.client.api.command.FinalCommandStep;
import io.zeebe.client.api.response.WorkflowInstanceResult;
import io.zeebe.client.impl.RetriableClientFutureImpl;
import io.zeebe.client.impl.ZeebeObjectMapper;
import io.zeebe.client.impl.response.CreateWorkflowInstanceWithResultResponseImpl;
import io.zeebe.gateway.protocol.GatewayGrpc.GatewayStub;
import io.zeebe.gateway.protocol.GatewayOuterClass;
import io.zeebe.gateway.protocol.GatewayOuterClass.CreateWorkflowInstanceRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.CreateWorkflowInstanceWithResultRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.CreateWorkflowInstanceWithResultRequest.Builder;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public final class CreateWorkflowInstanceWithResultCommandImpl
    implements CreateWorkflowInstanceWithResultCommandStep1 {

  private static final Duration DEADLINE_OFFSET = Duration.ofSeconds(10);
  private final ZeebeObjectMapper objectMapper;
  private final GatewayStub asyncStub;
  private final CreateWorkflowInstanceRequest.Builder createWorkflowInstanceRequestBuilder;
  private final Builder builder;
  private final Predicate<Throwable> retryPredicate;
  private Duration requestTimeout;

  public CreateWorkflowInstanceWithResultCommandImpl(
      final ZeebeObjectMapper objectMapper,
      final GatewayStub asyncStub,
      final CreateWorkflowInstanceRequest.Builder builder,
      final Predicate<Throwable> retryPredicate,
      final Duration requestTimeout) {
    this.objectMapper = objectMapper;
    this.asyncStub = asyncStub;
    createWorkflowInstanceRequestBuilder = builder;
    this.retryPredicate = retryPredicate;
    this.requestTimeout = requestTimeout;
    this.builder = CreateWorkflowInstanceWithResultRequest.newBuilder();
  }

  @Override
  public FinalCommandStep<WorkflowInstanceResult> requestTimeout(final Duration requestTimeout) {
    this.requestTimeout = requestTimeout;
    builder.setRequestTimeout(requestTimeout.toMillis());
    return this;
  }

  @Override
  public ZeebeFuture<WorkflowInstanceResult> send() {
    final CreateWorkflowInstanceWithResultRequest request =
        builder
            .setRequest(createWorkflowInstanceRequestBuilder)
            .setRequestTimeout(requestTimeout.toMillis())
            .build();

    final RetriableClientFutureImpl<
            WorkflowInstanceResult, GatewayOuterClass.CreateWorkflowInstanceWithResultResponse>
        future =
            new RetriableClientFutureImpl<>(
                response ->
                    new CreateWorkflowInstanceWithResultResponseImpl(objectMapper, response),
                retryPredicate,
                streamObserver -> send(request, streamObserver));

    send(request, future);
    return future;
  }

  private void send(
      final CreateWorkflowInstanceWithResultRequest request,
      final StreamObserver<GatewayOuterClass.CreateWorkflowInstanceWithResultResponse> future) {
    asyncStub
        .withDeadlineAfter(requestTimeout.plus(DEADLINE_OFFSET).toMillis(), TimeUnit.MILLISECONDS)
        .createWorkflowInstanceWithResult(request, future);
  }

  @Override
  public CreateWorkflowInstanceWithResultCommandStep1 fetchVariables(
      final List<String> fetchVariables) {
    builder.addAllFetchVariables(fetchVariables);
    return this;
  }

  @Override
  public CreateWorkflowInstanceWithResultCommandStep1 fetchVariables(
      final String... fetchVariables) {
    builder.addAllFetchVariables(Arrays.asList(fetchVariables));
    return this;
  }
}
