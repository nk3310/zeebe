/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.deployment.distribute;

import io.zeebe.db.DbValue;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.DocumentProperty;
import io.zeebe.msgpack.property.IntegerProperty;
import io.zeebe.msgpack.property.LongProperty;
import org.agrona.DirectBuffer;

public class PendingDeploymentDistribution extends UnpackedObject implements DbValue {

  private final IntegerProperty distributionCountProp = new IntegerProperty("distributionCount", 0);
  private final LongProperty sourcePositionProp = new LongProperty("sourcePosition", -1L);
  private final DocumentProperty deploymentProp = new DocumentProperty("deployment");

  public PendingDeploymentDistribution(
      final DirectBuffer deployment, final long sourcePosition, final int distributionCount) {
    declareProperty(distributionCountProp)
        .declareProperty(sourcePositionProp)
        .declareProperty(deploymentProp);

    deploymentProp.setValue(deployment);
    sourcePositionProp.setValue(sourcePosition);
    distributionCountProp.setValue(distributionCount);
  }

  public int decrementDistributionCount() {
    return distributionCountProp.decrement();
  }

  public DirectBuffer getDeployment() {
    return deploymentProp.getValue();
  }

  public long getSourcePosition() {
    return sourcePositionProp.getValue();
  }
}
