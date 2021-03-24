/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.message;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.DbContext;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.DbCompositeKey;
import io.zeebe.db.impl.DbLong;
import io.zeebe.db.impl.DbNil;
import io.zeebe.db.impl.DbString;
import io.zeebe.engine.state.ZbColumnFamilies;
import io.zeebe.protocol.impl.record.value.message.MessageStartEventSubscriptionRecord;
import org.agrona.DirectBuffer;

public final class MessageStartEventSubscriptionState {

  private final DbString messageName;
  private final DbLong workflowKey;

  // (messageName, workflowKey => MessageSubscription)
  private final DbCompositeKey<DbString, DbLong> messageNameAndWorkflowKey;
  private final ColumnFamily<DbCompositeKey<DbString, DbLong>, SubscriptionValue>
      subscriptionsColumnFamily;
  private final SubscriptionValue subscriptionValue = new SubscriptionValue();

  // (workflowKey, messageName) => \0  : to find existing subscriptions of a workflow
  private final DbCompositeKey<DbLong, DbString> workflowKeyAndMessageName;
  private final ColumnFamily<DbCompositeKey<DbLong, DbString>, DbNil>
      subscriptionsOfWorkflowKeyColumnFamily;

  public MessageStartEventSubscriptionState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb, final DbContext dbContext) {
    messageName = new DbString();
    workflowKey = new DbLong();
    messageNameAndWorkflowKey = new DbCompositeKey<>(messageName, workflowKey);
    subscriptionsColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.MESSAGE_START_EVENT_SUBSCRIPTION_BY_NAME_AND_KEY,
            dbContext,
            messageNameAndWorkflowKey,
            subscriptionValue);

    workflowKeyAndMessageName = new DbCompositeKey<>(workflowKey, messageName);
    subscriptionsOfWorkflowKeyColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.MESSAGE_START_EVENT_SUBSCRIPTION_BY_KEY_AND_NAME,
            dbContext,
            workflowKeyAndMessageName,
            DbNil.INSTANCE);
  }

  public void put(final MessageStartEventSubscriptionRecord subscription) {
    subscriptionValue.set(subscription);

    messageName.wrapBuffer(subscription.getMessageNameBuffer());
    workflowKey.wrapLong(subscription.getWorkflowKey());
    subscriptionsColumnFamily.put(messageNameAndWorkflowKey, subscriptionValue);
    subscriptionsOfWorkflowKeyColumnFamily.put(workflowKeyAndMessageName, DbNil.INSTANCE);
  }

  public void removeSubscriptionsOfWorkflow(final long workflowKey) {
    this.workflowKey.wrapLong(workflowKey);

    subscriptionsOfWorkflowKeyColumnFamily.whileEqualPrefix(
        this.workflowKey,
        (key, value) -> {
          subscriptionsColumnFamily.delete(messageNameAndWorkflowKey);
          subscriptionsOfWorkflowKeyColumnFamily.delete(key);
        });
  }

  public boolean exists(final MessageStartEventSubscriptionRecord subscription) {
    messageName.wrapBuffer(subscription.getMessageNameBuffer());
    workflowKey.wrapLong(subscription.getWorkflowKey());

    return subscriptionsColumnFamily.exists(messageNameAndWorkflowKey);
  }

  public void visitSubscriptionsByMessageName(
      final DirectBuffer messageName, final MessageStartEventSubscriptionVisitor visitor) {

    this.messageName.wrapBuffer(messageName);
    subscriptionsColumnFamily.whileEqualPrefix(
        this.messageName,
        (key, value) -> {
          visitor.visit(value.get());
        });
  }

  @FunctionalInterface
  public interface MessageStartEventSubscriptionVisitor {
    void visit(MessageStartEventSubscriptionRecord subscription);
  }
}
