/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.instance;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.DbContext;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.DbCompositeKey;
import io.zeebe.db.impl.DbLong;
import io.zeebe.engine.state.ZbColumnFamilies;
import java.util.Collection;
import java.util.function.BiConsumer;
import org.agrona.DirectBuffer;

public final class EventScopeInstanceState {

  private final DbLong eventScopeKey;
  private final EventScopeInstance eventScopeInstance;
  private final ColumnFamily<DbLong, EventScopeInstance> eventScopeInstanceColumnFamily;

  private final DbLong eventTriggerScopeKey;
  private final DbLong eventTriggerEventKey;
  private final DbCompositeKey<DbLong, DbLong> eventTriggerKey;
  private final EventTrigger eventTrigger;
  private final ColumnFamily<DbCompositeKey<DbLong, DbLong>, EventTrigger> eventTriggerColumnFamily;

  public EventScopeInstanceState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb, final DbContext dbContext) {
    eventScopeKey = new DbLong();
    eventScopeInstance = new EventScopeInstance();
    eventScopeInstanceColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.EVENT_SCOPE, dbContext, eventScopeKey, eventScopeInstance);

    eventTriggerScopeKey = new DbLong();
    eventTriggerEventKey = new DbLong();
    eventTriggerKey = new DbCompositeKey<>(eventTriggerScopeKey, eventTriggerEventKey);
    eventTrigger = new EventTrigger();
    eventTriggerColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.EVENT_TRIGGER, dbContext, eventTriggerKey, eventTrigger);
  }

  /**
   * If the scope exists, sets its accepting property to false.
   *
   * @param eventScopeKey the event scope key
   */
  public void shutdownInstance(final long eventScopeKey) {
    final EventScopeInstance instance = getInstance(eventScopeKey);
    if (instance != null) {
      this.eventScopeKey.wrapLong(eventScopeKey);
      instance.setAccepting(false);
      eventScopeInstanceColumnFamily.put(this.eventScopeKey, instance);
    }
  }

  /**
   * Creates a new event scope instance in the state
   *
   * @param eventScopeKey the event scope key
   * @param interruptingIds list of element IDs which should set accepting to false
   * @return whether the scope was created or not
   */
  public boolean createIfNotExists(
      final long eventScopeKey, final Collection<DirectBuffer> interruptingIds) {
    this.eventScopeKey.wrapLong(eventScopeKey);
    boolean wasCreated = false;

    if (!eventScopeInstanceColumnFamily.exists(this.eventScopeKey)) {
      createInstance(eventScopeKey, interruptingIds);
      wasCreated = true;
    }

    return wasCreated;
  }

  /**
   * Creates a new event scope instance in the state
   *
   * @param eventScopeKey the event scope key
   * @param interruptingIds list of element IDs which should set accepting to false
   */
  public void createInstance(
      final long eventScopeKey, final Collection<DirectBuffer> interruptingIds) {
    eventScopeInstance.reset();

    this.eventScopeKey.wrapLong(eventScopeKey);
    eventScopeInstance.setAccepting(true);
    for (final DirectBuffer interruptingId : interruptingIds) {
      eventScopeInstance.addInterrupting(interruptingId);
    }

    eventScopeInstanceColumnFamily.put(this.eventScopeKey, eventScopeInstance);
  }

  /**
   * Returns a event scope instance by key or null if none exists with this key.
   *
   * @param eventScopeKey the key of the event scope
   * @return the event scope instance or null
   */
  public EventScopeInstance getInstance(final long eventScopeKey) {
    this.eventScopeKey.wrapLong(eventScopeKey);
    final EventScopeInstance instance = eventScopeInstanceColumnFamily.get(this.eventScopeKey);
    return instance != null ? new EventScopeInstance(instance) : null;
  }

  /**
   * Delete an event scope from the state. Does not fail in case the scope does not exist.
   *
   * @param eventScopeKey the key of the event scope to delete
   */
  public void deleteInstance(final long eventScopeKey) {
    eventTriggerScopeKey.wrapLong(eventScopeKey);

    eventTriggerColumnFamily.whileEqualPrefix(
        eventTriggerScopeKey,
        (BiConsumer<DbCompositeKey<DbLong, DbLong>, EventTrigger>)
            (key, value) -> deleteTrigger(key));

    this.eventScopeKey.wrapLong(eventScopeKey);
    eventScopeInstanceColumnFamily.delete(this.eventScopeKey);
  }

  /**
   * @param eventScopeKey the key of the event scope the event is triggered in
   * @param eventKey the key of the event record (used for ordering)
   * @param elementId the id of the element which should be triggered, e.g. boundary event
   * @param variables the variables of the occurred event, i.e. message variables
   * @return true if the event was accepted by the event scope, false otherwise
   */
  public boolean triggerEvent(
      final long eventScopeKey,
      final long eventKey,
      final DirectBuffer elementId,
      final DirectBuffer variables) {
    this.eventScopeKey.wrapLong(eventScopeKey);
    final EventScopeInstance instance = eventScopeInstanceColumnFamily.get(this.eventScopeKey);

    if (instance != null && instance.isAccepting()) {
      if (instance.isInterrupting(elementId)) {
        instance.setAccepting(false);
        eventScopeInstanceColumnFamily.put(this.eventScopeKey, instance);
      }

      createTrigger(eventScopeKey, eventKey, elementId, variables);

      return true;
    } else {
      return false;
    }
  }

  private void createTrigger(
      final long eventScopeKey,
      final long eventKey,
      final DirectBuffer elementId,
      final DirectBuffer variables) {
    eventTriggerScopeKey.wrapLong(eventScopeKey);
    eventTriggerEventKey.wrapLong(eventKey);

    eventTrigger.setElementId(elementId).setVariables(variables).setEventKey(eventKey);

    eventTriggerColumnFamily.put(eventTriggerKey, eventTrigger);
  }

  /**
   * Returns the next event trigger for the event scope or null if none exists. This will not remove
   * the event trigger from the state.
   *
   * @param eventScopeKey the key of the event scope
   * @return the next event trigger or null if none exist
   */
  public EventTrigger peekEventTrigger(final long eventScopeKey) {
    eventTriggerScopeKey.wrapLong(eventScopeKey);
    final EventTrigger[] next = new EventTrigger[1];
    eventTriggerColumnFamily.whileEqualPrefix(
        eventTriggerScopeKey,
        (key, value) -> {
          next[0] = new EventTrigger(value);
          return false;
        });

    return next[0];
  }

  /**
   * Returns the next event trigger for the event scope or null if none exists. This will remove the
   * polled event trigger from the state if it exists.
   *
   * @param eventScopeKey the key of the event scope
   * @return the next event trigger or null if none exist
   */
  public EventTrigger pollEventTrigger(final long eventScopeKey) {
    eventTriggerScopeKey.wrapLong(eventScopeKey);
    final EventTrigger[] next = new EventTrigger[1];
    eventTriggerColumnFamily.whileEqualPrefix(
        eventTriggerScopeKey,
        (key, value) -> {
          next[0] = new EventTrigger(value);
          eventTriggerColumnFamily.delete(key);
          return false;
        });

    return next[0];
  }

  /**
   * Deletes an event trigger by key and scope key. Does not fail if the trigger does not exist.
   *
   * @param eventScopeKey the key of the event scope
   * @param eventKey the key of the event trigger
   */
  public void deleteTrigger(final long eventScopeKey, final long eventKey) {
    eventTriggerScopeKey.wrapLong(eventScopeKey);
    eventTriggerEventKey.wrapLong(eventKey);
    deleteTrigger(eventTriggerKey);
  }

  private void deleteTrigger(final DbCompositeKey<DbLong, DbLong> triggerKey) {
    eventTriggerColumnFamily.delete(triggerKey);
  }
}
