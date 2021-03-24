/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.system.configuration;

import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

public final class RocksdbCfg implements ConfigurationEntry {

  private Properties columnFamilyOptions;

  @Override
  public void init(final BrokerCfg globalConfig, final String brokerBase) {
    if (columnFamilyOptions == null) {
      // lazy init to not have to deal with null when using these options
      columnFamilyOptions = new Properties();
    } else {
      // Since (some of) the columnFamilyOptions may have been provided as environment variables,
      // we must do some transformations on the entries of this properties object.
      columnFamilyOptions = initColumnFamilyOptions(columnFamilyOptions);
    }
  }

  private static Properties initColumnFamilyOptions(final Properties original) {
    final var result = new Properties();
    original.entrySet().stream()
        .map(RocksDBColumnFamilyOption::new)
        .forEach(entry -> result.put(entry.key, entry.value));
    return result;
  }

  public Properties getColumnFamilyOptions() {
    return columnFamilyOptions;
  }

  public void setColumnFamilyOptions(final Properties columnFamilyOptions) {
    this.columnFamilyOptions = columnFamilyOptions;
  }

  private static final class RocksDBColumnFamilyOption {

    private static final Pattern DOT_CHAR_PATTERN = Pattern.compile("\\.");
    private static final String UNDERSCORE_CHAR = "_";
    private final String key;
    private final Object value;

    private RocksDBColumnFamilyOption(final Entry<Object, Object> entry) {
      Objects.requireNonNull(entry.getKey());
      // The key of the entry may contain dot chars when provided as an Environment Variable.
      // These should always be replaced with underscores to match the available property names.
      // For example: `ZEEBE_BROKER_DATA_ROCKSDB_COLUMNFAMILYOPTIONS_WRITE_BUFFER_SIZE=8388608`
      // would result in a key with name `write.buffer.size`, but should be `write_buffer_size`.
      key = replaceAllDotCharsWithUnderscore(entry.getKey().toString());
      value = entry.getValue(); // the value can stay the same
    }

    private static String replaceAllDotCharsWithUnderscore(final String key) {
      return DOT_CHAR_PATTERN.matcher(key).replaceAll(UNDERSCORE_CHAR);
    }
  }
}
