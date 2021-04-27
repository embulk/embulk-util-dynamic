/*
 * Copyright 2015 The Embulk project
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

package org.embulk.util.dynamic;

import java.util.Map;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.JsonType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
import org.embulk.util.timestamp.TimestampFormatter;

class DynamicColumnSetterFactory {
    private DynamicColumnSetterFactory(
            final String defaultZoneString,
            final Map<String, ConfigSource> columnOptions,
            final DefaultValueSetter defaultValueSetter,
            final boolean useColumnForTimestampMetadata) {
        this.defaultZoneString = defaultZoneString;
        this.columnOptions = columnOptions;
        this.defaultValueSetter = defaultValueSetter;
        this.useColumnForTimestampMetadata = useColumnForTimestampMetadata;
    }

    static DynamicColumnSetterFactory createWithTimestampMetadata(
            final String defaultZoneString,
            final Map<String, ConfigSource> columnOptions,
            final DefaultValueSetter defaultValueSetter) {
        return new DynamicColumnSetterFactory(defaultZoneString, columnOptions, defaultValueSetter, false);
    }

    static DynamicColumnSetterFactory createWithTimestampMetadataFromColumn(
            final String defaultZoneString,
            final Map<String, ConfigSource> columnOptions,
            final DefaultValueSetter defaultValueSetter) {
        return new DynamicColumnSetterFactory(defaultZoneString, columnOptions, defaultValueSetter, true);
    }

    public static DefaultValueSetter nullDefaultValueSetter() {
        return new NullDefaultValueSetter();
    }

    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1298
    public DynamicColumnSetter newColumnSetter(final PageBuilder pageBuilder, final Column column) {
        final Type type = column.getType();
        if (type instanceof BooleanType) {
            return new BooleanColumnSetter(pageBuilder, column, this.defaultValueSetter);
        } else if (type instanceof LongType) {
            return new LongColumnSetter(pageBuilder, column, this.defaultValueSetter);
        } else if (type instanceof DoubleType) {
            return new DoubleColumnSetter(pageBuilder, column, this.defaultValueSetter);
        } else if (type instanceof StringType) {
            final TimestampFormatter formatter = TimestampFormatter.builder(getTimestampFormatForFormatter(column), true)
                    .setDefaultZoneFromString(getTimeZoneId(column))
                    .build();
            return new StringColumnSetter(pageBuilder, column, this.defaultValueSetter, formatter);
        } else if (type instanceof TimestampType) {
            final TimestampFormatter parser;
            if (this.useColumnForTimestampMetadata) {
                final TimestampType timestampType = (TimestampType) type;
                // TODO: Remove use of TimestampType's format. See: https://github.com/embulk/embulk/issues/935
                parser = TimestampFormatter.builder(getFormatFromTimestampTypeWithDepracationSuppressed(timestampType), true)
                        .setDefaultZoneFromString(getTimeZoneId(column))
                        .build();
            } else {
                parser = TimestampFormatter.builder(getTimestampFormatForParser(column), true)
                        .setDefaultZoneFromString(getTimeZoneId(column))
                        .build();
            }
            return new TimestampColumnSetter(pageBuilder, column, this.defaultValueSetter, parser);
        } else if (type instanceof JsonType) {
            final TimestampFormatter formatter = TimestampFormatter.builder(getTimestampFormatForFormatter(column), true)
                    .setDefaultZoneFromString(getTimeZoneId(column))
                    .build();
            return new JsonColumnSetter(pageBuilder, column, this.defaultValueSetter, formatter);
        }
        throw new ConfigException("Unknown column type: " + type);
    }

    private String getTimestampFormatForFormatter(final Column column) {
        final ConfigSource option = this.columnOptions.get(column.getName());
        if (option == null) {
            return "%Y-%m-%d %H:%M:%S.%6N";
        }
        return option.get(String.class, "timestamp_format", "%Y-%m-%d %H:%M:%S.%6N");
    }

    private String getTimestampFormatForParser(final Column column) {
        final ConfigSource option = this.columnOptions.get(column.getName());
        if (option == null) {
            return "%Y-%m-%d %H:%M:%S.%N";
        }
        return option.get(String.class, "timestamp_format", "%Y-%m-%d %H:%M:%S.%N");
    }

    private String getTimeZoneId(final Column column) {
        final ConfigSource option = this.columnOptions.get(column.getName());
        if (option == null) {
            return this.defaultZoneString;
        }
        return option.get(String.class, "timezone", this.defaultZoneString);
    }

    // TODO: Stop using TimestampType.getFormat.
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/935
    private String getFormatFromTimestampTypeWithDepracationSuppressed(final TimestampType timestampType) {
        return timestampType.getFormat();
    }

    private final String defaultZoneString;
    private final Map<String, ConfigSource> columnOptions;
    private final DefaultValueSetter defaultValueSetter;
    private final boolean useColumnForTimestampMetadata;
}
