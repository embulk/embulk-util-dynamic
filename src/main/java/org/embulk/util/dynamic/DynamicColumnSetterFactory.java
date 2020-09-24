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

class DynamicColumnSetterFactory {
    private DynamicColumnSetterFactory(
            final DynamicPageBuilder.BuilderTask task,
            final DefaultValueSetter defaultValue,
            final boolean useColumnForTimestampMetadata) {
        this.defaultValue = defaultValue;
        this.task = task;
        this.useColumnForTimestampMetadata = useColumnForTimestampMetadata;
    }

    static DynamicColumnSetterFactory createWithTimestampMetadataFromBuilderTask(
            final DynamicPageBuilder.BuilderTask task,
            final DefaultValueSetter defaultValue) {
        return new DynamicColumnSetterFactory(task, defaultValue, false);
    }

    static DynamicColumnSetterFactory createWithTimestampMetadataFromColumn(
            final DynamicPageBuilder.BuilderTask task,
            final DefaultValueSetter defaultValue) {
        return new DynamicColumnSetterFactory(task, defaultValue, true);
    }

    public static DefaultValueSetter nullDefaultValue() {
        return new NullDefaultValueSetter();
    }

    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1298
    public DynamicColumnSetter newColumnSetter(PageBuilder pageBuilder, Column column) {
        Type type = column.getType();
        if (type instanceof BooleanType) {
            return new BooleanColumnSetter(pageBuilder, column, defaultValue);
        } else if (type instanceof LongType) {
            return new LongColumnSetter(pageBuilder, column, defaultValue);
        } else if (type instanceof DoubleType) {
            return new DoubleColumnSetter(pageBuilder, column, defaultValue);
        } else if (type instanceof StringType) {
            final org.embulk.spi.time.TimestampFormatter formatter = org.embulk.spi.time.TimestampFormatter.of(
                    getTimestampFormatForFormatter(column), getTimeZoneId(column));
            return new StringColumnSetter(pageBuilder, column, defaultValue, formatter);
        } else if (type instanceof TimestampType) {
            // TODO use flexible time format like Ruby's Time.parse
            final org.embulk.spi.time.TimestampParser parser;
            if (this.useColumnForTimestampMetadata) {
                final TimestampType timestampType = (TimestampType) type;
                // https://github.com/embulk/embulk/issues/935
                parser = org.embulk.spi.time.TimestampParser.of(
                        getFormatFromTimestampTypeWithDepracationSuppressed(timestampType), getTimeZoneId(column));
            } else {
                parser = org.embulk.spi.time.TimestampParser.of(getTimestampFormatForParser(column), getTimeZoneId(column));
            }
            return new TimestampColumnSetter(pageBuilder, column, defaultValue, parser);
        } else if (type instanceof JsonType) {
            final org.embulk.spi.time.TimestampFormatter formatter = org.embulk.spi.time.TimestampFormatter.of(
                    getTimestampFormatForFormatter(column), getTimeZoneId(column));
            return new JsonColumnSetter(pageBuilder, column, defaultValue, formatter);
        }
        throw new ConfigException("Unknown column type: " + type);
    }

    private String getTimestampFormatForFormatter(Column column) {
        DynamicPageBuilder.ColumnOption option = getColumnOption(column);
        if (option != null) {
            return option.getTimestampFormatString();
        } else {
            return "%Y-%m-%d %H:%M:%S.%6N";
        }
    }

    private String getTimestampFormatForParser(Column column) {
        DynamicPageBuilder.ColumnOption option = getColumnOption(column);
        if (option != null) {
            return option.getTimestampFormatString();
        } else {
            return "%Y-%m-%d %H:%M:%S.%N";
        }
    }

    private String getTimeZoneId(Column column) {
        DynamicPageBuilder.ColumnOption option = getColumnOption(column);
        if (option != null) {
            return option.getTimeZoneId().or(task.getDefaultTimeZoneId());
        } else {
            return task.getDefaultTimeZoneId();
        }
    }

    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1301
    private DynamicPageBuilder.ColumnOption getColumnOption(Column column) {
        ConfigSource option = task.getColumnOptions().get(column.getName());
        if (option != null) {
            return option.loadConfig(DynamicPageBuilder.ColumnOption.class);
        } else {
            return null;
        }
    }

    // TODO: Stop using TimestampType.getFormat.
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/935
    private String getFormatFromTimestampTypeWithDepracationSuppressed(final TimestampType timestampType) {
        return timestampType.getFormat();
    }

    private final DefaultValueSetter defaultValue;
    private final DynamicPageBuilder.BuilderTask task;
    private final boolean useColumnForTimestampMetadata;
}
