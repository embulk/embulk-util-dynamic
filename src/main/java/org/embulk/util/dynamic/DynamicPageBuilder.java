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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimeZoneIds;

public class DynamicPageBuilder implements AutoCloseable {
    private DynamicPageBuilder(
            final DynamicColumnSetterFactory factory,
            final BufferAllocator allocator,
            final Schema schema,
            final PageOutput output) {
        this.pageBuilder = new PageBuilder(allocator, schema, output);
        this.schema = schema;
        final ImmutableList.Builder<DynamicColumnSetter> setters = ImmutableList.builder();
        final ImmutableMap.Builder<String, DynamicColumnSetter> lookup = ImmutableMap.builder();
        for (final Column c : schema.getColumns()) {
            final DynamicColumnSetter setter = factory.newColumnSetter(this.pageBuilder, c);
            setters.add(setter);
            lookup.put(c.getName(), setter);
        }
        this.setters = setters.build().toArray(new DynamicColumnSetter[0]);
        this.columnLookup = lookup.build();
    }

    public static DynamicPageBuilder createWithTimestampMetadataFromBuilderTask(
            final BuilderTask task,
            final BufferAllocator allocator,
            final Schema schema,
            final PageOutput output) {
        // TODO configurable default value
        final DynamicColumnSetterFactory factory = DynamicColumnSetterFactory.createWithTimestampMetadataFromBuilderTask(
                task, DynamicColumnSetterFactory.nullDefaultValueSetter());
        return new DynamicPageBuilder(factory, allocator, schema, output);
    }

    public static DynamicPageBuilder createWithTimestampMetadataFromColumn(
            final BuilderTask task,
            final BufferAllocator allocator,
            final Schema schema,
            final PageOutput output) {
        // TODO configurable default value
        final DynamicColumnSetterFactory factory = DynamicColumnSetterFactory.createWithTimestampMetadataFromColumn(
                task, DynamicColumnSetterFactory.nullDefaultValueSetter());
        return new DynamicPageBuilder(factory, allocator, schema, output);
    }

    public static interface BuilderTask extends Task {
        @Config("default_timezone")
        @ConfigDefault("\"UTC\"")
        public String getDefaultTimeZoneId();

        // Using Joda-Time is deprecated, but the getter returns org.joda.time.DateTimeZone for plugin compatibility.
        // It won't be removed very soon at least until Embulk v0.10.
        @Deprecated
        public default org.joda.time.DateTimeZone getDefaultTimeZone() {
            if (getDefaultTimeZoneId() != null) {
                return TimeZoneIds.parseJodaDateTimeZone(getDefaultTimeZoneId());
            } else {
                return null;
            }
        }

        @Config("column_options")
        @ConfigDefault("{}")
        public Map<String, ConfigSource> getColumnOptions();
    }

    public static interface ColumnOption extends Task {
        // DynamicPageBuilder is used for inputs, then datetime parsing.
        // Ruby's strptime does not accept numeric prefixes in specifiers such as "%6N".
        @Config("timestamp_format")
        @ConfigDefault("\"%Y-%m-%d %H:%M:%S.%N\"")
        public String getTimestampFormatString();

        // org.embulk.spi.time.TimestampFormat is deprecated, but the getter returns TimestampFormat for compatibility.
        // It won't be removed very soon at least until Embulk v0.10.
        @Deprecated
        public default org.embulk.spi.time.TimestampFormat getTimestampFormat() {
            return new org.embulk.spi.time.TimestampFormat(getTimestampFormatString());
        }

        @Config("timezone")
        @ConfigDefault("null")
        public Optional<String> getTimeZoneId();

        // Using Joda-Time is deprecated, but the getter returns org.joda.time.DateTimeZone for plugin compatibility.
        // It won't be removed very soon at least until Embulk v0.10.
        @Deprecated
        public default Optional<org.joda.time.DateTimeZone> getTimeZone() {
            if (getTimeZoneId().isPresent()) {
                return Optional.of(TimeZoneIds.parseJodaDateTimeZone(getTimeZoneId().get()));
            } else {
                return Optional.absent();
            }
        }
    }

    public List<Column> getColumns() {
        return this.schema.getColumns();
    }

    public DynamicColumnSetter column(final Column c) {
        return this.setters[c.getIndex()];
    }

    public DynamicColumnSetter column(final int index) {
        if (index < 0 || this.setters.length <= index) {
            throw new DynamicColumnNotFoundException("Column index '" + index + "' is not exist");
        }
        return this.setters[index];
    }

    public DynamicColumnSetter lookupColumn(final String columnName) {
        final DynamicColumnSetter setter = this.columnLookup.get(columnName);
        if (setter == null) {
            throw new DynamicColumnNotFoundException("Column '" + columnName + "' is not exist");
        }
        return setter;
    }

    public DynamicColumnSetter columnOrSkip(final int index) {
        if (index < 0 || this.setters.length <= index) {
            return SkipColumnSetter.get();
        }
        return this.setters[index];
    }

    public DynamicColumnSetter columnOrSkip(final String columnName) {
        final DynamicColumnSetter setter = this.columnLookup.get(columnName);
        if (setter == null) {
            return SkipColumnSetter.get();
        }
        return setter;
    }

    // for jruby
    protected DynamicColumnSetter columnOrNull(final int index) {
        if (index < 0 || this.setters.length <= index) {
            return null;
        }
        return this.setters[index];
    }

    // for jruby
    protected DynamicColumnSetter columnOrNull(final String columnName) {
        return this.columnLookup.get(columnName);
    }

    public void addRecord() {
        this.pageBuilder.addRecord();
    }

    public void flush() {
        this.pageBuilder.flush();
    }

    public void finish() {
        this.pageBuilder.finish();
    }

    @Override
    public void close() {
        this.pageBuilder.close();
    }

    private final PageBuilder pageBuilder;
    private final Schema schema;
    private final DynamicColumnSetter[] setters;
    private final Map<String, DynamicColumnSetter> columnLookup;
}
