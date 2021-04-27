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

import java.time.Instant;
import java.time.format.DateTimeParseException;
import org.embulk.spi.Column;
import org.embulk.spi.PageBuilder;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;

public class TimestampColumnSetter extends AbstractDynamicColumnSetter {
    public TimestampColumnSetter(
            final PageBuilder pageBuilder,
            final Column column,
            final DefaultValueSetter defaultValueSetter,
            final TimestampFormatter timestampParser) {
        super(pageBuilder, column, defaultValueSetter);
        this.timestampParser = timestampParser;
    }

    @Override
    public void setNull() {
        this.pageBuilder.setNull(this.column);
    }

    @Override
    public void set(final boolean v) {
        this.defaultValueSetter.setTimestamp(this.pageBuilder, this.column);
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(final long v) {
        if (HAS_SET_TIMESTAMP_INSTANT) {
            this.pageBuilder.setTimestamp(this.column, Instant.ofEpochSecond(v));
        } else if (HAS_SET_TIMESTAMP_TIMESTAMP) {
            // This embulk-util-dynamic is still to be used in plugins for Embulk v0.9.*,
            // but PageBuilder.setTimestamp(Column, Instant) is added in recently v0.10.13.
            // https://github.com/embulk/embulk/pull/1294
            //
            // TODO: Stop the reflection tweak, and always call PageBuilder#setTimestamp(Column, Instant) for Embulk 0.11+.
            // https://github.com/embulk/embulk-util-dynamic/issues/5
            this.pageBuilder.setTimestamp(this.column, org.embulk.spi.time.Timestamp.ofEpochSecond(v));
        } else {
            throw new IllegalStateException(
                    "Neither PageBuilder#setTimestamp(Column, Instant) nor PageBuilder#setTimestamp(Column, Timestamp) found.");
        }
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(final double v) {
        final long sec = (long) v;
        final int nsec = (int) ((v - (double) sec) * 1000000000);

        if (HAS_SET_TIMESTAMP_INSTANT) {
            this.pageBuilder.setTimestamp(this.column, Instant.ofEpochSecond(sec, nsec));
        } else if (HAS_SET_TIMESTAMP_TIMESTAMP) {
            // This embulk-util-dynamic is still to be used in plugins for Embulk v0.9.*,
            // but PageBuilder.setTimestamp(Column, Instant) is added in recently v0.10.13.
            // https://github.com/embulk/embulk/pull/1294
            //
            // TODO: Stop the reflection tweak, and always call PageBuilder#setTimestamp(Column, Instant) for Embulk 0.11+.
            // https://github.com/embulk/embulk-util-dynamic/issues/5
            this.pageBuilder.setTimestamp(this.column, org.embulk.spi.time.Timestamp.ofEpochSecond(sec, nsec));
        }

        this.defaultValueSetter.setTimestamp(this.pageBuilder, this.column);
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1298
    public void set(final String v) {
        final Instant parsed;
        try {
            parsed = this.timestampParser.parse(v);
        } catch (final DateTimeParseException ex) {
            this.defaultValueSetter.setTimestamp(this.pageBuilder, this.column);
            return;
        }

        if (HAS_SET_TIMESTAMP_INSTANT) {
            this.pageBuilder.setTimestamp(this.column, parsed);
        } else if (HAS_SET_TIMESTAMP_TIMESTAMP) {
            // This embulk-util-dynamic is still to be used in plugins for Embulk v0.9.*,
            // but PageBuilder.setTimestamp(Column, Instant) is added in recently v0.10.13.
            // https://github.com/embulk/embulk/pull/1294
            //
            // TODO: Stop the reflection tweak, and always call PageBuilder#setTimestamp(Column, Instant) for Embulk 0.11+.
            // https://github.com/embulk/embulk-util-dynamic/issues/5
            this.pageBuilder.setTimestamp(this.column, org.embulk.spi.time.Timestamp.ofInstant(parsed));
        } else {
            throw new IllegalStateException(
                    "Neither PageBuilder#setTimestamp(Column, Instant) nor PageBuilder#setTimestamp(Column, Timestamp) found.");
        }
    }

    @Override
    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    public void set(final Instant v) {
        if (HAS_SET_TIMESTAMP_INSTANT) {
            this.pageBuilder.setTimestamp(this.column, v);
        } else if (HAS_SET_TIMESTAMP_TIMESTAMP) {
            // This embulk-util-dynamic is still to be used in plugins for Embulk v0.9.*,
            // but PageBuilder.setTimestamp(Column, Instant) is added in recently v0.10.13.
            // https://github.com/embulk/embulk/pull/1294
            //
            // TODO: Stop the reflection tweak, and always call PageBuilder#setTimestamp(Column, Instant) for Embulk 0.11+.
            // https://github.com/embulk/embulk-util-dynamic/issues/5
            this.pageBuilder.setTimestamp(this.column, org.embulk.spi.time.Timestamp.ofInstant(v));
        } else {
            throw new IllegalStateException(
                    "Neither PageBuilder#setTimestamp(Column, Instant) nor PageBuilder#setTimestamp(Column, Timestamp) found.");
        }
    }

    @Override
    public void set(final Value v) {
        this.defaultValueSetter.setTimestamp(this.pageBuilder, this.column);
    }

    @SuppressWarnings("deprecation")  // https://github.com/embulk/embulk/issues/1292
    private static boolean hasSetTimestampTimestamp() {
        try {
            PageBuilder.class.getMethod("setTimestamp", Column.class, org.embulk.spi.time.Timestamp.class);
        } catch (final NoSuchMethodException ex) {
            return false;
        }
        return true;
    }

    private static boolean hasSetTimestampInstant() {
        try {
            PageBuilder.class.getMethod("setTimestamp", Column.class, Instant.class);
        } catch (final NoSuchMethodException ex) {
            return false;
        }
        return true;
    }

    private static final boolean HAS_SET_TIMESTAMP_INSTANT = hasSetTimestampInstant();

    private static final boolean HAS_SET_TIMESTAMP_TIMESTAMP = hasSetTimestampTimestamp();

    private final TimestampFormatter timestampParser;
}
